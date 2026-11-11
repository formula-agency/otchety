const date = '2026-11-11';
const from = Date.parse(`${date}T00:00:00+05:00`);
const to = Date.parse('2026-11-12T00:00:00+05:00');
const webhook = `${process.env.BITRIX_WEBHOOK_URL.replace(/\/+$/, '')}/`;

async function call(method, params) {
  for (let attempt = 0; attempt < 6; attempt++) {
    const response = await fetch(`${webhook}${method}.json`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(params),
    });
    const data = await response.json();
    if (response.ok && !data.error) return data;
    if (data.error !== 'QUERY_LIMIT_EXCEEDED' && response.status !== 429) {
      throw new Error(`${method}: ${data.error || response.status} ${data.error_description || ''}`);
    }
    await new Promise((resolve) => setTimeout(resolve, 1000 * (attempt + 1)));
  }
  throw new Error(`${method}: retry limit exceeded`);
}

async function list(method, params, extract, validate = () => {}) {
  const rows = [];
  let start = 0;
  do {
    const data = await call(method, { ...params, start });
    const page = extract(data);
    validate(page, data);
    rows.push(...page);
    start = data.next ?? null;
  } while (start !== null);
  return rows;
}

const fields = ['ID', 'DATE_CREATE', 'UF_LEAD_FIRST_UTM_MEDIUM', 'UF_LEAD_FIRST_UTM_SOURCE',
  'UF_LEAD_FIRST_UTM_CONTENT', 'UTM_MEDIUM', 'UTM_SOURCE', 'UTM_CONTENT'];
const leads = await list('crm.lead.list', {
  filter: { '>=DATE_CREATE': `${date}T00:00:00+05:00`, '<DATE_CREATE': '2026-11-12T00:00:00+05:00' },
  select: fields,
}, (data) => data.result || []);
const groups = {};
const contentGroups = {};
for (const lead of leads) {
  const actual = Date.parse(lead.DATE_CREATE);
  if (!(actual >= from && actual < to)) throw new Error('Lead date filter was not applied');
  const first = [lead.UF_LEAD_FIRST_UTM_MEDIUM, lead.UF_LEAD_FIRST_UTM_SOURCE, lead.UF_LEAD_FIRST_UTM_CONTENT];
  const current = [lead.UTM_MEDIUM, lead.UTM_SOURCE, lead.UTM_CONTENT];
  const selected = first.some(Boolean) ? first : current;
  const medium = String(selected[0] || '').trim().toLowerCase();
  const source = String(selected[1] || '').trim().toLowerCase();
  const content = String(selected[2] || '').trim().toLowerCase();
  const label = content.includes('reanimation_aiup') ? 'AiUp'
    : [medium, source, content].some((x) => /reanim/.test(x)) ? 'Реанимация'
    : ({ d2: 'Media Take', d1: 'DMP', r0: 'WR', r1: 'Хэши МТС', rean: 'Реанимация' }[medium] || 'Другие / без меток');
  groups[label] = (groups[label] || 0) + 1;
  const contentKey = `${label} | ${content || '(нет utm_content)'}`;
  contentGroups[contentKey] = (contentGroups[contentKey] || 0) + 1;
}
console.log('CREATED_LEADS', JSON.stringify({ count: leads.length, groups, contentGroups, measuredAt: new Date().toISOString() }));

try {
  const statuses = await list('crm.status.list', { filter: { ENTITY_ID: 'STATUS' } }, (data) => data.result || []);
  const ids = statuses.filter((s) => String(s.NAME || '').trim().toLowerCase() === 'необработанное').map((s) => String(s.STATUS_ID));
  console.log('UNPROCESSED_STATUSES', JSON.stringify(ids));
  if (ids.length !== 1) throw new Error('Unprocessed stage is ambiguous');
  const events = await list('crm.stagehistory.list', {
    entityTypeId: 1,
    filter: { '>=CREATED_TIME': `${date}T00:00:00+05:00`, '<CREATED_TIME': '2026-11-12T00:00:00+05:00', STATUS_ID: ids[0], TYPE_ID: 2 },
    select: ['ID', 'OWNER_ID', 'TYPE_ID', 'STATUS_ID', 'CREATED_TIME'],
  }, (data) => data.result?.items || [], (page) => {
    if (page.some((e) => !(Date.parse(e.CREATED_TIME) >= from && Date.parse(e.CREATED_TIME) < to)
      || String(e.STATUS_ID) !== ids[0] || String(e.TYPE_ID) !== '2')) {
      throw new Error('History filter was not applied');
    }
  });
  const badDates = events.filter((e) => !(Date.parse(e.CREATED_TIME) >= from && Date.parse(e.CREATED_TIME) < to)).length;
  if (badDates) throw new Error(`History date filter returned ${badDates} out-of-range records; counts unavailable`);
  const returns = events.filter((e) => ids.includes(String(e.STATUS_ID)) && String(e.TYPE_ID) === '2'
    && Date.parse(e.CREATED_TIME) <= Date.parse('2026-11-11T10:09:23.900Z'));
  const ownerIds = [...new Set(returns.map((e) => String(e.OWNER_ID)))];
  const initialDayIds = new Set(leads.map((lead) => String(lead.ID)));
  console.log('STAGE_RETURNS', JSON.stringify({ events: returns.length, uniqueLeads: ownerIds.length, createdSameDay: ownerIds.filter((id) => initialDayIds.has(id)).length }));
  const histories = new Map();
  for (let i = 0; i < ownerIds.length; i += 50) {
    const group = ownerIds.slice(i, i + 50);
    const cmd = Object.fromEntries(group.map((id) => [`h${id}`, `crm.stagehistory.list?entityTypeId=1&order[ID]=ASC&filter[OWNER_ID]=${encodeURIComponent(id)}&select[0]=ID&select[1]=OWNER_ID&select[2]=TYPE_ID&select[3]=STATUS_ID&select[4]=CREATED_TIME`]));
    const batch = await call('batch', { halt: 0, cmd });
    if (Object.keys(batch.result?.result_error || {}).length) throw new Error('History batch has failed requests');
    for (const id of group) {
      const key = `h${id}`;
      let history = batch.result?.result?.[key]?.items;
      if (!Array.isArray(history)) throw new Error('History batch response is incomplete');
      if (batch.result?.result_next?.[key] != null) {
        history = await list('crm.stagehistory.list', { entityTypeId: 1, order: { ID: 'ASC' }, filter: { OWNER_ID: id }, select: ['ID', 'OWNER_ID', 'TYPE_ID', 'STATUS_ID', 'CREATED_TIME'] }, (data) => data.result?.items || []);
      }
      histories.set(id, history);
    }
  }
  const repeated = new Set();
  let repeatedEvents = 0;
  for (const event of returns) {
    const history = histories.get(String(event.OWNER_ID)).sort((a, b) => Number(a.ID) - Number(b.ID));
    const index = history.findIndex((h) => String(h.ID) === String(event.ID));
    if (index < 0) throw new Error('History event missing from lead history');
    const previous = history.slice(0, index);
    if (previous.some((h) => ids.includes(String(h.STATUS_ID))) && previous.length && !ids.includes(String(previous.at(-1).STATUS_ID))) {
      repeated.add(String(event.OWNER_ID));
      repeatedEvents++;
    }
  }
  console.log('CONFIRMED_REPEAT_ROUNDS', JSON.stringify({ uniqueLeads: repeated.size, events: repeatedEvents, completeHistories: histories.size, measuredAt: new Date().toISOString() }));
  const summary = {};
  let missing = 0;
  const repeatedIds = [...repeated];
  const safeTag = (value) => {
    const tag = String(value || '').trim().toLowerCase();
    return /^[\p{L}\p{N}_\- .]{1,80}$/u.test(tag) ? tag : '(нет или нестандартная метка)';
  };
  const dateTag = (value) => {
    const text = String(value || '').trim();
    let match = text.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
    if (match) return `${match[3]}-${match[2]}-${match[1]}`;
    match = text.match(/^(\d{4}-\d{2}-\d{2})$/);
    return match ? match[1] : '(дата не указана)';
  };
  for (let i = 0; i < repeatedIds.length; i += 50) {
    const group = repeatedIds.slice(i, i + 50);
    const cmd = Object.fromEntries(group.map((id) => [`l${id}`, `crm.lead.get?id=${encodeURIComponent(id)}`]));
    const batch = await call('batch', { halt: 0, cmd });
    for (const id of group) {
      const lead = batch.result?.result?.[`l${id}`];
      if (!lead) { missing++; continue; }
      const primary = ['MEDIUM', 'SOURCE', 'CAMPAIGN', 'CONTENT', 'TERM'].map((field) => lead[`UF_LEAD_FIRST_UTM_${field}`] || '');
      const regular = ['MEDIUM', 'SOURCE', 'CAMPAIGN', 'CONTENT', 'TERM'].map((field) => lead[`UTM_${field}`] || '');
      const selected = primary.some(Boolean) ? primary : regular;
      const [medium, source, , content, term] = selected;
      const tags = [safeTag(medium), safeTag(source), safeTag(content)];
      const key = JSON.stringify(tags);
      const row = summary[key] ??= { count: 0, terms: {}, created: {} };
      row.count++;
      const termDate = dateTag(term);
      row.terms[termDate] = (row.terms[termDate] || 0) + 1;
      const creationDate = String(lead.DATE_CREATE || '').slice(0, 10) || '(нет даты)';
      row.created[creationDate] = (row.created[creationDate] || 0) + 1;
    }
  }
  console.log('REPEAT_SOURCE_DATES', JSON.stringify({ missing, cutoff: '2026-11-11T10:09:23.900Z', summary }));
} catch (error) {
  console.log('STAGE_RETURNS_UNAVAILABLE', error.message);
}
