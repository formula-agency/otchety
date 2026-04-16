import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { google } from 'googleapis';
import path from 'node:path';

const DEFAULT_DB_PATH = 'data/reporting-db.json';
const DEFAULT_REPORTS_DIR = 'reports';
const BITRIX_BATCH_SIZE = 50;
const SKOROZVON_PAGE_SIZE = 500;
const SKOROZVON_REQUEST_DELAY_MS = 250;
const BITRIX_FIRST_UTM_FIELDS = {
  medium: 'UF_LEAD_FIRST_UTM_MEDIUM',
  source: 'UF_LEAD_FIRST_UTM_SOURCE',
  campaign: 'UF_LEAD_FIRST_UTM_CAMPAIGN',
  content: 'UF_LEAD_FIRST_UTM_CONTENT',
  term: 'UF_LEAD_FIRST_UTM_TERM',
};
const BASE_LABELS = [
  { label: 'Сайты стандартные', tokens: ['site-standard', 'site_standard'] },
  { label: 'Сайты расширенные', tokens: ['site-expanded', 'site_expanded'] },
  { label: 'Телефоны', tokens: ['phone'] },
  { label: 'SMS', tokens: ['sms'] },
  { label: 'Пиксель', tokens: ['pixel'] },
  { label: 'Реанимация сделки', tokens: ['deal-reanimation', 'deal_reanimation', 'reanimation_deal', 'reanimation_formula'] },
  { label: 'Реанимация', tokens: ['reanimation', 'reanim'] },
  { label: 'Карты', tokens: ['maps', 'map'] },
];

function parseArgs(argv) {
  const args = { _: [] };

  for (let i = 0; i < argv.length; i += 1) {
    const item = argv[i];
    if (!item.startsWith('--')) {
      args._.push(item);
      continue;
    }

    const eqIndex = item.indexOf('=');
    if (eqIndex !== -1) {
      args[item.slice(2, eqIndex)] = item.slice(eqIndex + 1);
      continue;
    }

    const key = item.slice(2);
    const next = argv[i + 1];
    if (!next || next.startsWith('--')) {
      args[key] = true;
      continue;
    }

    args[key] = next;
    i += 1;
  }

  return args;
}

function todayIsoDate() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function monthStartIso(dateIso = todayIsoDate()) {
  return `${dateIso.slice(0, 7)}-01`;
}

function parseDateLike(value, fallbackYear = new Date().getFullYear()) {
  const raw = String(value ?? '').trim();
  if (!raw) return null;

  let match = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (match) return `${match[1]}-${match[2]}-${match[3]}`;

  match = raw.match(/^(\d{1,2})[.\/-](\d{1,2})[.\/-](\d{4})$/);
  if (match) {
    return `${match[3]}-${match[2].padStart(2, '0')}-${match[1].padStart(2, '0')}`;
  }

  match = raw.match(/^(\d{1,2})[.\/-](\d{1,2})$/);
  if (match) {
    return `${fallbackYear}-${match[2].padStart(2, '0')}-${match[1].padStart(2, '0')}`;
  }

  return null;
}

function dateToUnixSeconds(dateIso, endOfDay = false) {
  const [year, month, day] = dateIso.split('-').map(Number);
  const date = new Date(year, month - 1, day, 0, 0, 0, 0);
  if (endOfDay) date.setDate(date.getDate() + 1);
  return Math.floor(date.getTime() / 1000);
}

function addDays(dateIso, days) {
  const [year, month, day] = dateIso.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day + days));
  return date.toISOString().slice(0, 10);
}

function dateRange(fromIso, toIso) {
  const result = [];
  for (let date = fromIso; date <= toIso; date = addDays(date, 1)) {
    result.push(date);
  }
  return result;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function normalizePhone(value) {
  let digits = String(value ?? '').replace(/\D/g, '');
  if (digits.length === 10) digits = `7${digits}`;
  if (digits.length === 11 && digits.startsWith('8')) digits = `7${digits.slice(1)}`;
  return digits;
}

function safeSegment(value, fallback = 'empty') {
  const segment = String(value ?? '')
    .trim()
    .replace(/\s+/g, '_')
    .replace(/[^\p{L}\p{N}_-]+/gu, '')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');

  return segment || fallback;
}

function baseLabel(value) {
  const raw = String(value ?? '').trim();
  if (!raw) return 'Без меток';

  const normalized = raw.toLowerCase().replace(/[\s-]+/g, '_');
  const match = BASE_LABELS.find((item) => item.tokens.some((token) => normalized.includes(token.toLowerCase().replace(/[\s-]+/g, '_'))));
  return match?.label || raw;
}

function stableJson(value) {
  if (Array.isArray(value)) return `[${value.map(stableJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map((key) => `${JSON.stringify(key)}:${stableJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

function hashText(text) {
  return createHash('sha256').update(text).digest('hex');
}

function parseCsv(text, delimiter = ';') {
  const rows = [];
  let row = [];
  let cell = '';
  let inQuotes = false;
  const source = text.replace(/^\uFEFF/, '');

  for (let i = 0; i < source.length; i += 1) {
    const char = source[i];
    const next = source[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        cell += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (!inQuotes && char === delimiter) {
      row.push(cell);
      cell = '';
      continue;
    }

    if (!inQuotes && (char === '\n' || char === '\r')) {
      if (char === '\r' && next === '\n') i += 1;
      row.push(cell);
      if (row.some((value) => value !== '')) rows.push(row);
      row = [];
      cell = '';
      continue;
    }

    cell += char;
  }

  if (cell !== '' || row.length > 0) {
    row.push(cell);
    if (row.some((value) => value !== '')) rows.push(row);
  }

  if (rows.length === 0) return [];
  const headers = rows[0].map((header) => header.trim());

  return rows.slice(1).map((values) => {
    const record = {};
    headers.forEach((header, index) => {
      record[header] = values[index] ?? '';
    });
    return record;
  });
}

function csvEscape(value, delimiter = ';') {
  if (value === null || value === undefined) return '';
  const text = String(value);
  if (text.includes('"') || text.includes('\n') || text.includes('\r') || text.includes(delimiter)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

async function writeCsv(filePath, rows, columns) {
  await mkdir(path.dirname(filePath), { recursive: true });
  const header = columns.map((column) => csvEscape(column.header)).join(';');
  const body = rows.map((row) => columns.map((column) => csvEscape(column.value(row))).join(';'));
  await writeFile(filePath, `\uFEFF${[header, ...body].join('\r\n')}\r\n`, 'utf8');
}

function pick(record, names) {
  for (const name of names) {
    if (record[name] !== undefined && record[name] !== null && String(record[name]).trim() !== '') {
      return String(record[name]).trim();
    }
  }
  return '';
}

function mode(values) {
  const counts = new Map();
  for (const value of values.filter(Boolean)) {
    counts.set(value, (counts.get(value) ?? 0) + 1);
  }

  let bestValue = '';
  let bestCount = 0;
  for (const [value, count] of counts.entries()) {
    if (count > bestCount) {
      bestValue = value;
      bestCount = count;
    }
  }

  return bestValue;
}

async function loadJson(filePath, fallback) {
  if (!existsSync(filePath)) return fallback;
  const text = await readFile(filePath, 'utf8');
  return JSON.parse(text);
}

async function saveJson(filePath, value) {
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(value, null, 2)}\n`, 'utf8');
}

function createEmptyDb() {
  return {
    version: 1,
    report_context: null,
    uploads: [],
    upload_items: [],
    phones: {},
    bitrix_leads: {},
    bitrix_statuses: {},
    skorozvon_calls: {},
  };
}

function loadEnvFile(filePath) {
  const env = { ...process.env };
  const rawValues = [];

  if (!existsSync(filePath)) return env;

  const text = readFileSync(filePath, 'utf8');

  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    const eqIndex = trimmed.indexOf('=');
    if (eqIndex === -1) {
      rawValues.push(trimmed);
      continue;
    }

    const key = trimmed.slice(0, eqIndex).trim();
    const value = trimmed.slice(eqIndex + 1).trim();
    env[key] = value;
  }

  if (rawValues.length > 0) env.__raw = rawValues;
  return env;
}

function getBitrixWebhookUrl() {
  const env = loadEnvFile('bitrix.env');
  const url = env.BITRIX_WEBHOOK_URL || env.BITRIX_WEBHOOK || env.__raw?.[0];
  if (!url) throw new Error('Bitrix webhook is missing. Put it into bitrix.env.');
  return url.endsWith('/') ? url : `${url}/`;
}

function getSkorozvonConfig() {
  const env = loadEnvFile('skorozvon.env');
  const required = [
    'SKOROZVON_USERNAME',
    'SKOROZVON_API_KEY',
    'SKOROZVON_CLIENT_ID',
    'SKOROZVON_CLIENT_SECRET',
  ];
  const missing = required.filter((key) => !env[key]);
  if (missing.length > 0) {
    throw new Error(`Skorozvon credentials are missing: ${missing.join(', ')}`);
  }
  return env;
}

function extractUploadRows(records) {
  return records.map((record, index) => {
    const phone = normalizePhone(pick(record, ['Рабочий телефон', 'Телефон', 'Phone', 'PHONE', 'phone']));
    return {
      row_number: index + 2,
      phone,
      stage: pick(record, ['Стадия', 'STATUS', 'status']),
      responsible: pick(record, ['Ответственный', 'ASSIGNED_BY', 'assigned']),
      source: pick(record, ['Источник', 'SOURCE', 'source']),
      title: pick(record, ['Название лида', 'TITLE', 'title']),
      utm_medium: pick(record, ['Первичный utm_medium', 'utm_medium', 'UTM_MEDIUM']),
      utm_source: pick(record, ['Первичный utm_source', 'utm_source', 'UTM_SOURCE']),
      utm_campaign: pick(record, ['Первичный utm_campaign', 'utm_campaign', 'UTM_CAMPAIGN']),
      utm_content: pick(record, ['Первичный utm_content', 'utm_content', 'UTM_CONTENT']),
      utm_term: pick(record, ['Первичный utm_term', 'utm_term', 'UTM_TERM']),
    };
  }).filter((row) => row.phone);
}

function buildUploadMetadata(sourceFile, rows, db, fileHash, forceNewUpload) {
  const utmMedium = mode(rows.map((row) => row.utm_medium));
  const utmSource = mode(rows.map((row) => row.utm_source));
  const utmCampaign = mode(rows.map((row) => row.utm_campaign));
  const utmContent = mode(rows.map((row) => row.utm_content));
  const utmTerm = mode(rows.map((row) => row.utm_term));
  const stemDate = parseDateLike(path.parse(sourceFile).name);
  const termDate = parseDateLike(utmTerm);
  const uploadDate = termDate || stemDate || todayIsoDate();
  const prefix = [
    uploadDate,
    safeSegment(utmMedium),
    safeSegment(utmSource),
    safeSegment(utmCampaign),
    safeSegment(utmContent),
  ].join('_');
  const normalizedSource = path.resolve(sourceFile);

  if (!forceNewUpload) {
    const existing = db.uploads.find((upload) => upload.source_file === normalizedSource && upload.file_hash === fileHash);
    if (existing) return { upload: existing, created: false };
  }

  const existingSequences = db.uploads
    .filter((upload) => upload.upload_id.startsWith(`${prefix}_`))
    .map((upload) => Number(upload.upload_id.slice(prefix.length + 1)))
    .filter(Number.isFinite);
  const sequence = existingSequences.length > 0 ? Math.max(...existingSequences) + 1 : 1;
  const uploadId = `${prefix}_${String(sequence).padStart(3, '0')}`;

  return {
    created: true,
    upload: {
      upload_id: uploadId,
      upload_date: uploadDate,
      upload_prefix: prefix,
      sequence,
      imported_at: new Date().toISOString(),
      source_file: normalizedSource,
      file_hash: fileHash,
      utm_medium: utmMedium,
      utm_source: utmSource,
      utm_campaign: utmCampaign,
      utm_content: utmContent,
      utm_term: utmTerm,
      row_count: 0,
      unique_phone_count: 0,
      new_phone_count: 0,
      reload_phone_count: 0,
      duplicate_in_file_count: 0,
    },
  };
}

async function importUploadCsv(db, sourceFile, options) {
  const text = await readFile(sourceFile, 'utf8');
  const fileHash = hashText(text);
  const records = parseCsv(text, ';');
  const rows = extractUploadRows(records);
  const { upload, created } = buildUploadMetadata(sourceFile, rows, db, fileHash, options.forceNewUpload);

  if (!created) {
    return {
      upload,
      created: false,
      message: `Upload ${upload.upload_id} already exists for this file.`,
    };
  }

  const seenInFile = new Set();
  const registryBeforeImport = new Set(Object.keys(db.phones));
  const uniquePhones = new Set();
  let newPhoneCount = 0;
  let duplicateInFileCount = 0;

  for (const row of rows) {
    const duplicateInFile = seenInFile.has(row.phone);
    if (duplicateInFile) duplicateInFileCount += 1;
    seenInFile.add(row.phone);
    uniquePhones.add(row.phone);

    const existedBefore = registryBeforeImport.has(row.phone);
    const isNewBase = !existedBefore && !duplicateInFile;

    if (isNewBase) {
      newPhoneCount += 1;
      db.phones[row.phone] = {
        phone: row.phone,
        first_upload_id: upload.upload_id,
        first_seen_at: upload.upload_date,
        first_source_file: upload.source_file,
        bitrix_lead_ids: [],
      };
    }

    db.upload_items.push({
      upload_id: upload.upload_id,
      row_number: row.row_number,
      phone: row.phone,
      is_new_base: isNewBase,
      is_reload: existedBefore,
      is_duplicate_in_file: duplicateInFile,
      stage: row.stage,
      responsible: row.responsible,
      source: row.source,
      title: row.title,
      utm_medium: row.utm_medium,
      utm_source: row.utm_source,
      utm_campaign: row.utm_campaign,
      utm_content: row.utm_content,
      utm_term: row.utm_term,
    });
  }

  upload.row_count = rows.length;
  upload.unique_phone_count = uniquePhones.size;
  upload.new_phone_count = newPhoneCount;
  upload.reload_phone_count = uniquePhones.size - newPhoneCount;
  upload.duplicate_in_file_count = duplicateInFileCount;
  db.uploads.push(upload);

  return {
    upload,
    created: true,
    message: `Imported upload ${upload.upload_id}.`,
  };
}

async function postForm(url, params) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams(params),
  });
  const text = await response.text();
  let data;

  try {
    data = JSON.parse(text);
  } catch {
    data = { raw: text };
  }

  if (!response.ok || data.error) {
    const detail = data.error_description || data.error || text;
    throw new Error(`Request failed ${response.status}: ${detail}`);
  }

  return data;
}

async function bitrixCall(webhookUrl, method, params) {
  return postForm(`${webhookUrl}${method}.json`, params);
}

async function bitrixBatch(webhookUrl, commands) {
  const params = { halt: '0' };
  Object.entries(commands).forEach(([key, command]) => {
    params[`cmd[${key}]`] = command;
  });
  const data = await postForm(`${webhookUrl}batch.json`, params);
  return data.result?.result ?? {};
}

function chunks(items, size) {
  const result = [];
  for (let i = 0; i < items.length; i += size) {
    result.push(items.slice(i, i + size));
  }
  return result;
}

async function syncBitrixStatuses(db, webhookUrl) {
  const data = await postForm(`${webhookUrl}crm.status.list.json`, {
    'filter[ENTITY_ID]': 'STATUS',
  });

  for (const status of data.result ?? []) {
    db.bitrix_statuses[status.STATUS_ID] = {
      status_id: status.STATUS_ID,
      name: status.NAME,
      sort: status.SORT,
      semantics: status.SEMANTICS || '',
    };
  }
}

function bitrixPhoneValues(lead) {
  const values = [];
  for (const item of lead.PHONE ?? []) {
    const phone = normalizePhone(item.VALUE);
    if (phone) values.push(phone);
  }
  return [...new Set(values)];
}

function leadCreatedDate(lead) {
  return String(lead?.date_create || lead?.DATE_CREATE || '').slice(0, 10);
}

function leadUploadDate(lead) {
  const createdDate = leadCreatedDate(lead);
  const fallbackYear = Number(createdDate.slice(0, 4)) || new Date().getFullYear();
  return parseDateLike(lead?.utm_term, fallbackYear) || createdDate;
}

function leadTimestamp(lead) {
  const value = lead?.date_create || lead?.DATE_CREATE || '';
  const time = Date.parse(value);
  return Number.isFinite(time) ? time : Number.MAX_SAFE_INTEGER;
}

function leadPhones(lead) {
  return lead.phones ?? bitrixPhoneValues(lead);
}

function hasLeadUtm(lead) {
  return Boolean(lead?.utm_medium || lead?.utm_source || lead?.utm_campaign || lead?.utm_content || lead?.utm_term);
}

function isBitrixUploadedLead(lead) {
  return Boolean(lead?.source_id);
}

function normalizeBitrixLead(lead) {
  return {
    id: String(lead.ID),
    title: lead.TITLE || '',
    status_id: lead.STATUS_ID || '',
    status_semantic_id: lead.STATUS_SEMANTIC_ID || '',
    source_id: lead.SOURCE_ID || '',
    assigned_by_id: lead.ASSIGNED_BY_ID || '',
    date_create: lead.DATE_CREATE || '',
    date_modify: lead.DATE_MODIFY || '',
    date_closed: lead.DATE_CLOSED || '',
    utm_medium: lead[BITRIX_FIRST_UTM_FIELDS.medium] || lead.UTM_MEDIUM || '',
    utm_source: lead[BITRIX_FIRST_UTM_FIELDS.source] || lead.UTM_SOURCE || '',
    utm_campaign: lead[BITRIX_FIRST_UTM_FIELDS.campaign] || lead.UTM_CAMPAIGN || '',
    utm_content: lead[BITRIX_FIRST_UTM_FIELDS.content] || lead.UTM_CONTENT || '',
    utm_term: lead[BITRIX_FIRST_UTM_FIELDS.term] || lead.UTM_TERM || '',
    bitrix_utm_medium: lead.UTM_MEDIUM || '',
    bitrix_utm_source: lead.UTM_SOURCE || '',
    bitrix_utm_campaign: lead.UTM_CAMPAIGN || '',
    bitrix_utm_content: lead.UTM_CONTENT || '',
    bitrix_utm_term: lead.UTM_TERM || '',
    first_utm_medium: lead[BITRIX_FIRST_UTM_FIELDS.medium] || '',
    first_utm_source: lead[BITRIX_FIRST_UTM_FIELDS.source] || '',
    first_utm_campaign: lead[BITRIX_FIRST_UTM_FIELDS.campaign] || '',
    first_utm_content: lead[BITRIX_FIRST_UTM_FIELDS.content] || '',
    first_utm_term: lead[BITRIX_FIRST_UTM_FIELDS.term] || '',
    phones: bitrixPhoneValues(lead),
  };
}

function updatePhoneRegistryFromLead(db, lead) {
  for (const phone of leadPhones(lead)) {
    if (!phone) continue;
    if (!db.phones[phone]) {
      db.phones[phone] = {
        phone,
        first_upload_id: '',
        first_seen_at: leadCreatedDate(lead),
        first_source_file: '',
        first_bitrix_lead_id: lead.id,
        bitrix_lead_ids: [],
      };
    }

    const currentFirstLead = db.bitrix_leads[db.phones[phone].first_bitrix_lead_id];
    if (!db.phones[phone].first_bitrix_lead_id || leadTimestamp(lead) < leadTimestamp(currentFirstLead)) {
      db.phones[phone].first_seen_at = leadCreatedDate(lead);
      db.phones[phone].first_bitrix_lead_id = lead.id;
    }

    const ids = new Set(db.phones[phone].bitrix_lead_ids ?? []);
    ids.add(lead.id);
    db.phones[phone].bitrix_lead_ids = [...ids].sort((a, b) => Number(a) - Number(b));
  }
}

async function syncBitrixForPhones(db, phones) {
  if (phones.length === 0) return { leadCount: 0 };

  const webhookUrl = getBitrixWebhookUrl();
  await syncBitrixStatuses(db, webhookUrl);
  const phoneToLeadIds = new Map();

  for (const group of chunks(phones, BITRIX_BATCH_SIZE)) {
    const commands = {};
    group.forEach((phone, index) => {
      commands[`dup_${index}`] = `crm.duplicate.findbycomm?type=PHONE&values[0]=${encodeURIComponent(phone)}&entity_type[0]=LEAD`;
    });

    const results = await bitrixBatch(webhookUrl, commands);
    group.forEach((phone, index) => {
      const leadIds = results[`dup_${index}`]?.LEAD ?? [];
      phoneToLeadIds.set(phone, leadIds.map(String));
    });
  }

  const leadIds = [...new Set([...phoneToLeadIds.values()].flat())];

  for (const group of chunks(leadIds, BITRIX_BATCH_SIZE)) {
    const commands = {};
    group.forEach((id) => {
      commands[`lead_${id}`] = `crm.lead.get?id=${encodeURIComponent(id)}`;
    });

    const results = await bitrixBatch(webhookUrl, commands);
    for (const id of group) {
      const lead = results[`lead_${id}`];
      if (!lead) continue;

      const normalized = normalizeBitrixLead(lead);
      db.bitrix_leads[normalized.id] = normalized;
      updatePhoneRegistryFromLead(db, normalized);
    }
  }

  for (const [phone, ids] of phoneToLeadIds.entries()) {
    if (!db.phones[phone]) continue;
    const current = new Set(db.phones[phone].bitrix_lead_ids ?? []);
    ids.forEach((id) => current.add(String(id)));
    db.phones[phone].bitrix_lead_ids = [...current].sort((a, b) => Number(a) - Number(b));
  }

  return { leadCount: leadIds.length };
}

function bitrixLeadSelectParams() {
  const fields = [
    'ID',
    'TITLE',
    'STATUS_ID',
    'STATUS_SEMANTIC_ID',
    'SOURCE_ID',
    'ASSIGNED_BY_ID',
    'DATE_CREATE',
    'DATE_MODIFY',
    'DATE_CLOSED',
    'UTM_MEDIUM',
    'UTM_SOURCE',
    'UTM_CAMPAIGN',
    'UTM_CONTENT',
    'UTM_TERM',
    BITRIX_FIRST_UTM_FIELDS.medium,
    BITRIX_FIRST_UTM_FIELDS.source,
    BITRIX_FIRST_UTM_FIELDS.campaign,
    BITRIX_FIRST_UTM_FIELDS.content,
    BITRIX_FIRST_UTM_FIELDS.term,
    'PHONE',
  ];

  return Object.fromEntries(fields.map((field, index) => [`select[${index}]`, field]));
}

async function fetchBitrixLeadsCreatedRange(db, fromIso, toIso) {
  const webhookUrl = getBitrixWebhookUrl();
  await syncBitrixStatuses(db, webhookUrl);

  let start = 0;
  const leads = [];

  do {
    const params = {
      ...bitrixLeadSelectParams(),
      'order[DATE_CREATE]': 'ASC',
      'filter[>=DATE_CREATE]': `${fromIso}T00:00:00+05:00`,
      'filter[<DATE_CREATE]': `${addDays(toIso, 1)}T00:00:00+05:00`,
      start: String(start),
    };
    const data = await bitrixCall(webhookUrl, 'crm.lead.list', params);
    const pageLeads = (data.result ?? []).map(normalizeBitrixLead);

    for (const lead of pageLeads) {
      db.bitrix_leads[lead.id] = lead;
      updatePhoneRegistryFromLead(db, lead);
    }

    leads.push(...pageLeads);
    start = data.next ?? null;
  } while (start !== null && start !== undefined);

  const phones = [...new Set(leads.flatMap((lead) => lead.phones).filter(Boolean))];
  const duplicates = await syncBitrixForPhones(db, phones);

  return {
    leadCount: leads.length,
    phoneCount: phones.length,
    duplicateLeadCount: duplicates.leadCount,
  };
}

async function getSkorozvonAccessToken() {
  const config = getSkorozvonConfig();
  const response = await fetch('https://api.skorozvon.ru/oauth/token', {
    method: 'POST',
    body: new URLSearchParams({
      grant_type: 'password',
      username: config.SKOROZVON_USERNAME,
      api_key: config.SKOROZVON_API_KEY,
      client_id: config.SKOROZVON_CLIENT_ID,
      client_secret: config.SKOROZVON_CLIENT_SECRET,
    }),
  });
  const data = await response.json();

  if (!response.ok || !data.access_token) {
    throw new Error(`Skorozvon OAuth failed: ${data.error_description || data.error || response.status}`);
  }

  return data.access_token;
}

async function fetchJsonWithRetry(url, options, label, attempts = 6) {
  let lastError = null;

  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    const response = await fetch(url, options);
    const text = await response.text();
    let data = null;

    try {
      data = JSON.parse(text);
    } catch {
      data = null;
    }

    const rateLimited = response.status === 429 || /rate limit/i.test(text);
    if (response.ok && data) return data;

    lastError = new Error(`${label} failed: ${data?.error || text || response.status}`);

    if (!rateLimited && response.status < 500) break;
    await sleep(Math.min(30000, 1000 * attempt * attempt));
  }

  throw lastError;
}

async function fetchSkorozvonCallsForDate(dateIso) {
  const accessToken = await getSkorozvonAccessToken();
  const startTime = dateToUnixSeconds(dateIso);
  const endTime = dateToUnixSeconds(dateIso, true);
  const calls = [];
  let page = 1;
  let totalPages = 1;

  do {
    const data = await fetchJsonWithRetry('https://api.skorozvon.ru/api/reports/calls_total.json', {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        length: SKOROZVON_PAGE_SIZE,
        page,
        start_time: startTime,
        end_time: endTime,
        filter: {
          results_ids: 'all',
          scenarios_ids: 'all',
          types: 'all',
        },
      }),
    }, `Skorozvon calls report ${dateIso} page ${page}`);

    calls.push(...(data.data ?? []));
    totalPages = Number(data.total_pages ?? 1);
    page += 1;
    await sleep(SKOROZVON_REQUEST_DELAY_MS);
  } while (page <= totalPages);

  return calls;
}

function upsertSkorozvonCalls(db, calls) {
  for (const call of calls) {
    const phone = normalizePhone(call.phone);
    const startedAt = call.started_at || '';
    db.skorozvon_calls[String(call.id)] = {
      id: String(call.id),
      started_at: startedAt,
      date: startedAt.slice(0, 10),
      phone,
      call_type: call.call_type || '',
      call_type_code: call.call_type_code || '',
      duration: Number(call.duration ?? 0),
      waiting_time: Number(call.waiting_time ?? 0),
      reason: call.reason || '',
      user_id: call.user?.id ?? '',
      user_name: call.user?.name ?? '',
      scenario_id: call.scenario?.id ?? '',
      scenario_name: call.scenario?.name ?? '',
      result_id: call.scenario_result?.id ?? '',
      result_name: call.scenario_result?.name ?? '',
      result_group_id: call.scenario_result_group?.id ?? '',
      result_group_name: call.scenario_result_group?.name ?? '',
      call_project_id: call.call_project?.id ?? '',
      call_project_title: call.call_project?.title ?? '',
      lead_id: call.lead?.id ?? '',
      lead_name: call.lead?.name ?? '',
      lead_tags: call.lead?.tags || '',
      organization_tags: call.organization?.tags || '',
    };
  }
}

function statusSemantic(db, statusId) {
  return db.bitrix_statuses[statusId]?.semantics || '';
}

function isConvertedLead(db, lead) {
  return lead?.status_id === 'CONVERTED' || lead?.status_semantic_id === 'S' || statusSemantic(db, lead?.status_id) === 'S';
}

function isLostLead(db, lead) {
  return lead?.status_id === 'JUNK' || lead?.status_semantic_id === 'F' || statusSemantic(db, lead?.status_id) === 'F';
}

function leadName(db, statusId) {
  return db.bitrix_statuses[statusId]?.name || statusId || '';
}

function phoneLeadIds(db, phone) {
  return db.phones[phone]?.bitrix_lead_ids ?? [];
}

function leadsForPhone(db, phone) {
  return phoneLeadIds(db, phone).map((id) => db.bitrix_leads[id]).filter(Boolean);
}

function uploadUniquePhones(db, uploadId) {
  return [...new Set(db.upload_items.filter((item) => item.upload_id === uploadId).map((item) => item.phone))];
}

function callsForFirstUpload(db, uploadId) {
  return Object.values(db.skorozvon_calls).filter((call) => db.phones[call.phone]?.first_upload_id === uploadId);
}

function reportContext(db) {
  return db.report_context ?? {};
}

function inReportPeriod(db, dateIso) {
  const context = reportContext(db);
  if (!dateIso) return false;
  if (context.from && dateIso < context.from) return false;
  if (context.to && dateIso > context.to) return false;
  return true;
}

function percent(numerator, denominator) {
  if (!denominator) return '';
  return `${((numerator / denominator) * 100).toFixed(2).replace('.', ',')}%`;
}

function aggregateCalls(calls) {
  const total = calls.length;
  const uniquePhones = new Set(calls.map((call) => call.phone).filter(Boolean));
  const durationGt0 = calls.filter((call) => call.duration > 0).length;
  const durationGte10 = calls.filter((call) => call.duration >= 10).length;
  const durationGte30 = calls.filter((call) => call.duration >= 30).length;
  const phonesGt0 = new Set(calls.filter((call) => call.duration > 0).map((call) => call.phone).filter(Boolean));
  const phonesGte10 = new Set(calls.filter((call) => call.duration >= 10).map((call) => call.phone).filter(Boolean));
  const phonesGte30 = new Set(calls.filter((call) => call.duration >= 30).map((call) => call.phone).filter(Boolean));
  const successfulByGroup = calls.filter((call) => call.result_group_name === 'Успешные').length;
  const durationSum = calls.reduce((sum, call) => sum + Number(call.duration || 0), 0);

  return {
    total,
    unique_phone_count: uniquePhones.size,
    duration_gt_0: durationGt0,
    duration_gte_10: durationGte10,
    duration_gte_30: durationGte30,
    phones_gt_0: phonesGt0.size,
    phones_gte_10: phonesGte10.size,
    phones_gte_30: phonesGte30.size,
    successful_by_group: successfulByGroup,
    duration_sum: durationSum,
    connect_rate_calls_10: percent(durationGte10, total),
    connect_rate_phones_10: percent(phonesGte10.size, uniquePhones.size),
  };
}

function firstLeadForPhone(db, phone) {
  const phoneRecord = db.phones[phone];
  if (phoneRecord?.first_bitrix_lead_id && db.bitrix_leads[phoneRecord.first_bitrix_lead_id]) {
    return db.bitrix_leads[phoneRecord.first_bitrix_lead_id];
  }

  return leadsForPhone(db, phone).sort((a, b) => leadTimestamp(a) - leadTimestamp(b))[0] ?? null;
}

function buildBitrixBaseReportRows(db) {
  const groups = new Map();

  for (const lead of Object.values(db.bitrix_leads)) {
    const uploadDate = leadUploadDate(lead);
    if (!lead || !inReportPeriod(db, uploadDate)) continue;
    if (!hasLeadUtm(lead)) continue;
    if (!isBitrixUploadedLead(lead)) continue;

    const key = stableJson({
      date: uploadDate,
      utm_medium: lead.utm_medium,
      utm_source: lead.utm_source,
      utm_campaign: lead.utm_campaign,
      utm_content: lead.utm_content,
      utm_term: lead.utm_term,
    });

    if (!groups.has(key)) {
      groups.set(key, {
        upload_id: '',
        upload_date: uploadDate,
        utm_medium: lead.utm_medium,
        utm_source: lead.utm_source,
        utm_campaign: lead.utm_campaign,
        utm_content: lead.utm_content,
        utm_term: lead.utm_term,
        leads: [],
      });
    }

    groups.get(key).leads.push(lead);
  }

  return [...groups.values()]
    .sort((a, b) => `${a.upload_date}_${a.utm_medium}_${a.utm_source}_${a.utm_campaign}_${a.utm_content}`.localeCompare(`${b.upload_date}_${b.utm_medium}_${b.utm_source}_${b.utm_campaign}_${b.utm_content}`))
    .map((group) => {
      const createdLeads = group.leads;
      const createdLeadIds = new Set(createdLeads.map((lead) => lead.id));
      const phones = [...new Set(createdLeads.flatMap((lead) => leadPhones(lead)).filter(Boolean))];
      const newPhones = phones.filter((phone) => {
        const firstLead = firstLeadForPhone(db, phone);
        return firstLead ? createdLeadIds.has(firstLead.id) : false;
      });
      const leadIds = [...new Set(newPhones.flatMap((phone) => phoneLeadIds(db, phone)))];
      const leads = leadIds.map((id) => db.bitrix_leads[id]).filter(Boolean);
      const convertedLeads = leads.filter((lead) => isConvertedLead(db, lead));
      const convertedPhones = newPhones.filter((phone) => leadsForPhone(db, phone).some((lead) => isConvertedLead(db, lead)));
      const lostPhones = newPhones.filter((phone) => {
        const phoneLeads = leadsForPhone(db, phone);
        return phoneLeads.length > 0 && phoneLeads.every((lead) => isLostLead(db, lead));
      });
      const workingPhones = newPhones.filter((phone) => {
        const phoneLeads = leadsForPhone(db, phone);
        return phoneLeads.some((lead) => !isConvertedLead(db, lead) && !isLostLead(db, lead));
      });

      return {
        source_type: 'bitrix_api',
        upload_id: '',
        upload_date: group.upload_date,
        utm_medium: group.utm_medium,
        utm_source: group.utm_source,
        utm_campaign: group.utm_campaign,
        utm_content: group.utm_content,
        utm_term: group.utm_term,
        row_count: createdLeads.length,
        upload_lead_count: createdLeads.length,
        unique_phone_count: phones.length,
        new_phone_count: newPhones.length,
        reload_phone_count: Math.max(phones.length - newPhones.length, 0),
        duplicate_in_file_count: Math.max(createdLeads.length - phones.length, 0),
        bitrix_lead_count: createdLeads.length,
        working_phone_count: workingPhones.length,
        lost_phone_count: lostPhones.length,
        converted_phone_count: convertedPhones.length,
        converted_lead_count: convertedLeads.length,
        cr_by_phone: percent(convertedPhones.length, newPhones.length),
        cr_by_lead: percent(convertedLeads.length, newPhones.length),
        calls_total: '',
        calls_unique_phones: '',
        calls_duration_gte_10: '',
        callability_by_calls_10: '',
        callability_by_phones_10: '',
      };
    });
}

function buildBaseReportRows(db) {
  if (reportContext(db).source === 'bitrix') {
    return buildBitrixBaseReportRows(db);
  }

  return db.uploads.map((upload) => {
    const phones = uploadUniquePhones(db, upload.upload_id);
    const newPhones = phones.filter((phone) => db.phones[phone]?.first_upload_id === upload.upload_id);
    const leadIds = [...new Set(newPhones.flatMap((phone) => phoneLeadIds(db, phone)))];
    const leads = leadIds.map((id) => db.bitrix_leads[id]).filter(Boolean);
    const convertedLeads = leads.filter((lead) => isConvertedLead(db, lead));
    const convertedPhones = newPhones.filter((phone) => leadsForPhone(db, phone).some((lead) => isConvertedLead(db, lead)));
    const lostPhones = newPhones.filter((phone) => {
      const phoneLeads = leadsForPhone(db, phone);
      return phoneLeads.length > 0 && phoneLeads.every((lead) => isLostLead(db, lead));
    });
    const workingPhones = newPhones.filter((phone) => {
      const phoneLeads = leadsForPhone(db, phone);
      return phoneLeads.some((lead) => !isConvertedLead(db, lead) && !isLostLead(db, lead));
    });
    const callStats = aggregateCalls(callsForFirstUpload(db, upload.upload_id));

    return {
      ...upload,
      upload_lead_count: upload.row_count,
      bitrix_lead_count: leadIds.length,
      working_phone_count: workingPhones.length,
      lost_phone_count: lostPhones.length,
      converted_phone_count: convertedPhones.length,
      converted_lead_count: convertedLeads.length,
      cr_by_phone: percent(convertedPhones.length, upload.new_phone_count),
      cr_by_lead: percent(convertedLeads.length, upload.new_phone_count),
      calls_total: callStats.total,
      calls_unique_phones: callStats.unique_phone_count,
      calls_duration_gte_10: callStats.duration_gte_10,
      callability_by_calls_10: callStats.connect_rate_calls_10,
      callability_by_phones_10: callStats.connect_rate_phones_10,
    };
  });
}

function buildUploadItemsRows(db) {
  return db.upload_items.map((item) => {
    const leads = leadsForPhone(db, item.phone);
    const converted = leads.some((lead) => isConvertedLead(db, lead));
    const statuses = leads.map((lead) => leadName(db, lead.status_id)).filter(Boolean).join(', ');
    return {
      ...item,
      first_upload_id: db.phones[item.phone]?.first_upload_id || '',
      bitrix_lead_ids: phoneLeadIds(db, item.phone).join(','),
      bitrix_statuses: statuses,
      converted: converted ? 'yes' : 'no',
    };
  });
}

function buildCallabilityDailyRows(db) {
  const groups = new Map();

  for (const call of Object.values(db.skorozvon_calls)) {
    if (!inReportPeriod(db, call.date)) continue;
    const firstUploadId = db.phones[call.phone]?.first_upload_id || 'unmatched';
    const key = stableJson({ date: call.date, firstUploadId });
    if (!groups.has(key)) groups.set(key, { date: call.date, first_upload_id: firstUploadId, calls: [] });
    groups.get(key).calls.push(call);
  }

  return [...groups.values()]
    .sort((a, b) => `${a.date}_${a.first_upload_id}`.localeCompare(`${b.date}_${b.first_upload_id}`))
    .map((group) => ({ ...group, ...aggregateCalls(group.calls) }));
}

function buildCallabilityByBaseRows(db) {
  const uploadIds = new Set(db.uploads.map((upload) => upload.upload_id));
  for (const call of Object.values(db.skorozvon_calls)) {
    if (!inReportPeriod(db, call.date)) continue;
    uploadIds.add(db.phones[call.phone]?.first_upload_id || 'unmatched');
  }

  return [...uploadIds].sort().map((uploadId) => {
    const calls = Object.values(db.skorozvon_calls).filter((call) => inReportPeriod(db, call.date) && (db.phones[call.phone]?.first_upload_id || 'unmatched') === uploadId);
    const upload = db.uploads.find((item) => item.upload_id === uploadId);
    return {
      first_upload_id: uploadId,
      upload_date: upload?.upload_date || '',
      utm_medium: upload?.utm_medium || '',
      utm_source: upload?.utm_source || '',
      utm_campaign: upload?.utm_campaign || '',
      utm_content: upload?.utm_content || '',
      ...aggregateCalls(calls),
    };
  });
}

function callabilityMetricColumns() {
  return [
    { header: 'Звонков', value: (row) => row.total },
    { header: 'Уникальных телефонов', value: (row) => row.unique_phone_count },
    { header: 'Звонков >0 сек', value: (row) => row.duration_gt_0 },
    { header: 'Звонков 10+ сек', value: (row) => row.duration_gte_10 },
    { header: 'Звонков 30+ сек', value: (row) => row.duration_gte_30 },
    { header: 'Телефонов >0 сек', value: (row) => row.phones_gt_0 },
    { header: 'Телефонов 10+ сек', value: (row) => row.phones_gte_10 },
    { header: 'Телефонов 30+ сек', value: (row) => row.phones_gte_30 },
    { header: 'Успешные по группе Скорозвона', value: (row) => row.successful_by_group },
    { header: 'Суммарная длительность, сек', value: (row) => row.duration_sum },
    { header: 'Дозвон по звонкам 10+ сек', value: (row) => row.connect_rate_calls_10 },
    { header: 'Дозвон по телефонам 10+ сек', value: (row) => row.connect_rate_phones_10 },
  ];
}

function callabilityColumns(dateHeader) {
  return [
    { header: dateHeader, value: (row) => row.date },
    { header: 'first_upload_id', value: (row) => row.first_upload_id },
    ...callabilityMetricColumns(),
  ];
}

function percentValue(value) {
  const text = String(value ?? '').trim();
  if (!text) return '';
  return Number(text.replace('%', '').replace(',', '.')) / 100;
}

function ratioValue(numerator, denominator) {
  if (!denominator) return '';
  return numerator / denominator;
}

function uploadVolume(row) {
  return Number(row.upload_lead_count ?? row.row_count ?? row.unique_phone_count ?? 0);
}

function newBaseVolume(row) {
  return Number(row.new_phone_count ?? 0);
}

function yesNo(value) {
  return value ? 'Да' : 'Нет';
}

function tableValues(rows, columns) {
  return [
    columns.map((column) => column.header),
    ...rows.map((row) => columns.map((column) => column.value(row))),
  ];
}

function columnFormats(columns) {
  return columns
    .map((column, index) => ({ index, format: column.format }))
    .filter((item) => item.format);
}

function baseSheetColumns() {
  return [
    { header: 'Дата загрузки', value: (row) => row.upload_date, format: 'date' },
    { header: 'База / сегмент', value: (row) => row.utm_content || row.upload_id },
    { header: 'Источник', value: (row) => row.utm_source },
    { header: 'Канал', value: (row) => row.utm_medium },
    { header: 'Кампания / город', value: (row) => row.utm_campaign },
    { header: 'Объем загрузки', value: (row) => uploadVolume(row), format: 'integer' },
    { header: 'Уникальных телефонов в загрузке', value: (row) => row.unique_phone_count, format: 'integer' },
    { header: 'Новая база', value: (row) => row.new_phone_count, format: 'integer' },
    { header: 'Перезаливы', value: (row) => row.reload_phone_count, format: 'integer' },
    { header: 'Дубли в файле', value: (row) => row.duplicate_in_file_count, format: 'integer' },
    { header: 'Лидов в Битриксе', value: (row) => row.bitrix_lead_count, format: 'integer' },
    { header: 'В работе', value: (row) => row.working_phone_count, format: 'integer' },
    { header: 'Проиграно', value: (row) => row.lost_phone_count, format: 'integer' },
    { header: 'Сконвертировано телефонов', value: (row) => row.converted_phone_count, format: 'integer' },
    { header: 'Сконвертировано лидов', value: (row) => row.converted_lead_count, format: 'integer' },
    { header: 'CR по телефонам', value: (row) => percentValue(row.cr_by_phone), format: 'percent' },
    { header: 'CR по лидам', value: (row) => percentValue(row.cr_by_lead), format: 'percent' },
    { header: 'Звонков', value: (row) => row.calls_total, format: 'integer' },
    { header: 'Телефонов в звонках', value: (row) => row.calls_unique_phones, format: 'integer' },
    { header: 'Звонков 10+ сек', value: (row) => row.calls_duration_gte_10, format: 'integer' },
    { header: 'Дозвон по звонкам 10+ сек', value: (row) => percentValue(row.callability_by_calls_10), format: 'percent' },
    { header: 'Дозвон по телефонам 10+ сек', value: (row) => percentValue(row.callability_by_phones_10), format: 'percent' },
    { header: 'upload_id', value: (row) => row.upload_id },
    { header: 'utm_term', value: (row) => row.utm_term },
  ];
}

function callabilitySheetColumns(firstColumnHeader = 'Дата') {
  return [
    { header: firstColumnHeader, value: (row) => row.date ?? row.first_upload_id, format: firstColumnHeader === 'Дата' ? 'date' : undefined },
    { header: 'База', value: (row) => row.utm_content || row.first_upload_id },
    { header: 'Звонков', value: (row) => row.total, format: 'integer' },
    { header: 'Уникальных телефонов', value: (row) => row.unique_phone_count, format: 'integer' },
    { header: 'Звонков >0 сек', value: (row) => row.duration_gt_0, format: 'integer' },
    { header: 'Звонков 10+ сек', value: (row) => row.duration_gte_10, format: 'integer' },
    { header: 'Звонков 30+ сек', value: (row) => row.duration_gte_30, format: 'integer' },
    { header: 'Телефонов >0 сек', value: (row) => row.phones_gt_0, format: 'integer' },
    { header: 'Телефонов 10+ сек', value: (row) => row.phones_gte_10, format: 'integer' },
    { header: 'Телефонов 30+ сек', value: (row) => row.phones_gte_30, format: 'integer' },
    { header: 'Успешные по Скорозвону', value: (row) => row.successful_by_group, format: 'integer' },
    { header: 'Суммарная длительность, сек', value: (row) => row.duration_sum, format: 'integer' },
    { header: 'Дозвон по звонкам 10+ сек', value: (row) => percentValue(row.connect_rate_calls_10), format: 'percent' },
    { header: 'Дозвон по телефонам 10+ сек', value: (row) => percentValue(row.connect_rate_phones_10), format: 'percent' },
    { header: 'first_upload_id', value: (row) => row.first_upload_id },
  ];
}

function detailSheetColumns() {
  return [
    { header: 'Телефон', value: (row) => row.phone },
    { header: 'Новая база', value: (row) => yesNo(row.is_new_base) },
    { header: 'Перезалив', value: (row) => yesNo(row.is_reload) },
    { header: 'Дубль в файле', value: (row) => yesNo(row.is_duplicate_in_file) },
    { header: 'Сконвертирован', value: (row) => row.converted === 'yes' ? 'Да' : 'Нет' },
    { header: 'Статусы Битрикс', value: (row) => row.bitrix_statuses },
    { header: 'ID лидов Битрикс', value: (row) => row.bitrix_lead_ids },
    { header: 'Ответственный', value: (row) => row.responsible },
    { header: 'Источник', value: (row) => row.source },
    { header: 'Название лида', value: (row) => row.title },
    { header: 'upload_id', value: (row) => row.upload_id },
    { header: 'first_upload_id', value: (row) => row.first_upload_id },
    { header: 'utm_medium', value: (row) => row.utm_medium },
    { header: 'utm_source', value: (row) => row.utm_source },
    { header: 'utm_campaign', value: (row) => row.utm_campaign },
    { header: 'utm_content', value: (row) => row.utm_content },
    { header: 'utm_term', value: (row) => row.utm_term },
  ];
}

function buildSummaryValues(baseRows, byBaseRows) {
  const knownCallRows = byBaseRows.filter((row) => row.first_upload_id !== 'unmatched');
  const unmatched = byBaseRows.find((row) => row.first_upload_id === 'unmatched');
  const totalNew = baseRows.reduce((sum, row) => sum + Number(row.new_phone_count || 0), 0);
  const totalReloads = baseRows.reduce((sum, row) => sum + Number(row.reload_phone_count || 0), 0);
  const totalConvertedPhones = baseRows.reduce((sum, row) => sum + Number(row.converted_phone_count || 0), 0);
  const totalConvertedLeads = baseRows.reduce((sum, row) => sum + Number(row.converted_lead_count || 0), 0);
  const knownCalls = knownCallRows.reduce((sum, row) => sum + Number(row.total || 0), 0);
  const knownPhones = knownCallRows.reduce((sum, row) => sum + Number(row.unique_phone_count || 0), 0);
  const knownPhones10 = knownCallRows.reduce((sum, row) => sum + Number(row.phones_gte_10 || 0), 0);
  const recentRows = [...baseRows]
    .sort((a, b) => `${b.upload_date}_${b.upload_id}`.localeCompare(`${a.upload_date}_${a.upload_id}`))
    .slice(0, 10);

  const values = [
    ['Отчетики', '', 'Обновлено', new Date().toLocaleString('ru-RU')],
    [''],
    ['Ключевые показатели'],
    ['Метрика', 'Значение', 'Комментарий'],
    ['Загрузок', baseRows.length, 'Количество импортированных файлов/заливок'],
    ['Новая база', totalNew, 'Телефоны, впервые попавшие в реестр'],
    ['Перезаливы', totalReloads, 'Телефоны, которые уже были в реестре'],
    ['Сконвертировано телефонов', totalConvertedPhones, 'Уникальные телефоны со сконвертированным лидом'],
    ['Сконвертировано лидов', totalConvertedLeads, 'Количество сконвертированных лидов Битрикса'],
    ['CR по телефонам', ratioValue(totalConvertedPhones, totalNew), 'Сконвертированные телефоны / новая база'],
    ['Звонков по известным базам', knownCalls, 'Звонки Скорозвона, телефон которых найден в реестре'],
    ['Дозвон по телефонам 10+ сек', ratioValue(knownPhones10, knownPhones), 'Телефоны с разговором 10+ сек / телефоны в звонках'],
    ['Звонков без привязки к базе', unmatched?.total ?? 0, 'Эти номера еще не импортированы через файлы баз'],
    [''],
    ['Последние загрузки'],
    ['Дата', 'База / сегмент', 'Новая база', 'Перезаливы', 'CR телефоны', 'Дозвон телефоны 10+ сек', 'upload_id'],
  ];

  for (const row of recentRows) {
    values.push([
      row.upload_date,
      row.utm_content || row.upload_id,
      row.new_phone_count,
      row.reload_phone_count,
      percentValue(row.cr_by_phone),
      percentValue(row.callability_by_phones_10),
      row.upload_id,
    ]);
  }

  return values;
}

function russianMonth(dateIso) {
  const monthNames = [
    'Январь',
    'Февраль',
    'Март',
    'Апрель',
    'Май',
    'Июнь',
    'Июль',
    'Август',
    'Сентябрь',
    'Октябрь',
    'Ноябрь',
    'Декабрь',
  ];
  const month = Number(String(dateIso || '').slice(5, 7));
  return monthNames[month - 1] || '';
}

function buildSourceSummaryRows(baseRows) {
  const groups = new Map();

  for (const row of baseRows.filter((item) => item.source_type !== 'bitrix_only')) {
    const key = stableJson({
      period: russianMonth(row.upload_date),
      source: row.utm_source || row.utm_medium || 'Без источника',
      segment: row.utm_content || 'Без сегмента',
    });

    if (!groups.has(key)) {
      groups.set(key, {
        period: russianMonth(row.upload_date),
        source: row.utm_source || row.utm_medium || 'Без источника',
        segment: row.utm_content || 'Без сегмента',
        uploadVolume: 0,
        newBase: 0,
        converted: 0,
      });
    }

    const group = groups.get(key);
    group.uploadVolume += uploadVolume(row);
    group.newBase += newBaseVolume(row);
    group.converted += Number(row.converted_lead_count || 0);
  }

  return [...groups.values()]
    .sort((a, b) => `${a.period}_${a.source}_${a.segment}`.localeCompare(`${b.period}_${b.source}_${b.segment}`))
    .map((row) => ({
      ...row,
      cr: ratioValue(row.converted, row.newBase),
    }));
}

function buildIndicatorsValues(baseRows) {
  const sortedBaseRows = [...baseRows].sort((a, b) => `${a.upload_date}_${a.upload_id}`.localeCompare(`${b.upload_date}_${b.upload_id}`));
  const summaryRows = buildSourceSummaryRows(sortedBaseRows);
  const values = [
    [
      '№',
      'Метки',
      '',
      '',
      '',
      '',
      'База',
      'Дата создания',
      'Объем загрузки',
      'Новая база',
      'В работе',
      'Проиграно',
      'Сконвертировано',
      'CR',
      '',
      'Итог по источникам',
      '',
      '',
      '',
      '',
      '',
      '',
    ],
    [
      '',
      'utm medium',
      'utm source',
      'utm campaign',
      'utm content',
      'utm term',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      '',
      'Период',
      'Источник',
      'Сегмент',
      'Суммарный объем загрузки',
      'Новая база',
      'Сконвертировано лидов',
      'CR новой базы',
    ],
  ];

  const maxRows = Math.max(sortedBaseRows.length, summaryRows.length);
  for (let index = 0; index < maxRows; index += 1) {
    const base = sortedBaseRows[index];
    const summary = summaryRows[index];
    const row = Array.from({ length: 22 }, () => '');

    if (base) {
      row[0] = index + 1;
      row[1] = base.utm_medium || '—';
      row[2] = base.utm_source || '—';
      row[3] = base.utm_campaign || '—';
      row[4] = base.utm_content || '—';
      row[5] = base.utm_term || '—';
      row[6] = baseLabel(base.utm_content || base.utm_source || base.utm_medium);
      row[7] = base.upload_date;
      row[8] = uploadVolume(base);
      row[9] = base.new_phone_count;
      row[10] = base.working_phone_count;
      row[11] = base.lost_phone_count;
      row[12] = base.converted_lead_count;
      row[13] = percentValue(base.cr_by_lead);
    }

    if (summary) {
      row[15] = summary.period;
      row[16] = summary.source;
      row[17] = summary.segment;
      row[18] = summary.uploadVolume;
      row[19] = summary.newBase;
      row[20] = summary.converted;
      row[21] = summary.cr;
    }

    values.push(row);
  }

  return values;
}

function buildReadableCallabilityRows(byBaseRows) {
  return byBaseRows
    .filter((row) => row.first_upload_id !== 'unmatched')
    .map((row) => ({
      date: row.upload_date,
      base: row.utm_content || row.first_upload_id,
      calls: row.total,
      uniquePhones: row.unique_phone_count,
      calls10: row.duration_gte_10,
      phones10: row.phones_gte_10,
      calls30: row.duration_gte_30,
      durationSum: row.duration_sum,
      callRate: ratioValue(row.phones_gte_10, row.unique_phone_count),
    }));
}

function buildAllCallabilityRows(db) {
  const groups = new Map();

  for (const call of Object.values(db.skorozvon_calls)) {
    if (!call.date) continue;
    if (!inReportPeriod(db, call.date)) continue;
    if (!groups.has(call.date)) groups.set(call.date, []);
    groups.get(call.date).push(call);
  }

  return [...groups.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, calls]) => {
      const stats = aggregateCalls(calls);
      return {
        date,
        calls: stats.total,
        uniquePhones: stats.unique_phone_count,
        calls10: stats.duration_gte_10,
        phones10: stats.phones_gte_10,
        calls30: stats.duration_gte_30,
        durationSum: stats.duration_sum,
        callRate: ratioValue(stats.phones_gte_10, stats.unique_phone_count),
      };
    });
}

function buildReadableCallabilityValues(db) {
  const rows = buildAllCallabilityRows(db);
  const totals = rows.reduce((acc, row) => {
    acc.calls += Number(row.calls || 0);
    acc.uniquePhones += Number(row.uniquePhones || 0);
    acc.calls10 += Number(row.calls10 || 0);
    acc.phones10 += Number(row.phones10 || 0);
    acc.calls30 += Number(row.calls30 || 0);
    acc.durationSum += Number(row.durationSum || 0);
    return acc;
  }, {
    calls: 0,
    uniquePhones: 0,
    calls10: 0,
    phones10: 0,
    calls30: 0,
    durationSum: 0,
  });

  const values = [
    ['Дозваниваемость'],
    ['Дата', 'Звонков', 'Уникальных номеров', 'Дозвонов 10+ сек', 'Дозваниваемость', 'Разговоров 30+ сек', 'Суммарная длительность, сек'],
    ...rows.map((row) => [
      row.date,
      row.calls,
      row.uniquePhones,
      row.phones10,
      row.callRate,
      row.calls30,
      row.durationSum,
    ]),
  ];

  if (rows.length > 0) {
    values.push([
      'Итого',
      totals.calls,
      totals.uniquePhones,
      totals.phones10,
      ratioValue(totals.phones10, totals.uniquePhones),
      totals.calls30,
      totals.durationSum,
    ]);
  }

  return values;
}

function buildGoogleWorksheets(db) {
  const baseRows = buildBaseReportRows(db);
  const dailyRows = buildCallabilityDailyRows(db);
  const byBaseRows = buildCallabilityByBaseRows(db);
  const detailRows = buildUploadItemsRows(db);
  const baseColumns = baseSheetColumns();
  const dailyColumns = callabilitySheetColumns('Дата');
  const byBaseColumns = callabilitySheetColumns('first_upload_id');
  const detailColumns = detailSheetColumns();

  return [
    {
      title: 'Показатели',
      values: buildIndicatorsValues(baseRows),
      frozenRows: 2,
      headerRows: [0, 1],
      merges: [
        { startRow: 0, endRow: 2, startColumn: 0, endColumn: 1 },
        { startRow: 0, endRow: 1, startColumn: 1, endColumn: 6 },
        { startRow: 0, endRow: 2, startColumn: 6, endColumn: 7 },
        { startRow: 0, endRow: 2, startColumn: 7, endColumn: 8 },
        { startRow: 0, endRow: 2, startColumn: 8, endColumn: 9 },
        { startRow: 0, endRow: 2, startColumn: 9, endColumn: 10 },
        { startRow: 0, endRow: 2, startColumn: 10, endColumn: 11 },
        { startRow: 0, endRow: 2, startColumn: 11, endColumn: 12 },
        { startRow: 0, endRow: 2, startColumn: 12, endColumn: 13 },
        { startRow: 0, endRow: 2, startColumn: 13, endColumn: 14 },
        { startRow: 0, endRow: 1, startColumn: 15, endColumn: 22 },
      ],
      columnFormats: [
        { index: 7, format: 'date', startRowIndex: 2 },
        { index: 8, format: 'integer', startRowIndex: 2 },
        { index: 9, format: 'integer', startRowIndex: 2 },
        { index: 10, format: 'integer', startRowIndex: 2 },
        { index: 11, format: 'integer', startRowIndex: 2 },
        { index: 12, format: 'integer', startRowIndex: 2 },
        { index: 13, format: 'percent', startRowIndex: 2 },
        { index: 18, format: 'integer', startRowIndex: 2 },
        { index: 19, format: 'integer', startRowIndex: 2 },
        { index: 20, format: 'integer', startRowIndex: 2 },
        { index: 21, format: 'percent', startRowIndex: 2 },
      ],
    },
    {
      title: 'Дозваниваемость',
      values: buildReadableCallabilityValues(db),
      frozenRows: 2,
      headerRows: [0, 1],
      columnFormats: [
        { index: 0, format: 'date', startRowIndex: 2 },
        { index: 1, format: 'integer', startRowIndex: 2 },
        { index: 2, format: 'integer', startRowIndex: 2 },
        { index: 3, format: 'integer', startRowIndex: 2 },
        { index: 4, format: 'percent', startRowIndex: 2 },
        { index: 5, format: 'integer', startRowIndex: 2 },
        { index: 6, format: 'integer', startRowIndex: 2 },
      ],
    },
    {
      title: 'Методика',
      values: [
        ['Метрика', 'Как считается'],
        ['Объем загрузки', 'Лиды Битрикса с источником загрузки и указанными первичными UTM-метками. Дата выгрузки берется из utm_term, если там указана дата, иначе из даты создания лида.'],
        ['Новая база', 'Уникальные телефоны, у которых первый найденный лид Битрикса относится к этой выгрузке.'],
        ['В работе', 'Телефоны из новой базы, у которых есть лид Битрикса не в финальном успешном и не в финальном проигранном статусе.'],
        ['Проиграно', 'Телефоны из новой базы, у которых все найденные лиды находятся в проигранных статусах.'],
        ['Сконвертировано', 'Количество сконвертированных лидов Битрикса, закрепленных за первой базой телефона.'],
        ['CR', 'Сконвертировано / Новая база.'],
        ['Дозваниваемость', 'Уникальные телефоны с разговором 10 секунд и больше / уникальные телефоны, по которым были звонки.'],
        ['Пустые метки', 'Лиды без первичных UTM-меток не попадают в отчет по базам.'],
        ['Перезаливы', 'Повторные появления номера скрыты из основного отчета и хранятся на технических листах.'],
      ],
      frozenRows: 1,
      headerRows: [0],
    },
    {
      title: 'Тех. базы',
      values: tableValues(baseRows, baseColumns),
      frozenRows: 1,
      filter: true,
      columnFormats: columnFormats(baseColumns),
      hidden: true,
    },
    {
      title: 'Тех. дозвон по дням',
      values: tableValues(dailyRows, dailyColumns),
      frozenRows: 1,
      filter: true,
      columnFormats: columnFormats(dailyColumns),
      hidden: true,
    },
    {
      title: 'Тех. дозвон по базам',
      values: tableValues(byBaseRows, byBaseColumns),
      frozenRows: 1,
      filter: true,
      columnFormats: columnFormats(byBaseColumns),
      hidden: true,
    },
    {
      title: 'Тех. детализация',
      values: tableValues(detailRows, detailColumns),
      frozenRows: 1,
      filter: true,
      columnFormats: columnFormats(detailColumns),
      hidden: true,
    },
  ];
}

function getGoogleConfig() {
  const env = loadEnvFile('google.env');
  const jsonValue = env.GOOGLE_SERVICE_ACCOUNT_JSON;
  let credentials = null;

  if (env.GOOGLE_SERVICE_ACCOUNT_JSON_BASE64) {
    credentials = JSON.parse(Buffer.from(env.GOOGLE_SERVICE_ACCOUNT_JSON_BASE64, 'base64').toString('utf8'));
  } else if (jsonValue?.trim().startsWith('{')) {
    credentials = JSON.parse(jsonValue);
  } else if (jsonValue && existsSync(jsonValue)) {
    credentials = JSON.parse(readFileSync(jsonValue, 'utf8'));
  } else if (env.GOOGLE_CLIENT_EMAIL && env.GOOGLE_PRIVATE_KEY) {
    credentials = {
      client_email: env.GOOGLE_CLIENT_EMAIL,
      private_key: env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
    };
  }

  if (!credentials?.client_email || !credentials?.private_key) {
    throw new Error('Google credentials are missing. Create google.env from google.env.example.');
  }

  return {
    credentials,
    spreadsheetId: env.GOOGLE_SPREADSHEET_ID || '',
    title: env.GOOGLE_SPREADSHEET_TITLE || 'Отчетики',
    shareEmail: env.GOOGLE_SHARE_EMAIL || '',
  };
}

function googleAuth(credentials) {
  return new google.auth.GoogleAuth({
    credentials,
    scopes: [
      'https://www.googleapis.com/auth/spreadsheets',
      'https://www.googleapis.com/auth/drive',
    ],
  });
}

async function ensureSpreadsheet(sheets, drive, config, worksheets) {
  if (config.spreadsheetId) {
    const response = await sheets.spreadsheets.get({ spreadsheetId: config.spreadsheetId });
    return {
      spreadsheetId: config.spreadsheetId,
      spreadsheetUrl: response.data.spreadsheetUrl,
      metadata: response.data,
      created: false,
    };
  }

  const response = await sheets.spreadsheets.create({
    requestBody: {
      properties: { title: config.title },
      sheets: worksheets.map((worksheet) => ({
        properties: {
          title: worksheet.title,
          gridProperties: {
            rowCount: Math.max(100, worksheet.values.length + 10),
            columnCount: Math.max(20, worksheet.values[0]?.length ?? 1),
          },
        },
      })),
    },
  });

  const spreadsheetId = response.data.spreadsheetId;

  if (config.shareEmail) {
    await drive.permissions.create({
      fileId: spreadsheetId,
      requestBody: {
        type: 'user',
        role: 'writer',
        emailAddress: config.shareEmail,
      },
      sendNotificationEmail: false,
    });
  }

  const metadata = await sheets.spreadsheets.get({ spreadsheetId });
  return {
    spreadsheetId,
    spreadsheetUrl: response.data.spreadsheetUrl,
    metadata: metadata.data,
    created: true,
  };
}

async function ensureWorksheetTabs(sheets, spreadsheetId, metadata, worksheets) {
  const existingTitles = new Set((metadata.sheets ?? []).map((sheet) => sheet.properties.title));
  const requests = worksheets
    .filter((worksheet) => !existingTitles.has(worksheet.title))
    .map((worksheet) => ({
      addSheet: {
        properties: {
          title: worksheet.title,
          gridProperties: {
            rowCount: Math.max(100, worksheet.values.length + 10),
            columnCount: Math.max(20, worksheet.values[0]?.length ?? 1),
          },
        },
      },
    }));

  if (requests.length > 0) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId,
      requestBody: { requests },
    });
  }

  const refreshed = await sheets.spreadsheets.get({
    spreadsheetId,
    includeGridData: false,
  });
  return refreshed.data;
}

function sheetIdByTitle(metadata) {
  const result = new Map();
  for (const sheet of metadata.sheets ?? []) {
    result.set(sheet.properties.title, sheet.properties.sheetId);
  }
  return result;
}

function obsoleteReportSheetTitles() {
  return new Set([
    'Сводка',
    'Базы',
    'Дозвон по дням',
    'Дозвон по базам',
    'Детализация',
    'Лист1',
    'Sheet1',
  ]);
}

function hideObsoleteWorksheetRequests(metadata, worksheets) {
  const currentTitles = new Set(worksheets.map((worksheet) => worksheet.title));
  const obsoleteTitles = obsoleteReportSheetTitles();
  const requests = [];

  for (const sheet of metadata.sheets ?? []) {
    const title = sheet.properties.title;
    if (!obsoleteTitles.has(title) || currentTitles.has(title) || sheet.properties.hidden) continue;

    requests.push({
      updateSheetProperties: {
        properties: {
          sheetId: sheet.properties.sheetId,
          hidden: true,
        },
        fields: 'hidden',
      },
    });
  }

  return requests;
}

function color(hex) {
  const normalized = hex.replace('#', '');
  return {
    red: parseInt(normalized.slice(0, 2), 16) / 255,
    green: parseInt(normalized.slice(2, 4), 16) / 255,
    blue: parseInt(normalized.slice(4, 6), 16) / 255,
  };
}

function numberFormat(format) {
  if (format === 'percent') return { type: 'PERCENT', pattern: '0.00%' };
  if (format === 'integer') return { type: 'NUMBER', pattern: '#,##0' };
  if (format === 'date') return { type: 'DATE', pattern: 'yyyy-mm-dd' };
  return null;
}

function formatRequestsForWorksheet(sheetId, worksheet, desiredIndex) {
  const rowCount = Math.max(1, worksheet.values.length);
  const columnCount = Math.max(1, worksheet.values[0]?.length ?? 1);
  const requests = [
    {
      updateSheetProperties: {
        properties: {
          sheetId,
          index: desiredIndex,
          gridProperties: {
            frozenRowCount: worksheet.frozenRows ?? 1,
            rowCount: Math.max(100, rowCount + 10),
            columnCount: Math.max(20, columnCount),
          },
          hidden: Boolean(worksheet.hidden),
        },
        fields: 'index,gridProperties.frozenRowCount,gridProperties.rowCount,gridProperties.columnCount,hidden',
      },
    },
    {
      repeatCell: {
        range: {
          sheetId,
          startRowIndex: 0,
          endRowIndex: Math.max(100, rowCount + 10),
          startColumnIndex: 0,
          endColumnIndex: Math.max(20, columnCount),
        },
        cell: {
          userEnteredFormat: {
            backgroundColor: color('#FFFFFF'),
            textFormat: { foregroundColor: color('#111827'), fontSize: 10 },
            wrapStrategy: 'WRAP',
            verticalAlignment: 'MIDDLE',
          },
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat,wrapStrategy,verticalAlignment)',
      },
    },
    {
      autoResizeDimensions: {
        dimensions: {
          sheetId,
          dimension: 'COLUMNS',
          startIndex: 0,
          endIndex: columnCount,
        },
      },
    },
  ];

  if (worksheet.filter && rowCount > 1) {
    requests.push({ clearBasicFilter: { sheetId } });
    requests.push({
      setBasicFilter: {
        filter: {
          range: {
            sheetId,
            startRowIndex: 0,
            endRowIndex: rowCount,
            startColumnIndex: 0,
            endColumnIndex: columnCount,
          },
        },
      },
    });
  }

  if (worksheet.merges?.length) {
    requests.push({
      unmergeCells: {
        range: {
          sheetId,
          startRowIndex: 0,
          endRowIndex: rowCount,
          startColumnIndex: 0,
          endColumnIndex: columnCount,
        },
      },
    });

    for (const item of worksheet.merges) {
      requests.push({
        mergeCells: {
          mergeType: 'MERGE_ALL',
          range: {
            sheetId,
            startRowIndex: item.startRow,
            endRowIndex: item.endRow,
            startColumnIndex: item.startColumn,
            endColumnIndex: item.endColumn,
          },
        },
      });
    }
  }

  for (const headerRow of worksheet.headerRows ?? [0]) {
    requests.push({
      repeatCell: {
        range: {
          sheetId,
          startRowIndex: headerRow,
          endRowIndex: headerRow + 1,
          startColumnIndex: 0,
          endColumnIndex: columnCount,
        },
        cell: {
          userEnteredFormat: {
            backgroundColor: headerRow === 0 ? color('#111827') : color('#E5E7EB'),
            textFormat: {
              bold: true,
              foregroundColor: headerRow === 0 ? color('#FFFFFF') : color('#111827'),
              fontSize: headerRow === 0 && worksheet.title === 'Сводка' ? 14 : 10,
            },
            horizontalAlignment: headerRow === 0 && worksheet.title === 'Сводка' ? 'LEFT' : 'CENTER',
            wrapStrategy: 'WRAP',
          },
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,wrapStrategy)',
      },
    });
  }

  for (const item of worksheet.columnFormats ?? []) {
    const format = numberFormat(item.format);
    if (!format) continue;

    requests.push({
      repeatCell: {
        range: {
          sheetId,
          startRowIndex: item.startRowIndex ?? 1,
          endRowIndex: item.endRowIndex ?? rowCount,
          startColumnIndex: item.index,
          endColumnIndex: item.index + 1,
        },
        cell: {
          userEnteredFormat: {
            numberFormat: format,
          },
        },
        fields: 'userEnteredFormat.numberFormat',
      },
    });
  }

  return requests;
}

function quotedSheetTitle(title) {
  return `'${String(title).replace(/'/g, "''")}'`;
}

async function syncGoogleSheets(db) {
  const config = getGoogleConfig();
  const auth = googleAuth(config.credentials);
  const sheets = google.sheets({ version: 'v4', auth });
  const drive = google.drive({ version: 'v3', auth });
  const worksheets = buildGoogleWorksheets(db);
  const spreadsheet = await ensureSpreadsheet(sheets, drive, config, worksheets);
  const metadata = await ensureWorksheetTabs(sheets, spreadsheet.spreadsheetId, spreadsheet.metadata, worksheets);
  const idsByTitle = sheetIdByTitle(metadata);

  for (const worksheet of worksheets) {
    await sheets.spreadsheets.values.clear({
      spreadsheetId: spreadsheet.spreadsheetId,
      range: `${quotedSheetTitle(worksheet.title)}!A:ZZ`,
    });
    await sheets.spreadsheets.values.update({
      spreadsheetId: spreadsheet.spreadsheetId,
      range: `${quotedSheetTitle(worksheet.title)}!A1`,
      valueInputOption: 'USER_ENTERED',
      requestBody: { values: worksheet.values },
    });
  }

  const requests = [
    ...hideObsoleteWorksheetRequests(metadata, worksheets),
    ...worksheets.flatMap((worksheet, index) => {
    const sheetId = idsByTitle.get(worksheet.title);
    if (sheetId === undefined) return [];
    return formatRequestsForWorksheet(sheetId, worksheet, index);
    }),
  ];

  if (requests.length > 0) {
    await sheets.spreadsheets.batchUpdate({
      spreadsheetId: spreadsheet.spreadsheetId,
      requestBody: { requests },
    });
  }

  return {
    spreadsheetId: spreadsheet.spreadsheetId,
    spreadsheetUrl: spreadsheet.spreadsheetUrl,
    created: spreadsheet.created,
  };
}

async function generateReports(db, reportsDir) {
  const baseRows = buildBaseReportRows(db);
  await writeCsv(path.join(reportsDir, 'base_report.csv'), baseRows, [
    { header: 'Дата загрузки', value: (row) => row.upload_date },
    { header: 'upload_id', value: (row) => row.upload_id },
    { header: 'utm_medium', value: (row) => row.utm_medium },
    { header: 'utm_source', value: (row) => row.utm_source },
    { header: 'utm_campaign', value: (row) => row.utm_campaign },
    { header: 'utm_content', value: (row) => row.utm_content },
    { header: 'utm_term', value: (row) => row.utm_term },
    { header: 'Объем загрузки', value: (row) => uploadVolume(row) },
    { header: 'Уникальных телефонов в загрузке', value: (row) => row.unique_phone_count },
    { header: 'Новая база', value: (row) => row.new_phone_count },
    { header: 'Перезаливы', value: (row) => row.reload_phone_count },
    { header: 'Дубли внутри файла', value: (row) => row.duplicate_in_file_count },
    { header: 'Лидов в Битриксе', value: (row) => row.bitrix_lead_count },
    { header: 'В работе, телефонов', value: (row) => row.working_phone_count },
    { header: 'Проиграно, телефонов', value: (row) => row.lost_phone_count },
    { header: 'Сконвертировано телефонов', value: (row) => row.converted_phone_count },
    { header: 'Сконвертировано лидов', value: (row) => row.converted_lead_count },
    { header: 'CR по телефонам', value: (row) => row.cr_by_phone },
    { header: 'CR по лидам', value: (row) => row.cr_by_lead },
    { header: 'Звонков', value: (row) => row.calls_total },
    { header: 'Уникальных телефонов в звонках', value: (row) => row.calls_unique_phones },
    { header: 'Звонков 10+ сек', value: (row) => row.calls_duration_gte_10 },
    { header: 'Дозвон по звонкам 10+ сек', value: (row) => row.callability_by_calls_10 },
    { header: 'Дозвон по телефонам 10+ сек', value: (row) => row.callability_by_phones_10 },
  ]);

  const dailyRows = buildCallabilityDailyRows(db);
  await writeCsv(path.join(reportsDir, 'callability_daily.csv'), dailyRows, callabilityColumns('Дата'));

  const byBaseRows = buildCallabilityByBaseRows(db);
  await writeCsv(path.join(reportsDir, 'callability_by_base.csv'), byBaseRows, [
    { header: 'first_upload_id', value: (row) => row.first_upload_id },
    { header: 'Дата загрузки', value: (row) => row.upload_date },
    { header: 'utm_medium', value: (row) => row.utm_medium },
    { header: 'utm_source', value: (row) => row.utm_source },
    { header: 'utm_campaign', value: (row) => row.utm_campaign },
    { header: 'utm_content', value: (row) => row.utm_content },
    ...callabilityMetricColumns(),
  ]);

  const itemRows = buildUploadItemsRows(db);
  await writeCsv(path.join(reportsDir, 'upload_items.csv'), itemRows, [
    { header: 'upload_id', value: (row) => row.upload_id },
    { header: 'first_upload_id', value: (row) => row.first_upload_id },
    { header: 'Строка файла', value: (row) => row.row_number },
    { header: 'Телефон', value: (row) => row.phone },
    { header: 'Новая база', value: (row) => row.is_new_base ? 'yes' : 'no' },
    { header: 'Перезалив', value: (row) => row.is_reload ? 'yes' : 'no' },
    { header: 'Дубль внутри файла', value: (row) => row.is_duplicate_in_file ? 'yes' : 'no' },
    { header: 'Ответственный', value: (row) => row.responsible },
    { header: 'Источник', value: (row) => row.source },
    { header: 'Название лида', value: (row) => row.title },
    { header: 'utm_medium', value: (row) => row.utm_medium },
    { header: 'utm_source', value: (row) => row.utm_source },
    { header: 'utm_campaign', value: (row) => row.utm_campaign },
    { header: 'utm_content', value: (row) => row.utm_content },
    { header: 'utm_term', value: (row) => row.utm_term },
    { header: 'ID лидов Битрикс', value: (row) => row.bitrix_lead_ids },
    { header: 'Статусы Битрикс', value: (row) => row.bitrix_statuses },
    { header: 'Сконвертирован', value: (row) => row.converted },
  ]);

  return {
    baseRows: baseRows.length,
    dailyRows: dailyRows.length,
    byBaseRows: byBaseRows.length,
    itemRows: itemRows.length,
  };
}

function uploadPhones(db, uploadId) {
  return uploadUniquePhones(db, uploadId);
}

async function run() {
  const [command = 'run', ...rest] = process.argv.slice(2);
  const args = parseArgs(rest);
  if (command !== 'run') throw new Error(`Unknown command: ${command}`);

  const dbPath = String(args.db || DEFAULT_DB_PATH);
  const reportsDir = String(args.out || DEFAULT_REPORTS_DIR);
  const db = await loadJson(dbPath, createEmptyDb());
  const hasPeriodArgs = Boolean(
    args['month-current']
    || args.from
    || args.to
    || args['report-from']
    || args['report-to']
    || args['calls-from']
    || args['calls-to']
    || args['calls-date']
  );
  const defaultFrom = args['month-current'] ? monthStartIso(todayIsoDate()) : args.from;
  const defaultTo = args['month-current'] ? todayIsoDate() : args.to;
  const reportFrom = hasPeriodArgs
    ? String(args['report-from'] || defaultFrom || args['calls-from'] || args['calls-date'] || todayIsoDate())
    : String(db.report_context?.from || todayIsoDate());
  const reportTo = hasPeriodArgs
    ? String(args['report-to'] || defaultTo || args['calls-to'] || args['calls-date'] || reportFrom)
    : String(db.report_context?.to || reportFrom);
  const explicitSource = args.source === 'bitrix' || args['bitrix-range']
    ? 'bitrix'
    : (args.upload ? 'uploads' : null);
  db.report_context = {
    source: explicitSource || db.report_context?.source || 'uploads',
    from: reportFrom,
    to: reportTo,
  };
  let activeUpload = null;

  if (args.upload) {
    const result = await importUploadCsv(db, String(args.upload), {
      forceNewUpload: Boolean(args['force-new-upload']),
    });
    activeUpload = result.upload;
    console.log(result.message);
  }

  if (db.report_context.source === 'bitrix' && !args['skip-bitrix']) {
    const result = await fetchBitrixLeadsCreatedRange(db, db.report_context.from, db.report_context.to);
    console.log(`Bitrix range synced: ${result.leadCount} lead(s), ${result.phoneCount} phone(s), period: ${db.report_context.from}..${db.report_context.to}.`);
  }

  if (!args['skip-bitrix'] && db.report_context.source !== 'bitrix') {
    const phones = activeUpload ? uploadPhones(db, activeUpload.upload_id) : Object.keys(db.phones);
    const result = await syncBitrixForPhones(db, phones);
    console.log(`Bitrix synced: ${result.leadCount} lead(s).`);
  }

  if (!args['skip-skorozvon']) {
    const dates = args['calls-today']
      ? [todayIsoDate()]
      : args['calls-date']
        ? [String(args['calls-date'])]
        : args['calls-from'] || db.report_context.from
      ? dateRange(String(args['calls-from'] || db.report_context.from), String(args['calls-to'] || db.report_context.to || args['calls-from'] || db.report_context.from))
      : [todayIsoDate()];
    let totalCalls = 0;

    for (const date of dates) {
      const calls = await fetchSkorozvonCallsForDate(date);
      upsertSkorozvonCalls(db, calls);
      totalCalls += calls.length;
      console.log(`Skorozvon synced: ${calls.length} call(s) for ${date}.`);
    }

    if (dates.length > 1) {
      console.log(`Skorozvon range synced: ${totalCalls} call(s), ${dates[0]}..${dates.at(-1)}.`);
    }
  }

  await saveJson(dbPath, db);
  const reportStats = await generateReports(db, reportsDir);
  console.log(`Reports generated: ${JSON.stringify(reportStats)}.`);

  if (args['google-sheets']) {
    const sheetsResult = await syncGoogleSheets(db);
    console.log(`Google Sheets ${sheetsResult.created ? 'created' : 'updated'}: ${sheetsResult.spreadsheetUrl}`);
  }

  console.log(`Database: ${path.resolve(dbPath)}`);
  console.log(`Reports: ${path.resolve(reportsDir)}`);
}

run().catch((error) => {
  console.error(error.message);
  process.exitCode = 1;
});
