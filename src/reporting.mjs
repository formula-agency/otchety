import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { google } from 'googleapis';
import path from 'node:path';

const DEFAULT_DB_PATH = 'data/reporting-db.json';
const DEFAULT_REPORTS_DIR = 'reports';
const DEFAULT_DASHBOARD_DIR = 'dashboard';
const DEFAULT_REPORT_HISTORY_FROM = '2026-05-01';
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
  { label: 'Подменники', tokens: ['podmenniki', 'podmenniki_tyumen'] },
  { label: 'Телефоны', tokens: ['phone'] },
  { label: 'SMS', tokens: ['sms'] },
  { label: 'Пиксель', tokens: ['pixel'] },
  { label: 'Реанимация сделки', tokens: ['deal-reanimation', 'deal_reanimation', 'reanimation_deal', 'reanimation_formula'] },
  { label: 'Реанимация', tokens: ['reanimation', 'reanim'] },
  { label: 'Карты', tokens: ['maps', 'map'] },
];
const SOURCE_LABELS = [
  { label: 'Media Take', tokens: ['d2'] },
  { label: 'Реанимация', tokens: ['rean'] },
];
const REVISION_STATUS_NAMES = [
  'Перезвонить 30 дн',
  'Долгосрок от 6 мес.',
  'Добрифовать',
  'Прошел бриф',
  'Предконвертация',
];
const REVISION_STATUS_FALLBACK_IDS = new Set(['UC_PZNE6G', 'UC_SUU1DX', 'UC_5IFITJ', 'UC_F4HX04', 'UC_RGNREH']);

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

function resolveDateArg(value, fallback = '') {
  const text = String(value ?? '').trim();
  if (!text) return fallback;
  if (text.toLowerCase() === 'today') return todayIsoDate();
  return text;
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

function normalizeContentKey(value) {
  return safeSegment(String(value ?? '').toLowerCase(), 'empty');
}

function phoneContentRegistryKey(phone, utmContent) {
  return `${String(phone ?? '').trim()}::${normalizeContentKey(utmContent)}`;
}

function baseLabel(value) {
  const raw = String(value ?? '').trim();
  if (!raw) return 'Без меток';

  const normalized = raw.toLowerCase().replace(/[\s-]+/g, '_');
  const match = BASE_LABELS.find((item) => item.tokens.some((token) => normalized.includes(token.toLowerCase().replace(/[\s-]+/g, '_'))));
  return match?.label || raw;
}

function sourceLabel(utmMedium, utmSource = '') {
  const rawMedium = String(utmMedium ?? '').trim();
  const rawSource = String(utmSource ?? '').trim();
  const raw = rawMedium || rawSource;
  if (!raw) return 'Без источника';

  const normalized = raw.toLowerCase().replace(/[\s-]+/g, '_');
  const match = SOURCE_LABELS.find((item) => item.tokens.some((token) => normalized === token.toLowerCase().replace(/[\s-]+/g, '_')));
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

function ensurePhoneRecord(db, phone) {
  if (!db.phones[phone]) {
    db.phones[phone] = {
      phone,
      first_upload_id: '',
      first_seen_at: '',
      first_source_file: '',
      first_bitrix_lead_id: '',
      first_upload_id_by_content: {},
      first_seen_at_by_content: {},
      first_source_file_by_content: {},
      first_bitrix_lead_id_by_content: {},
      bitrix_lead_ids: [],
    };
  }

  db.phones[phone].first_upload_id_by_content ??= {};
  db.phones[phone].first_seen_at_by_content ??= {};
  db.phones[phone].first_source_file_by_content ??= {};
  db.phones[phone].first_bitrix_lead_id_by_content ??= {};
  db.phones[phone].bitrix_lead_ids ??= [];
  return db.phones[phone];
}

function buildPhoneContentRegistry(db) {
  const registry = new Set();

  for (const phoneRecord of Object.values(db.phones)) {
    if (!phoneRecord?.phone) continue;
    for (const contentKey of Object.keys(phoneRecord.first_upload_id_by_content ?? {})) {
      registry.add(`${phoneRecord.phone}::${contentKey}`);
    }
    for (const contentKey of Object.keys(phoneRecord.first_bitrix_lead_id_by_content ?? {})) {
      registry.add(`${phoneRecord.phone}::${contentKey}`);
    }
  }

  for (const item of db.upload_items) {
    if (!item?.phone) continue;
    registry.add(phoneContentRegistryKey(item.phone, item.utm_content));
  }

  for (const lead of Object.values(db.bitrix_leads)) {
    for (const phone of leadPhones(lead)) {
      registry.add(phoneContentRegistryKey(phone, lead.utm_content));
    }
  }

  return registry;
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
    bitrix_stage_history: {},
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
  const registryBeforeImport = buildPhoneContentRegistry(db);
  const uniquePhones = new Set();
  let newPhoneCount = 0;
  let duplicateInFileCount = 0;

  for (const row of rows) {
    const duplicateInFile = seenInFile.has(row.phone);
    if (duplicateInFile) duplicateInFileCount += 1;
    seenInFile.add(row.phone);
    uniquePhones.add(row.phone);

    const contentRegistryKey = phoneContentRegistryKey(row.phone, row.utm_content);
    const existedBefore = registryBeforeImport.has(contentRegistryKey);
    const isNewBase = !existedBefore && !duplicateInFile;
    const phoneRecord = ensurePhoneRecord(db, row.phone);
    const contentKey = normalizeContentKey(row.utm_content);

    if (isNewBase) {
      newPhoneCount += 1;
      if (!phoneRecord.first_upload_id) phoneRecord.first_upload_id = upload.upload_id;
      if (!phoneRecord.first_seen_at) phoneRecord.first_seen_at = upload.upload_date;
      if (!phoneRecord.first_source_file) phoneRecord.first_source_file = upload.source_file;
      phoneRecord.first_upload_id_by_content[contentKey] = upload.upload_id;
      phoneRecord.first_seen_at_by_content[contentKey] = upload.upload_date;
      phoneRecord.first_source_file_by_content[contentKey] = upload.source_file;
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

function unprocessedStatusIds(db) {
  const ids = Object.values(db.bitrix_statuses ?? {})
    .filter((status) => String(status.name || '').trim().toLowerCase() === 'необработанное')
    .map((status) => status.status_id);
  return new Set(ids.length > 0 ? ids : ['IN_PROCESS']);
}

function isUnprocessedStatus(db, statusId) {
  return unprocessedStatusIds(db).has(String(statusId || ''));
}

function normalizeStatusName(value) {
  return String(value || '').trim().toLowerCase().replace(/\s+/g, ' ');
}

function revisionStatusIds(db) {
  const revisionNames = new Set(REVISION_STATUS_NAMES.map(normalizeStatusName));
  const ids = Object.values(db.bitrix_statuses ?? {})
    .filter((status) => revisionNames.has(normalizeStatusName(status.name)))
    .map((status) => status.status_id);
  return new Set(ids.length > 0 ? ids : [...REVISION_STATUS_FALLBACK_IDS]);
}

function isRevisionStatus(db, statusId) {
  return revisionStatusIds(db).has(String(statusId || ''));
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
    const phoneRecord = ensurePhoneRecord(db, phone);
    const contentKey = normalizeContentKey(lead.utm_content);

    const currentFirstLead = db.bitrix_leads[phoneRecord.first_bitrix_lead_id];
    if (!phoneRecord.first_bitrix_lead_id || leadTimestamp(lead) < leadTimestamp(currentFirstLead)) {
      phoneRecord.first_seen_at = leadCreatedDate(lead);
      phoneRecord.first_bitrix_lead_id = lead.id;
    }

    const currentFirstLeadByContent = db.bitrix_leads[phoneRecord.first_bitrix_lead_id_by_content[contentKey]];
    if (!phoneRecord.first_bitrix_lead_id_by_content[contentKey] || leadTimestamp(lead) < leadTimestamp(currentFirstLeadByContent)) {
      phoneRecord.first_seen_at_by_content[contentKey] = leadCreatedDate(lead);
      phoneRecord.first_bitrix_lead_id_by_content[contentKey] = lead.id;
    }

    const ids = new Set(phoneRecord.bitrix_lead_ids ?? []);
    ids.add(lead.id);
    phoneRecord.bitrix_lead_ids = [...ids].sort((a, b) => Number(a) - Number(b));
  }
}

function normalizeBitrixStageHistoryItem(item) {
  return {
    id: String(item.ID),
    owner_id: String(item.OWNER_ID),
    type_id: String(item.TYPE_ID ?? ''),
    status_id: item.STATUS_ID || item.STAGE_ID || '',
    created_time: item.CREATED_TIME || '',
  };
}

function upsertBitrixStageHistory(db, ownerId, items) {
  db.bitrix_stage_history ??= {};
  const existing = new Map((db.bitrix_stage_history[String(ownerId)] ?? []).map((item) => [String(item.id), item]));

  for (const item of items) {
    const normalized = normalizeBitrixStageHistoryItem(item);
    if (!normalized.id) continue;
    existing.set(normalized.id, normalized);
  }

  db.bitrix_stage_history[String(ownerId)] = [...existing.values()]
    .sort((a, b) => `${a.created_time}_${a.id}`.localeCompare(`${b.created_time}_${b.id}`));
}

function stageHistoryCommand(leadId) {
  const params = [
    ['entityTypeId', '1'],
    ['order[ID]', 'ASC'],
    ['filter[OWNER_ID]', String(leadId)],
    ['select[0]', 'ID'],
    ['select[1]', 'OWNER_ID'],
    ['select[2]', 'TYPE_ID'],
    ['select[3]', 'STATUS_ID'],
    ['select[4]', 'CREATED_TIME'],
  ];
  const query = params.map(([key, value]) => `${key}=${encodeURIComponent(value)}`).join('&');
  return `crm.stagehistory.list?${query}`;
}

async function syncBitrixStageHistoryForLeads(db, leadIds) {
  const ids = [...new Set(leadIds.map(String).filter(Boolean))];
  if (ids.length === 0) return { leadCount: 0, historyCount: 0 };

  const webhookUrl = getBitrixWebhookUrl();
  let historyCount = 0;

  for (const group of chunks(ids, BITRIX_BATCH_SIZE)) {
    const commands = {};
    group.forEach((id) => {
      commands[`history_${id}`] = stageHistoryCommand(id);
    });

    const results = await bitrixBatch(webhookUrl, commands);
    for (const id of group) {
      const items = results[`history_${id}`]?.items ?? [];
      upsertBitrixStageHistory(db, id, items);
      historyCount += items.length;
    }
  }

  return { leadCount: ids.length, historyCount };
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
  const stageHistory = await syncBitrixStageHistoryForLeads(db, leads.map((lead) => lead.id));

  return {
    leadCount: leads.length,
    phoneCount: phones.length,
    duplicateLeadCount: duplicates.leadCount,
    stageHistoryCount: stageHistory.historyCount,
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

function historyReportRange(db, fallbackFrom = todayIsoDate(), fallbackTo = fallbackFrom) {
  const dates = [];

  for (const upload of db.uploads ?? []) {
    if (upload?.upload_date) dates.push(upload.upload_date);
  }

  for (const lead of Object.values(db.bitrix_leads ?? {})) {
    const uploadDate = leadUploadDate(lead);
    if (uploadDate) dates.push(uploadDate);
  }

  for (const call of Object.values(db.skorozvon_calls ?? {})) {
    if (call?.date) dates.push(call.date);
  }

  if (dates.length === 0) {
    return { from: fallbackFrom, to: fallbackTo };
  }

  dates.sort();
  const minAllowedFrom = DEFAULT_REPORT_HISTORY_FROM;
  const computedFrom = dates[0] || fallbackFrom;
  return {
    from: computedFrom < minAllowedFrom ? minAllowedFrom : computedFrom,
    to: dates.at(-1) || fallbackTo,
  };
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

function firstLeadForPhoneContent(db, phone, utmContent) {
  const phoneRecord = db.phones[phone];
  const contentKey = normalizeContentKey(utmContent);
  const firstLeadId = phoneRecord?.first_bitrix_lead_id_by_content?.[contentKey];
  if (firstLeadId && db.bitrix_leads[firstLeadId]) {
    return db.bitrix_leads[firstLeadId];
  }

  return leadsForPhone(db, phone)
    .filter((lead) => normalizeContentKey(lead.utm_content) === contentKey)
    .sort((a, b) => leadTimestamp(a) - leadTimestamp(b))[0] ?? null;
}

function firstUploadIdForPhoneContent(db, phone, utmContent) {
  const contentKey = normalizeContentKey(utmContent);
  return db.phones[phone]?.first_upload_id_by_content?.[contentKey] || db.phones[phone]?.first_upload_id || '';
}

function leadStageHistory(db, leadId) {
  return [...(db.bitrix_stage_history?.[String(leadId)] ?? [])]
    .sort((a, b) => `${a.created_time}_${a.id}`.localeCompare(`${b.created_time}_${b.id}`));
}

function leadRoundNumber(db, lead) {
  const history = leadStageHistory(db, lead.id);
  let rounds = 0;
  let previousStatus = '';

  for (const item of history) {
    const statusId = item.status_id || '';
    if (isUnprocessedStatus(db, statusId) && !isUnprocessedStatus(db, previousStatus)) {
      rounds += 1;
    }
    previousStatus = statusId;
  }

  if (rounds === 0 && isUnprocessedStatus(db, lead.status_id)) return 1;
  return rounds;
}

function buildBitrixBaseReportRows(db) {
  const groups = new Map();

  for (const lead of Object.values(db.bitrix_leads)) {
    const uploadDate = leadUploadDate(lead);
    if (!lead || !inReportPeriod(db, uploadDate)) continue;
    if (!hasLeadUtm(lead)) continue;
    if (!isBitrixUploadedLead(lead)) continue;
    const roundNumber = leadRoundNumber(db, lead);

    const key = stableJson({
      date: uploadDate,
      round_number: roundNumber,
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
        round_number: roundNumber,
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
    .sort((a, b) => `${a.upload_date}_${a.utm_medium}_${a.utm_source}_${a.utm_campaign}_${a.utm_content}_${roundSortValue(a.round_number)}`.localeCompare(`${b.upload_date}_${b.utm_medium}_${b.utm_source}_${b.utm_campaign}_${b.utm_content}_${roundSortValue(b.round_number)}`))
    .map((group) => {
      const createdLeads = group.leads;
      const phones = [...new Set(createdLeads.flatMap((lead) => leadPhones(lead)).filter(Boolean))];
      const createdLeadIds = new Set(createdLeads.map((lead) => lead.id));
      const newPhones = phones.filter((phone) => {
        const firstLead = firstLeadForPhoneContent(db, phone, group.utm_content);
        return firstLead && createdLeadIds.has(firstLead.id);
      });
      const convertedLeads = createdLeads.filter((lead) => isConvertedLead(db, lead));
      const lostLeads = createdLeads.filter((lead) => isLostLead(db, lead));
      const revisionLeads = createdLeads.filter((lead) => isRevisionStatus(db, lead.status_id));
      const workingLeads = createdLeads.filter((lead) => !isConvertedLead(db, lead) && !isLostLead(db, lead) && !isRevisionStatus(db, lead.status_id));
      const convertedPhones = phones.filter((phone) => createdLeads.some((lead) => leadPhones(lead).includes(phone) && isConvertedLead(db, lead)));

      return {
        source_type: 'bitrix_api',
        upload_id: '',
        upload_date: group.upload_date,
        round_number: group.round_number,
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
        working_phone_count: workingLeads.length,
        revision_lead_count: revisionLeads.length,
        lost_phone_count: lostLeads.length,
        converted_phone_count: convertedPhones.length,
        converted_lead_count: convertedLeads.length,
        cr_by_phone: percent(convertedPhones.length, phones.length),
        cr_by_lead: percent(convertedLeads.length, createdLeads.length),
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
    const leadIds = [...new Set(phones.flatMap((phone) => phoneLeadIds(db, phone)))];
    const leads = leadIds.map((id) => db.bitrix_leads[id]).filter(Boolean);
    const convertedLeads = leads.filter((lead) => isConvertedLead(db, lead));
    const convertedPhones = phones.filter((phone) => leadsForPhone(db, phone).some((lead) => isConvertedLead(db, lead)));
    const revisionPhones = phones.filter((phone) => leadsForPhone(db, phone).some((lead) => isRevisionStatus(db, lead.status_id)));
    const lostPhones = phones.filter((phone) => {
      const phoneLeads = leadsForPhone(db, phone);
      return phoneLeads.length > 0 && phoneLeads.every((lead) => isLostLead(db, lead));
    });
    const workingPhones = phones.filter((phone) => {
      const phoneLeads = leadsForPhone(db, phone);
      return phoneLeads.some((lead) => !isConvertedLead(db, lead) && !isLostLead(db, lead) && !isRevisionStatus(db, lead.status_id));
    });
    const callStats = aggregateCalls(callsForFirstUpload(db, upload.upload_id));

    return {
      ...upload,
      upload_lead_count: upload.row_count,
      bitrix_lead_count: leadIds.length,
      working_phone_count: workingPhones.length,
      revision_lead_count: revisionPhones.length,
      lost_phone_count: lostPhones.length,
      converted_phone_count: convertedPhones.length,
      converted_lead_count: convertedLeads.length,
      cr_by_phone: percent(convertedPhones.length, phones.length),
      cr_by_lead: percent(convertedLeads.length, upload.row_count),
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
      first_upload_id: firstUploadIdForPhoneContent(db, item.phone, item.utm_content),
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

function roundSortValue(value) {
  return String(Number(value || 0)).padStart(6, '0');
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
    { header: 'Номер круга', value: (row) => row.round_number, format: 'integer' },
    { header: 'Объем загрузки', value: (row) => uploadVolume(row), format: 'integer' },
    { header: 'Уникальных телефонов в загрузке', value: (row) => row.unique_phone_count, format: 'integer' },
    { header: 'Дубли в файле', value: (row) => row.duplicate_in_file_count, format: 'integer' },
    { header: 'Лидов в Битриксе', value: (row) => row.bitrix_lead_count, format: 'integer' },
    { header: 'В процессе обработки', value: (row) => row.working_phone_count, format: 'integer' },
    { header: 'В доработке', value: (row) => row.revision_lead_count, format: 'integer' },
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
    const segmentLabel = baseLabel(row.utm_content || row.utm_source || row.utm_medium);
    const source = sourceLabel(row.utm_medium, row.utm_source);
    const key = stableJson({
      period: russianMonth(row.upload_date),
      source,
      segment: segmentLabel,
    });

    if (!groups.has(key)) {
      groups.set(key, {
        period: russianMonth(row.upload_date),
        source,
        segment: segmentLabel,
        uploadVolume: 0,
        converted: 0,
      });
    }

    const group = groups.get(key);
    group.uploadVolume += uploadVolume(row);
    group.converted += Number(row.converted_lead_count || 0);
  }

  return [...groups.values()]
    .sort((a, b) => `${a.period}_${a.source}_${a.segment}`.localeCompare(`${b.period}_${b.source}_${b.segment}`))
    .map((row) => ({
      ...row,
      cr: ratioValue(row.converted, row.uploadVolume),
    }));
}

function monthKey(dateIso) {
  return String(dateIso || '').slice(0, 7);
}

function monthTitle(dateIso) {
  const year = String(dateIso || '').slice(0, 4);
  const month = russianMonth(dateIso);
  return [month, year].filter(Boolean).join(' ').trim();
}

function summarizeBaseRows(rows) {
  const uploadVolumeTotal = rows.reduce((sum, row) => sum + Number(uploadVolume(row) || 0), 0);
  const workingTotal = rows.reduce((sum, row) => sum + Number(row.working_phone_count || 0), 0);
  const revisionTotal = rows.reduce((sum, row) => sum + Number(row.revision_lead_count || 0), 0);
  const lostTotal = rows.reduce((sum, row) => sum + Number(row.lost_phone_count || 0), 0);
  const convertedTotal = rows.reduce((sum, row) => sum + Number(row.converted_lead_count || 0), 0);

  return {
    uploadVolume: uploadVolumeTotal,
    working: workingTotal,
    revision: revisionTotal,
    lost: lostTotal,
    converted: convertedTotal,
    cr: ratioValue(convertedTotal, uploadVolumeTotal),
  };
}

function buildIndicatorDetailRows(baseRows) {
  const rows = [];
  const rowGroups = [];
  const rowStyles = [];
  const merges = [];
  const sortedBaseRows = [...baseRows].sort((a, b) => `${a.upload_date}_${a.upload_id}_${roundSortValue(a.round_number)}_${a.utm_content}`.localeCompare(`${b.upload_date}_${b.upload_id}_${roundSortValue(b.round_number)}_${b.utm_content}`));
  const months = new Map();

  for (const row of sortedBaseRows) {
    const key = monthKey(row.upload_date);
    if (!months.has(key)) months.set(key, []);
    months.get(key).push(row);
  }

  let detailCounter = 0;
  let sheetRowIndex = 2;

  for (const [key, monthRows] of [...months.entries()].sort(([a], [b]) => a.localeCompare(b))) {
    const monthTotals = summarizeBaseRows(monthRows);
    const monthRow = Array.from({ length: 15 }, () => '');
    monthRow[0] = `Итого за ${monthTitle(`${key}-01`)}`;
    monthRow[9] = monthTotals.uploadVolume;
    monthRow[10] = monthTotals.working;
    monthRow[11] = monthTotals.revision;
    monthRow[12] = monthTotals.lost;
    monthRow[13] = monthTotals.converted;
    monthRow[14] = monthTotals.cr;
    rows.push(monthRow);
    rowStyles.push({ startRowIndex: sheetRowIndex, endRowIndex: sheetRowIndex + 1, style: 'month' });
    merges.push({ startRow: sheetRowIndex, endRow: sheetRowIndex + 1, startColumn: 0, endColumn: 9 });
    sheetRowIndex += 1;

    const monthGroupStart = sheetRowIndex;
    const dates = new Map();
    for (const row of monthRows) {
      if (!dates.has(row.upload_date)) dates.set(row.upload_date, []);
      dates.get(row.upload_date).push(row);
    }

    for (const [date, dayRows] of [...dates.entries()].sort(([a], [b]) => a.localeCompare(b))) {
      const dayTotals = summarizeBaseRows(dayRows);
      const dayRow = Array.from({ length: 15 }, () => '');
      dayRow[0] = `Итого за день ${date}`;
      dayRow[9] = dayTotals.uploadVolume;
      dayRow[10] = dayTotals.working;
      dayRow[11] = dayTotals.revision;
      dayRow[12] = dayTotals.lost;
      dayRow[13] = dayTotals.converted;
      dayRow[14] = dayTotals.cr;
      rows.push(dayRow);
      rowStyles.push({ startRowIndex: sheetRowIndex, endRowIndex: sheetRowIndex + 1, style: 'day' });
      merges.push({ startRow: sheetRowIndex, endRow: sheetRowIndex + 1, startColumn: 0, endColumn: 9 });
      sheetRowIndex += 1;

      const dayGroupStart = sheetRowIndex;
      for (const base of dayRows) {
        detailCounter += 1;
        const row = Array.from({ length: 15 }, () => '');
        row[0] = detailCounter;
        row[1] = base.utm_medium || '—';
        row[2] = base.utm_source || '—';
        row[3] = base.utm_campaign || '—';
        row[4] = base.utm_content || '—';
        row[5] = base.utm_term || '—';
        row[6] = baseLabel(base.utm_content || base.utm_source || base.utm_medium);
        row[7] = base.upload_date;
        row[8] = base.round_number;
        row[9] = uploadVolume(base);
        row[10] = base.working_phone_count;
        row[11] = base.revision_lead_count;
        row[12] = base.lost_phone_count;
        row[13] = base.converted_lead_count;
        row[14] = percentValue(base.cr_by_lead);
        rows.push(row);
        sheetRowIndex += 1;
      }

      if (sheetRowIndex > dayGroupStart) {
        rowGroups.push({ startIndex: dayGroupStart, endIndex: sheetRowIndex });
      }
    }

    if (sheetRowIndex > monthGroupStart) {
      rowGroups.push({ startIndex: monthGroupStart, endIndex: sheetRowIndex });
    }
  }

  return { rows, rowGroups, rowStyles, merges };
}

function buildIndicatorsValues(baseRows) {
  const sortedBaseRows = [...baseRows].sort((a, b) => `${a.upload_date}_${a.upload_id}_${roundSortValue(a.round_number)}`.localeCompare(`${b.upload_date}_${b.upload_id}_${roundSortValue(b.round_number)}`));
  const detailLayout = buildIndicatorDetailRows(sortedBaseRows);
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
      'Номер круга',
      'Объем загрузки',
      'В процессе обработки',
      'В доработке',
      'Проиграно',
      'Сконвертировано',
      'CR',
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
    ],
  ];

  for (let index = 0; index < detailLayout.rows.length; index += 1) {
    const base = detailLayout.rows[index];
    const row = Array.from({ length: 15 }, () => '');

    if (base) {
      for (let columnIndex = 0; columnIndex <= 14; columnIndex += 1) {
        row[columnIndex] = base[columnIndex] ?? '';
      }
    }

    values.push(row);
  }

  return {
    values,
    rowGroups: detailLayout.rowGroups,
    rowStyles: detailLayout.rowStyles,
    merges: detailLayout.merges,
  };
}

function buildSourceSummaryValues(baseRows) {
  const rows = buildSourceSummaryRows(baseRows);
  return [
    ['Итог по источникам'],
    ['Период', 'Источник', 'Сегмент', 'Суммарный объем загрузки', 'Сконвертировано лидов', 'CR'],
    ...rows.map((row) => [
      row.period,
      row.source,
      row.segment,
      row.uploadVolume,
      row.converted,
      row.cr,
    ]),
  ];
}

function buildReadableCallabilityRows(byBaseRows) {
  return byBaseRows
    .filter((row) => row.first_upload_id !== 'unmatched')
    .map((row) => ({
      date: row.upload_date,
      base: baseLabel(row.utm_content || row.first_upload_id),
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

function buildDashboardDailyRows(baseRows) {
  const groups = new Map();

  for (const row of baseRows) {
    const key = row.upload_date;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  return [...groups.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, rows]) => {
      const totals = summarizeBaseRows(rows);
      return {
        date,
        month: monthKey(date),
        uploadVolume: totals.uploadVolume,
        working: totals.working,
        revision: totals.revision,
        lost: totals.lost,
        converted: totals.converted,
        cr: totals.cr,
      };
    });
}

function buildDashboardPayload(db, baseRows) {
  const totals = summarizeBaseRows(baseRows);
  const sourceSummaryRows = buildSourceSummaryRows(baseRows);
  const dailyRows = buildDashboardDailyRows(baseRows);
  const ranges = {
    from: reportContext(db).from || '',
    to: reportContext(db).to || '',
  };
  const normalizedBaseRows = [...baseRows]
    .sort((a, b) => `${b.upload_date}_${roundSortValue(b.round_number)}_${b.utm_content}`.localeCompare(`${a.upload_date}_${roundSortValue(a.round_number)}_${a.utm_content}`))
    .map((row) => ({
      uploadDate: row.upload_date,
      month: monthKey(row.upload_date),
      monthLabel: monthTitle(row.upload_date),
      utmMedium: row.utm_medium || '',
      utmSource: row.utm_source || '',
      sourceLabel: sourceLabel(row.utm_medium, row.utm_source),
      utmCampaign: row.utm_campaign || '',
      utmContent: row.utm_content || '',
      utmTerm: row.utm_term || '',
      baseLabel: baseLabel(row.utm_content || row.utm_source || row.utm_medium),
      roundNumber: Number(row.round_number || 0),
      uploadVolume: Number(uploadVolume(row) || 0),
      working: Number(row.working_phone_count || 0),
      revision: Number(row.revision_lead_count || 0),
      lost: Number(row.lost_phone_count || 0),
      converted: Number(row.converted_lead_count || 0),
      cr: percentValue(row.cr_by_lead) || 0,
    }));

  return {
    generatedAt: new Date().toISOString(),
    report: {
      source: reportContext(db).source || 'bitrix',
      from: ranges.from,
      to: ranges.to,
    },
    totals: {
      uploadVolume: totals.uploadVolume,
      working: totals.working,
      revision: totals.revision,
      lost: totals.lost,
      converted: totals.converted,
      cr: totals.cr || 0,
      rows: normalizedBaseRows.length,
      sources: [...new Set(normalizedBaseRows.map((row) => row.sourceLabel).filter(Boolean))].length,
      segments: [...new Set(normalizedBaseRows.map((row) => row.baseLabel).filter(Boolean))].length,
    },
    filters: {
      months: [...new Set(normalizedBaseRows.map((row) => row.month))].sort(),
      sources: [...new Set(normalizedBaseRows.map((row) => row.sourceLabel).filter(Boolean))].sort(),
      segments: [...new Set(normalizedBaseRows.map((row) => row.baseLabel).filter(Boolean))].sort(),
      rounds: [...new Set(normalizedBaseRows.map((row) => row.roundNumber).filter((value) => Number.isFinite(value) && value > 0))].sort((a, b) => a - b),
      minDate: dailyRows[0]?.date || ranges.from,
      maxDate: dailyRows.at(-1)?.date || ranges.to,
    },
    dailyRows,
    sourceSummaryRows: sourceSummaryRows.map((row) => ({
      period: row.period,
      source: row.source,
      segment: row.segment,
      uploadVolume: Number(row.uploadVolume || 0),
      converted: Number(row.converted || 0),
      cr: row.cr || 0,
    })),
    baseRows: normalizedBaseRows,
  };
}

async function writeDashboardFiles(db, baseRows, dashboardDir) {
  const payload = buildDashboardPayload(db, baseRows);
  const dataDir = path.join(dashboardDir, 'data');
  await mkdir(dataDir, { recursive: true });
  await writeFile(path.join(dataDir, 'report-data.json'), `${JSON.stringify(payload, null, 2)}\n`, 'utf8');
  await writeFile(path.join(dataDir, 'report-data.js'), `window.REPORT_DASHBOARD_DATA = ${JSON.stringify(payload)};\n`, 'utf8');
}

function buildGoogleWorksheets(db) {
  const baseRows = buildBaseReportRows(db);
  const dailyRows = buildCallabilityDailyRows(db);
  const byBaseRows = buildCallabilityByBaseRows(db);
  const detailRows = buildUploadItemsRows(db);
  const indicatorsSheet = buildIndicatorsValues(baseRows);
  const sourceSummaryValues = buildSourceSummaryValues(baseRows);
  const baseColumns = baseSheetColumns();
  const dailyColumns = callabilitySheetColumns('Дата');
  const byBaseColumns = callabilitySheetColumns('first_upload_id');
  const detailColumns = detailSheetColumns();

  return [
    {
      title: 'Показатели',
      values: indicatorsSheet.values,
      frozenRows: 2,
      headerRows: [0, 1],
      rowGroups: indicatorsSheet.rowGroups,
      rowStyles: indicatorsSheet.rowStyles,
      columnWidths: [
        { startIndex: 0, endIndex: 1, pixelSize: 50 },
        { startIndex: 1, endIndex: 6, pixelSize: 130 },
        { startIndex: 6, endIndex: 7, pixelSize: 140 },
        { startIndex: 7, endIndex: 8, pixelSize: 115 },
        { startIndex: 8, endIndex: 15, pixelSize: 120 },
      ],
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
        { startRow: 0, endRow: 2, startColumn: 14, endColumn: 15 },
        ...(indicatorsSheet.merges ?? []),
      ],
      columnFormats: [
        { index: 7, format: 'date', startRowIndex: 2 },
        { index: 8, format: 'integer', startRowIndex: 2 },
        { index: 9, format: 'integer', startRowIndex: 2 },
        { index: 10, format: 'integer', startRowIndex: 2 },
        { index: 11, format: 'integer', startRowIndex: 2 },
        { index: 12, format: 'integer', startRowIndex: 2 },
        { index: 13, format: 'integer', startRowIndex: 2 },
        { index: 14, format: 'percent', startRowIndex: 2 },
      ],
    },
    {
      title: 'Итог по источникам',
      values: sourceSummaryValues,
      frozenRows: 2,
      headerRows: [0, 1],
      columnWidths: [
        { startIndex: 0, endIndex: 3, pixelSize: 160 },
        { startIndex: 3, endIndex: 5, pixelSize: 140 },
        { startIndex: 5, endIndex: 6, pixelSize: 110 },
      ],
      merges: [
        { startRow: 0, endRow: 1, startColumn: 0, endColumn: 6 },
      ],
      columnFormats: [
        { index: 3, format: 'integer', startRowIndex: 2 },
        { index: 4, format: 'integer', startRowIndex: 2 },
        { index: 5, format: 'percent', startRowIndex: 2 },
      ],
    },
    {
      title: 'Дозваниваемость',
      values: buildReadableCallabilityValues(db),
      frozenRows: 2,
      headerRows: [0, 1],
      columnWidths: [
        { startIndex: 0, endIndex: 1, pixelSize: 115 },
        { startIndex: 1, endIndex: 7, pixelSize: 145 },
      ],
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
        ['Номер круга', 'Количество входов лида в стадию "Необработанное": первое появление в этой стадии считается первым кругом, каждый возврат из другой стадии в "Необработанное" увеличивает круг на 1.'],
        ['В процессе обработки', 'Лиды загрузки не в финальном успешном, не в финальном проигранном статусе и не в стадиях доработки.'],
        ['В доработке', 'Лиды в стадиях "Перезвонить 30 дн", "Долгосрок от 6 мес.", "Добрифовать", "Прошел бриф", "Предконвертация".'],
        ['Проиграно', 'Лиды загрузки в проигранных статусах.'],
        ['Сконвертировано', 'Сконвертированные лиды загрузки.'],
        ['CR', 'Сконвертировано / Объем загрузки.'],
        ['Дозваниваемость', 'Уникальные телефоны с разговором 10 секунд и больше / уникальные телефоны, по которым были звонки.'],
        ['Пустые метки', 'Лиды без первичных UTM-меток не попадают в отчет по базам.'],
      ],
      frozenRows: 1,
      headerRows: [0],
      columnWidths: [
        { startIndex: 0, endIndex: 1, pixelSize: 220 },
        { startIndex: 1, endIndex: 2, pixelSize: 620 },
      ],
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

function columnWidthRequest(sheetId, startIndex, endIndex, pixelSize) {
  return {
    updateDimensionProperties: {
      range: {
        sheetId,
        dimension: 'COLUMNS',
        startIndex,
        endIndex,
      },
      properties: { pixelSize },
      fields: 'pixelSize',
    },
  };
}

function rowStyleFormat(style) {
  if (style === 'month') {
    return {
      backgroundColor: color('#D1D5DB'),
      textFormat: { bold: true, foregroundColor: color('#111827'), fontSize: 10 },
      horizontalAlignment: 'CENTER',
      wrapStrategy: 'WRAP',
      verticalAlignment: 'MIDDLE',
    };
  }

  if (style === 'day') {
    return {
      backgroundColor: color('#F3F4F6'),
      textFormat: { bold: true, foregroundColor: color('#111827'), fontSize: 10 },
      horizontalAlignment: 'CENTER',
      wrapStrategy: 'WRAP',
      verticalAlignment: 'MIDDLE',
    };
  }

  return null;
}

function deleteRowGroupRequests(sheet) {
  return [...(sheet.rowGroups ?? [])]
    .sort((a, b) => (Number(b.depth || 0) - Number(a.depth || 0)) || (Number((b.range?.endIndex ?? 0) - (b.range?.startIndex ?? 0)) - Number((a.range?.endIndex ?? 0) - (a.range?.startIndex ?? 0))))
    .map((group) => ({
      deleteDimensionGroup: {
        range: {
          sheetId: sheet.properties.sheetId,
          dimension: 'ROWS',
          startIndex: group.range.startIndex,
          endIndex: group.range.endIndex,
        },
      },
    }));
}

function addRowGroupRequests(sheetId, worksheet) {
  return [...(worksheet.rowGroups ?? [])]
    .sort((a, b) => (Number(b.endIndex - b.startIndex) - Number(a.endIndex - a.startIndex)) || (Number(a.startIndex) - Number(b.startIndex)))
    .map((group) => ({
      addDimensionGroup: {
        range: {
          sheetId,
          dimension: 'ROWS',
          startIndex: group.startIndex,
          endIndex: group.endIndex,
        },
      },
    }));
}

function formatRequestsForWorksheet(sheet, worksheet, desiredIndex) {
  const sheetId = sheet.properties.sheetId;
  const rowCount = Math.max(1, worksheet.values.length);
  const columnCount = Math.max(1, worksheet.values[0]?.length ?? 1);
  const requests = [
    ...deleteRowGroupRequests(sheet),
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
            horizontalAlignment: 'CENTER',
            wrapStrategy: 'WRAP',
            verticalAlignment: 'MIDDLE',
          },
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,wrapStrategy,verticalAlignment)',
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

  for (const item of worksheet.columnWidths ?? []) {
    requests.push(columnWidthRequest(sheetId, item.startIndex, item.endIndex, item.pixelSize));
  }

  requests.push(...addRowGroupRequests(sheetId, worksheet));

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
          endRowIndex: Math.max(100, rowCount + 10),
          startColumnIndex: 0,
          endColumnIndex: Math.max(26, columnCount),
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

  for (const item of worksheet.rowStyles ?? []) {
    const format = rowStyleFormat(item.style);
    if (!format) continue;

    requests.push({
      repeatCell: {
        range: {
          sheetId,
          startRowIndex: item.startRowIndex,
          endRowIndex: item.endRowIndex,
          startColumnIndex: 0,
          endColumnIndex: columnCount,
        },
        cell: {
          userEnteredFormat: format,
        },
        fields: 'userEnteredFormat(backgroundColor,textFormat,horizontalAlignment,wrapStrategy,verticalAlignment)',
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
  await ensureWorksheetTabs(sheets, spreadsheet.spreadsheetId, spreadsheet.metadata, worksheets);

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

  const formattingMetadataResponse = await sheets.spreadsheets.get({
    spreadsheetId: spreadsheet.spreadsheetId,
    fields: 'sheets(properties(title,sheetId),rowGroups)',
  });
  const formattingMetadata = formattingMetadataResponse.data;
  const sheetsByTitle = new Map((formattingMetadata.sheets ?? []).map((sheet) => [sheet.properties.title, sheet]));

  const requests = [
    ...hideObsoleteWorksheetRequests(formattingMetadata, worksheets),
    ...worksheets.flatMap((worksheet, index) => {
    const sheet = sheetsByTitle.get(worksheet.title);
    if (!sheet) return [];
    return formatRequestsForWorksheet(sheet, worksheet, index);
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

async function generateReports(db, reportsDir, dashboardDir = DEFAULT_DASHBOARD_DIR) {
  const baseRows = buildBaseReportRows(db);
  await writeCsv(path.join(reportsDir, 'base_report.csv'), baseRows, [
    { header: 'Дата загрузки', value: (row) => row.upload_date },
    { header: 'upload_id', value: (row) => row.upload_id },
    { header: 'utm_medium', value: (row) => row.utm_medium },
    { header: 'utm_source', value: (row) => row.utm_source },
    { header: 'utm_campaign', value: (row) => row.utm_campaign },
    { header: 'utm_content', value: (row) => row.utm_content },
    { header: 'utm_term', value: (row) => row.utm_term },
    { header: 'Номер круга', value: (row) => row.round_number },
    { header: 'Объем загрузки', value: (row) => uploadVolume(row) },
    { header: 'Уникальных телефонов в загрузке', value: (row) => row.unique_phone_count },
    { header: 'Дубли внутри файла', value: (row) => row.duplicate_in_file_count },
    { header: 'Лидов в Битриксе', value: (row) => row.bitrix_lead_count },
    { header: 'В процессе обработки, лидов', value: (row) => row.working_phone_count },
    { header: 'В доработке, лидов', value: (row) => row.revision_lead_count },
    { header: 'Проиграно, лидов', value: (row) => row.lost_phone_count },
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

  await writeDashboardFiles(db, baseRows, dashboardDir);

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
  const dashboardDir = String(args.dashboard || DEFAULT_DASHBOARD_DIR);
  const db = await loadJson(dbPath, createEmptyDb());
  db.bitrix_stage_history ??= {};
  const today = todayIsoDate();
  const syncFrom = resolveDateArg(args['month-current']
    ? monthStartIso(today)
    : args.from || args['calls-from'] || args['calls-date'] || db.report_context?.sync_from || db.report_context?.from || today, today);
  const syncTo = resolveDateArg(args['month-current']
    ? today
    : args.to || args['calls-to'] || args['calls-date'] || syncFrom, syncFrom);
  const hasExplicitReportRange = Boolean(args.from || args.to || args['report-from'] || args['report-to']);
  const explicitReportFrom = hasExplicitReportRange
    ? resolveDateArg(args['report-from'] || args.from || today, today)
    : null;
  const explicitReportTo = hasExplicitReportRange
    ? resolveDateArg(args['report-to'] || args.to || explicitReportFrom || today, explicitReportFrom || today)
    : null;
  const explicitSource = args.source === 'bitrix' || args['bitrix-range']
    ? 'bitrix'
    : (args.upload ? 'uploads' : null);
  const reportSource = explicitSource || db.report_context?.source || 'uploads';
  let activeUpload = null;

  if (args.upload) {
    const result = await importUploadCsv(db, String(args.upload), {
      forceNewUpload: Boolean(args['force-new-upload']),
    });
    activeUpload = result.upload;
    console.log(result.message);
  }

  if (reportSource === 'bitrix' && !args['skip-bitrix']) {
    const result = await fetchBitrixLeadsCreatedRange(db, syncFrom, syncTo);
    console.log(`Bitrix range synced: ${result.leadCount} lead(s), ${result.phoneCount} phone(s), ${result.stageHistoryCount} stage history item(s), period: ${syncFrom}..${syncTo}.`);
  }

  if (!args['skip-bitrix'] && reportSource !== 'bitrix') {
    const phones = activeUpload ? uploadPhones(db, activeUpload.upload_id) : Object.keys(db.phones);
    const result = await syncBitrixForPhones(db, phones);
    console.log(`Bitrix synced: ${result.leadCount} lead(s).`);
  }

  if (!args['skip-skorozvon']) {
    const dates = args['calls-today']
      ? [today]
      : args['calls-date']
        ? [resolveDateArg(args['calls-date'], today)]
        : args['calls-from'] || args['month-current'] || args.from || args.to
      ? dateRange(resolveDateArg(args['calls-from'] || syncFrom, syncFrom), resolveDateArg(args['calls-to'] || syncTo || args['calls-from'] || syncFrom, syncTo || syncFrom))
      : [today];
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

  const historyRange = historyReportRange(db, syncFrom, syncTo);
  db.report_context = {
    source: reportSource,
    from: explicitReportFrom || historyRange.from,
    to: explicitReportTo || historyRange.to,
    sync_from: syncFrom,
    sync_to: syncTo,
  };

  await saveJson(dbPath, db);
  const reportStats = await generateReports(db, reportsDir, dashboardDir);
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
