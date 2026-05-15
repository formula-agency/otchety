const data = window.REPORT_DASHBOARD_DATA;

const state = {
  source: 'all',
  segment: 'all',
  round: 'all',
  dateFrom: '',
  dateTo: '',
  detailDate: 'latest',
  search: '',
};

const els = {
  siteHeader: document.querySelector('.site-header'),
  navLinks: [...document.querySelectorAll('.site-nav a')],
  reportPeriod: document.getElementById('report-period'),
  updatedAt: document.getElementById('updated-at'),
  heroSources: document.getElementById('hero-sources'),
  heroSegments: document.getElementById('hero-segments'),
  heroVolume: document.getElementById('hero-volume'),
  heroCr: document.getElementById('hero-cr'),
  heroWorking: document.getElementById('hero-working'),
  heroRevision: document.getElementById('hero-revision'),
  heroLost: document.getElementById('hero-lost'),
  source: document.getElementById('filter-source'),
  segment: document.getElementById('filter-segment'),
  round: document.getElementById('filter-round'),
  dateFrom: document.getElementById('filter-date-from'),
  dateTo: document.getElementById('filter-date-to'),
  search: document.getElementById('filter-search'),
  reset: document.getElementById('reset-filters'),
  activeFilters: document.getElementById('active-filters'),
  selectionSummary: document.getElementById('selection-summary'),
  sourceSummaryBody: document.getElementById('source-summary-body'),
  detailCaption: document.getElementById('detail-caption'),
  detailDate: document.getElementById('detail-date-select'),
  detailBody: document.getElementById('detail-body'),
  exportCsv: document.getElementById('export-csv'),
  kpiVolume: document.getElementById('kpi-volume'),
  kpiWorking: document.getElementById('kpi-working'),
  kpiRevision: document.getElementById('kpi-revision'),
  kpiLost: document.getElementById('kpi-lost'),
  kpiConverted: document.getElementById('kpi-converted'),
  kpiCr: document.getElementById('kpi-cr'),
};

const chartPalette = {
  cyan: '#2f8cff',
  emerald: '#5aa68f',
  amber: '#efbd55',
  coral: '#d66b62',
  lime: '#66a05e',
  violet: '#7b7af0',
  ink: '#161414',
};

const numberFormatter = new Intl.NumberFormat('ru-RU');
const percentFormatter = new Intl.NumberFormat('ru-RU', { style: 'percent', minimumFractionDigits: 2, maximumFractionDigits: 2 });
const dateFormatter = new Intl.DateTimeFormat('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' });
const monthFormatter = new Intl.DateTimeFormat('ru-RU', { month: 'long', year: 'numeric' });

let dailyChart;
let sourceChart;
let roundChart;
let segmentChart;

function formatNumber(value) {
  return numberFormatter.format(Number(value || 0));
}

function formatPercent(value) {
  return percentFormatter.format(Number(value || 0));
}

function formatDate(value) {
  if (!value) return '—';
  return dateFormatter.format(new Date(`${value}T00:00:00`));
}

function formatMonth(value) {
  if (!value) return '—';
  const label = monthFormatter.format(new Date(`${value}-01T00:00:00`));
  return label.slice(0, 1).toUpperCase() + label.slice(1);
}

function clampDate(value, minValue, maxValue) {
  if (!value) return '';
  if (minValue && value < minValue) return minValue;
  if (maxValue && value > maxValue) return maxValue;
  return value;
}

function resolveDefaultDateRange(filters = {}) {
  const minDate = filters.minDate || '';
  const maxDate = filters.maxDate || '';
  if (!maxDate) {
    return { dateFrom: minDate, dateTo: maxDate };
  }

  const anchor = new Date(`${maxDate}T00:00:00`);
  const monthStart = new Date(anchor.getFullYear(), anchor.getMonth(), 1).toISOString().slice(0, 10);
  return {
    dateFrom: clampDate(monthStart, minDate, maxDate),
    dateTo: maxDate,
  };
}

function csvEscape(value) {
  const text = String(value ?? '');
  if (/[;"\r\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
  return text;
}

function buildDateRangeLabel(minDate, maxDate) {
  if (!minDate && !maxDate) return 'Весь период';
  if (!minDate) return `До ${formatDate(maxDate)}`;
  if (!maxDate) return `С ${formatDate(minDate)}`;
  return `${formatDate(minDate)} - ${formatDate(maxDate)}`;
}

function setText(element, value) {
  if (element) element.textContent = value;
}

function populateSelect(select, options, allLabel) {
  select.innerHTML = '';
  const allOption = document.createElement('option');
  allOption.value = 'all';
  allOption.textContent = allLabel;
  select.append(allOption);

  for (const option of options) {
    const element = document.createElement('option');
    element.value = String(option);
    element.textContent = String(option);
    select.append(element);
  }
}

function normalizeSearch(value) {
  return String(value || '').trim().toLowerCase();
}

function uniqueDates(rows) {
  return [...new Set(rows.map((row) => row.uploadDate).filter(Boolean))].sort((a, b) => b.localeCompare(a));
}

function filteredRows() {
  const query = normalizeSearch(state.search);
  return data.baseRows.filter((row) => {
    if (state.source !== 'all' && row.sourceLabel !== state.source) return false;
    if (state.segment !== 'all' && row.baseLabel !== state.segment) return false;
    if (state.round !== 'all' && String(row.roundNumber) !== state.round) return false;
    if (state.dateFrom && row.uploadDate < state.dateFrom) return false;
    if (state.dateTo && row.uploadDate > state.dateTo) return false;
    if (!query) return true;

    return [
      row.utmCampaign,
      row.utmContent,
      row.baseLabel,
      row.sourceLabel,
      row.utmSource,
      row.utmMedium,
      row.utmTerm,
    ].some((field) => normalizeSearch(field).includes(query));
  });
}

function summarizeRows(rows) {
  return rows.reduce((acc, row) => {
    acc.uploadVolume += Number(row.uploadVolume || 0);
    acc.working += Number(row.working || 0);
    acc.revision += Number(row.revision || 0);
    acc.lost += Number(row.lost || 0);
    acc.converted += Number(row.converted || 0);
    return acc;
  }, { uploadVolume: 0, working: 0, revision: 0, lost: 0, converted: 0 });
}

function summarizeByDate(rows) {
  const groups = new Map();

  for (const row of rows) {
    const key = row.uploadDate;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  return [...groups.entries()]
    .sort(([a], [b]) => a.localeCompare(b))
    .map(([date, items]) => {
      const summary = summarizeRows(items);
      return {
        date,
        ...summary,
        cr: summary.uploadVolume > 0 ? summary.converted / summary.uploadVolume : 0,
      };
    });
}

function summarizeBySource(rows) {
  const groups = new Map();

  for (const row of rows) {
    const key = row.sourceLabel || 'Без источника';
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  return [...groups.entries()]
    .map(([source, items]) => {
      const summary = summarizeRows(items);
      return {
        source,
        ...summary,
      };
    })
    .sort((a, b) => b.uploadVolume - a.uploadVolume);
}

function summarizeBySegment(rows) {
  const groups = new Map();

  for (const row of rows) {
    const key = row.baseLabel || 'Без базы';
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  return [...groups.entries()]
    .map(([segment, items]) => {
      const summary = summarizeRows(items);
      return {
        segment,
        ...summary,
      };
    })
    .sort((a, b) => b.uploadVolume - a.uploadVolume);
}

function summarizeByRound(rows) {
  const groups = new Map();

  for (const row of rows) {
    const key = Number(row.roundNumber || 0);
    if (!Number.isFinite(key) || key <= 0) continue;
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  return [...groups.entries()]
    .sort(([a], [b]) => a - b)
    .map(([round, items]) => {
      const summary = summarizeRows(items);
      return {
        round,
        ...summary,
        cr: summary.uploadVolume > 0 ? summary.converted / summary.uploadVolume : 0,
      };
    });
}

function summarizeSourceTable(rows) {
  const groups = new Map();

  for (const row of rows) {
    const period = formatMonth(row.month);
    const source = row.sourceLabel || 'Без источника';
    const segment = row.baseLabel || 'Без базы';
    const key = `${period}__${source}__${segment}`;
    if (!groups.has(key)) {
      groups.set(key, {
        period,
        source,
        segment,
        uploadVolume: 0,
        converted: 0,
      });
    }

    const group = groups.get(key);
    group.uploadVolume += Number(row.uploadVolume || 0);
    group.converted += Number(row.converted || 0);
  }

  return [...groups.values()]
    .map((row) => ({
      ...row,
      cr: row.uploadVolume > 0 ? row.converted / row.uploadVolume : 0,
    }))
    .sort((a, b) =>
      (b.uploadVolume - a.uploadVolume)
      || (b.converted - a.converted)
      || a.period.localeCompare(b.period)
      || a.source.localeCompare(b.source)
      || a.segment.localeCompare(b.segment));
}

function renderKpis(rows) {
  const summary = summarizeRows(rows);
  els.kpiVolume.textContent = formatNumber(summary.uploadVolume);
  els.kpiWorking.textContent = formatNumber(summary.working);
  els.kpiRevision.textContent = formatNumber(summary.revision);
  els.kpiLost.textContent = formatNumber(summary.lost);
  els.kpiConverted.textContent = formatNumber(summary.converted);
  els.kpiCr.textContent = formatPercent(summary.uploadVolume > 0 ? summary.converted / summary.uploadVolume : 0);
}

function renderHero(rows) {
  const summary = summarizeRows(rows);
  const uniqueSources = new Set(rows.map((row) => row.sourceLabel).filter(Boolean)).size;
  const uniqueSegments = new Set(rows.map((row) => row.baseLabel).filter(Boolean)).size;
  const sortedDates = rows.map((row) => row.uploadDate).filter(Boolean).sort((a, b) => a.localeCompare(b));
  const rangeLabel = buildDateRangeLabel(sortedDates[0] || state.dateFrom || data.report.from, sortedDates.at(-1) || state.dateTo || data.report.to);

  setText(els.reportPeriod, rangeLabel);
  setText(els.heroSources, formatNumber(uniqueSources));
  setText(els.heroSegments, formatNumber(uniqueSegments));
  setText(els.heroVolume, formatNumber(summary.uploadVolume));
  setText(els.heroCr, formatPercent(summary.uploadVolume > 0 ? summary.converted / summary.uploadVolume : 0));
  setText(els.heroWorking, formatNumber(summary.working));
  setText(els.heroRevision, formatNumber(summary.revision));
  setText(els.heroLost, formatNumber(summary.lost));
}

function renderActiveState(rows) {
  const chips = [];
  if (state.source !== 'all') chips.push(`Источник: ${state.source}`);
  if (state.segment !== 'all') chips.push(`База: ${state.segment}`);
  if (state.round !== 'all') chips.push(`Круг: ${state.round}`);
  if (state.dateFrom) chips.push(`От: ${formatDate(state.dateFrom)}`);
  if (state.dateTo) chips.push(`До: ${formatDate(state.dateTo)}`);
  if (normalizeSearch(state.search)) chips.push(`Поиск: ${state.search.trim()}`);

  els.activeFilters.innerHTML = chips.length > 0
    ? chips.map((chip) => `<span class="chip">${chip}</span>`).join('')
    : '<span class="chip">Все данные</span>';

  const uniqueSources = new Set(rows.map((row) => row.sourceLabel).filter(Boolean)).size;
  const uniqueSegments = new Set(rows.map((row) => row.baseLabel).filter(Boolean)).size;
  els.selectionSummary.textContent = `Строк: ${formatNumber(rows.length)} · Источников: ${formatNumber(uniqueSources)} · Баз: ${formatNumber(uniqueSegments)}`;
}

function populateDetailDateSelect(rows) {
  if (!els.detailDate) return;

  const dates = uniqueDates(rows);
  const previous = state.detailDate;
  els.detailDate.innerHTML = '';

  const options = [
    { value: 'latest', label: 'Последняя дата' },
    { value: 'all', label: 'Все даты' },
    ...dates.map((date) => ({ value: date, label: formatDate(date) })),
  ];

  for (const option of options) {
    const element = document.createElement('option');
    element.value = option.value;
    element.textContent = option.label;
    els.detailDate.append(element);
  }

  if (previous === 'all' || previous === 'latest' || dates.includes(previous)) {
    state.detailDate = previous;
  } else {
    state.detailDate = dates[0] || 'latest';
  }

  els.detailDate.value = state.detailDate;
}

function detailRowsForView(rows) {
  const dates = uniqueDates(rows);
  const sortRows = (items) => [...items].sort((a, b) =>
    (Number(b.uploadVolume || 0) - Number(a.uploadVolume || 0))
    || (Number(b.converted || 0) - Number(a.converted || 0))
    || a.uploadDate.localeCompare(b.uploadDate)
    || (a.sourceLabel || '').localeCompare(b.sourceLabel || '')
    || (a.baseLabel || '').localeCompare(b.baseLabel || '')
    || (Number(a.roundNumber || 0) - Number(b.roundNumber || 0)));

  if (state.detailDate === 'all') {
    return {
      rows: sortRows(rows),
      mode: 'all',
      date: '',
    };
  }

  const selectedDate = state.detailDate === 'latest'
    ? (dates[0] || '')
    : (dates.includes(state.detailDate) ? state.detailDate : (dates[0] || ''));

  return {
    rows: sortRows(selectedDate ? rows.filter((row) => row.uploadDate === selectedDate) : rows),
    mode: 'single',
    date: selectedDate,
  };
}

function renderDetailCaption(detailView) {
  if (!els.detailCaption) return;

  if (detailView.rows.length === 0) {
    els.detailCaption.textContent = 'Нет строк по текущим фильтрам';
    return;
  }

  if (detailView.mode === 'all') {
    els.detailCaption.textContent = `Все даты · ${formatNumber(detailView.rows.length)} строк`;
    return;
  }

  els.detailCaption.textContent = `${formatDate(detailView.date)} · ${formatNumber(detailView.rows.length)} строк`;
}

function renderSourceSummaryTable(rows) {
  const summaryRows = summarizeSourceTable(rows);
  if (summaryRows.length === 0) {
    els.sourceSummaryBody.innerHTML = '<tr class="empty-row"><td colspan="6">Нет данных</td></tr>';
    return;
  }

  els.sourceSummaryBody.innerHTML = summaryRows
    .map((row) => `
      <tr>
        <td>${row.period}</td>
        <td>${row.source}</td>
        <td>${row.segment}</td>
        <td>${formatNumber(row.uploadVolume)}</td>
        <td>${formatPercent(row.cr)}</td>
        <td>${formatNumber(row.converted)}</td>
      </tr>
    `)
    .join('');
}

function renderDetailTable(rows) {
  if (rows.length === 0) {
    els.detailBody.innerHTML = '<tr class="empty-row"><td colspan="10">Нет данных</td></tr>';
    return;
  }

  els.detailBody.innerHTML = rows
    .map((row) => `
      <tr>
        <td>${formatDate(row.uploadDate)}</td>
        <td>${row.sourceLabel || '—'}</td>
        <td>${row.baseLabel || '—'}</td>
        <td>${formatNumber(row.roundNumber)}</td>
        <td>${formatNumber(row.uploadVolume)}</td>
        <td>${formatPercent(row.cr)}</td>
        <td>${formatNumber(row.working)}</td>
        <td>${formatNumber(row.revision)}</td>
        <td>${formatNumber(row.lost)}</td>
        <td>${formatNumber(row.converted)}</td>
      </tr>
    `)
    .join('');
}

function ensureCharts() {
  if (!dailyChart) {
    dailyChart = new Chart(document.getElementById('daily-chart'), {
      type: 'bar',
      data: { labels: [], datasets: [] },
      options: {
        maintainAspectRatio: false,
        responsive: true,
        interaction: { mode: 'index', intersect: false },
        scales: {
          x: { grid: { display: false } },
          y: { beginAtZero: true, ticks: { callback: (value) => formatNumber(value) } },
          y1: {
            beginAtZero: true,
            position: 'right',
            grid: { drawOnChartArea: false },
            ticks: { callback: (value) => `${Math.round(value * 100)}%` },
          },
        },
        plugins: {
          legend: { position: 'bottom' },
          tooltip: {
            callbacks: {
              label(context) {
                const value = context.parsed.y;
                return context.dataset.yAxisID === 'y1'
                  ? `${context.dataset.label}: ${formatPercent(value)}`
                  : `${context.dataset.label}: ${formatNumber(value)}`;
              },
            },
          },
        },
      },
    });
  }

  if (!sourceChart) {
    sourceChart = new Chart(document.getElementById('source-chart'), {
      type: 'doughnut',
      data: { labels: [], datasets: [] },
      options: {
        maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom' },
          tooltip: {
            callbacks: {
              label(context) {
                return `${context.label}: ${formatNumber(context.parsed)}`;
              },
            },
          },
        },
      },
    });
  }

  if (!roundChart) {
    roundChart = new Chart(document.getElementById('round-chart'), {
      type: 'bar',
      data: { labels: [], datasets: [] },
      options: {
        maintainAspectRatio: false,
        responsive: true,
        interaction: { mode: 'index', intersect: false },
        scales: {
          x: { grid: { display: false } },
          y: { beginAtZero: true, ticks: { callback: (value) => formatNumber(value) } },
          y1: {
            beginAtZero: true,
            position: 'right',
            grid: { drawOnChartArea: false },
            ticks: { callback: (value) => `${Math.round(value * 100)}%` },
          },
        },
        plugins: {
          legend: { position: 'bottom' },
          tooltip: {
            callbacks: {
              label(context) {
                const value = context.parsed.y;
                return context.dataset.yAxisID === 'y1'
                  ? `${context.dataset.label}: ${formatPercent(value)}`
                  : `${context.dataset.label}: ${formatNumber(value)}`;
              },
            },
          },
        },
      },
    });
  }

  if (!segmentChart) {
    segmentChart = new Chart(document.getElementById('segment-chart'), {
      type: 'bar',
      data: { labels: [], datasets: [] },
      options: {
        indexAxis: 'y',
        maintainAspectRatio: false,
        responsive: true,
        scales: {
          x: { stacked: true, beginAtZero: true, ticks: { callback: (value) => formatNumber(value) } },
          y: { stacked: true },
        },
        plugins: {
          legend: { position: 'bottom' },
          tooltip: {
            callbacks: {
              label(context) {
                return `${context.dataset.label}: ${formatNumber(context.parsed.x)}`;
              },
            },
          },
        },
      },
    });
  }
}

function renderCharts(rows) {
  ensureCharts();

  const dailyRows = summarizeByDate(rows);
  dailyChart.data.labels = dailyRows.map((row) => formatDate(row.date));
  dailyChart.data.datasets = [
    {
      type: 'bar',
      label: 'Объем',
      data: dailyRows.map((row) => row.uploadVolume),
      backgroundColor: `${chartPalette.cyan}B3`,
      borderColor: chartPalette.cyan,
      borderWidth: 1,
      borderRadius: 4,
      yAxisID: 'y',
    },
    {
      type: 'line',
      label: 'CR',
      data: dailyRows.map((row) => row.cr),
      borderColor: chartPalette.violet,
      backgroundColor: chartPalette.violet,
      tension: 0.25,
      pointRadius: 3,
      yAxisID: 'y1',
    },
  ];
  dailyChart.update();

  const sourceRows = summarizeBySource(rows);
  sourceChart.data.labels = sourceRows.map((row) => row.source);
  sourceChart.data.datasets = [{
    data: sourceRows.map((row) => row.uploadVolume),
    backgroundColor: [
      chartPalette.cyan,
      chartPalette.emerald,
      chartPalette.amber,
      chartPalette.coral,
      chartPalette.lime,
      chartPalette.violet,
    ],
    borderWidth: 0,
  }];
  sourceChart.update();

  const roundRows = summarizeByRound(rows);
  roundChart.data.labels = roundRows.map((row) => `Круг ${row.round}`);
  roundChart.data.datasets = [
    {
      type: 'bar',
      label: 'Объем',
      data: roundRows.map((row) => row.uploadVolume),
      backgroundColor: `${chartPalette.emerald}B3`,
      borderColor: chartPalette.emerald,
      borderWidth: 1,
      borderRadius: 4,
      yAxisID: 'y',
    },
    {
      type: 'line',
      label: 'CR',
      data: roundRows.map((row) => row.cr),
      borderColor: chartPalette.coral,
      backgroundColor: chartPalette.coral,
      tension: 0.25,
      pointRadius: 3,
      yAxisID: 'y1',
    },
  ];
  roundChart.update();

  const segmentRows = summarizeBySegment(rows).slice(0, 8).reverse();
  segmentChart.data.labels = segmentRows.map((row) => row.segment);
  segmentChart.data.datasets = [
    {
      label: 'В процессе',
      data: segmentRows.map((row) => row.working),
      backgroundColor: chartPalette.emerald,
      borderRadius: 4,
    },
    {
      label: 'В доработке',
      data: segmentRows.map((row) => row.revision),
      backgroundColor: chartPalette.amber,
      borderRadius: 4,
    },
    {
      label: 'Проиграно',
      data: segmentRows.map((row) => row.lost),
      backgroundColor: chartPalette.coral,
      borderRadius: 4,
    },
    {
      label: 'Сконвертировано',
      data: segmentRows.map((row) => row.converted),
      backgroundColor: chartPalette.lime,
      borderRadius: 4,
    },
  ];
  segmentChart.update();
}

function exportCsv(rows) {
  const header = ['Дата', 'Источник', 'База', 'Круг', 'Объем', 'CR', 'В процессе', 'В доработке', 'Проиграно', 'Сконвертировано'];
  const body = rows.map((row) => [
    row.uploadDate,
    row.sourceLabel,
    row.baseLabel,
    row.roundNumber,
    row.uploadVolume,
    row.cr,
    row.working,
    row.revision,
    row.lost,
    row.converted,
  ]);
  const csv = [header, ...body].map((line) => line.map(csvEscape).join(';')).join('\r\n');
  const blob = new Blob([`\uFEFF${csv}`], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = 'base-dashboard-export.csv';
  link.click();
  URL.revokeObjectURL(url);
}

function render() {
  const rows = filteredRows();
  populateDetailDateSelect(rows);
  const detailView = detailRowsForView(rows);
  renderHero(rows);
  renderKpis(rows);
  renderActiveState(rows);
  renderSourceSummaryTable(rows);
  renderDetailCaption(detailView);
  renderDetailTable(detailView.rows);
  renderCharts(rows);
}

function bindHeaderState() {
  if (!els.siteHeader || els.navLinks.length === 0) return;

  const sections = els.navLinks
    .map((link) => document.querySelector(link.getAttribute('href')))
    .filter(Boolean);

  const syncHeader = () => {
    els.siteHeader.classList.toggle('is-scrolled', window.scrollY > 16);

    const checkpoint = window.scrollY + 130;
    let activeSection = sections[0];
    for (const section of sections) {
      if (section.offsetTop <= checkpoint) activeSection = section;
    }

    for (const link of els.navLinks) {
      const target = link.getAttribute('href');
      link.classList.toggle('is-active', activeSection && `#${activeSection.id}` === target);
    }
  };

  syncHeader();
  window.addEventListener('scroll', syncHeader, { passive: true });
}

function bindControls() {
  els.source.addEventListener('change', () => {
    state.source = els.source.value;
    render();
  });
  els.segment.addEventListener('change', () => {
    state.segment = els.segment.value;
    render();
  });
  els.round.addEventListener('change', () => {
    state.round = els.round.value;
    render();
  });
  els.dateFrom.addEventListener('change', () => {
    state.dateFrom = els.dateFrom.value;
    render();
  });
  els.dateTo.addEventListener('change', () => {
    state.dateTo = els.dateTo.value;
    render();
  });
  els.search.addEventListener('input', () => {
    state.search = els.search.value;
    render();
  });
  els.detailDate.addEventListener('change', () => {
    state.detailDate = els.detailDate.value;
    render();
  });
  els.reset.addEventListener('click', () => {
    const defaultRange = resolveDefaultDateRange(data.filters);
    state.source = 'all';
    state.segment = 'all';
    state.round = 'all';
    state.dateFrom = defaultRange.dateFrom;
    state.dateTo = defaultRange.dateTo;
    state.detailDate = 'latest';
    state.search = '';

    els.source.value = 'all';
    els.segment.value = 'all';
    els.round.value = 'all';
    els.dateFrom.value = state.dateFrom;
    els.dateTo.value = state.dateTo;
    els.detailDate.value = state.detailDate;
    els.search.value = '';
    render();
  });
  els.exportCsv.addEventListener('click', () => exportCsv(detailRowsForView(filteredRows()).rows));
}

function init() {
  if (!data) {
    document.body.innerHTML = '<main class="page-shell"><section class="panel"><div class="panel-head"><h2>Нет данных</h2></div></section></main>';
    return;
  }

  const defaultRange = resolveDefaultDateRange(data.filters);
  state.dateFrom = defaultRange.dateFrom;
  state.dateTo = defaultRange.dateTo;

  setText(els.reportPeriod, buildDateRangeLabel(data.report.from, data.report.to));
  setText(els.updatedAt, new Intl.DateTimeFormat('ru-RU', { dateStyle: 'short', timeStyle: 'short' }).format(new Date(data.generatedAt)));

  populateSelect(els.source, data.filters.sources, 'Все источники');
  populateSelect(els.segment, data.filters.segments, 'Все базы');
  populateSelect(els.round, data.filters.rounds, 'Все круги');

  els.dateFrom.value = state.dateFrom;
  els.dateTo.value = state.dateTo;
  els.dateFrom.min = data.filters.minDate || '';
  els.dateFrom.max = data.filters.maxDate || '';
  els.dateTo.min = data.filters.minDate || '';
  els.dateTo.max = data.filters.maxDate || '';

  bindControls();
  bindHeaderState();
  render();
}

init();
