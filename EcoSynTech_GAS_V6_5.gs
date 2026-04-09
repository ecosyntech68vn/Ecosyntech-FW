/**
 * EcoSynTech GAS V6.5 - Smart Control + QR Traceability + Advisory Engine
 * Version: 6.5.0
 * Features:
 * - Smart Control Engine với rule CRUD, hysteresis, cooldown
 * - QR Code Rendering cho truy xuất nguồn gốc
 * - Advisory Engine (AI nhẹ) cho cảnh báo sớm
 * - MQTT Integration
 */

const CFG = {
  VERSION: '6.5.0',
  SHEETS: {
    BATCH_INFO: 'BatchInfo',
    DYNAMIC_QR: 'DynamicQR',
    MANUAL_EVENTS: 'ManualEvents',
    BATCH_SHEET: 'Batchsheet',
    BATCH_MEDIA: 'BatchMedia',
    DEVICES: 'Devices',
    RULES: 'Rules',
    SCHEDULES: 'Schedules',
    ALERTS: 'Alerts',
    CONFIG: 'Config'
  },
  ADVISORY_DEFAULTS: {
    temp_high: 38.0,
    temp_low: 16.0,
    hum_low: 45.0,
    soil_low: 30.0,
    light_low: 5000,
    battery_low: 3.5
  }
};

const AdvisoryEngine = (() => {
  const DEFAULTS = CFG.ADVISORY_DEFAULTS;

  function analyzeLatestReadings(readings, context) {
    context = context || {};
    const alerts = [];
    const actions = [];
    const latest = indexLatestByType_(readings);

    if (latest.temperature !== null) {
      if (latest.temperature >= DEFAULTS.temp_high) {
        alerts.push({
          level: 'high',
          code: 'TEMP_HIGH',
          message: `Nhiệt độ cao bất thường: ${latest.temperature}°C`
        });
        actions.push('Bật quạt / tăng thông gió');
      }
      if (latest.temperature <= DEFAULTS.temp_low) {
        alerts.push({
          level: 'medium',
          code: 'TEMP_LOW',
          message: `Nhiệt độ thấp: ${latest.temperature}°C`
        });
      }
    }

    if (latest.humidity !== null && latest.humidity < DEFAULTS.hum_low) {
      alerts.push({
        level: 'medium',
        code: 'HUM_LOW',
        message: `Độ ẩm không khí thấp: ${latest.humidity}%`
      });
      actions.push('Theo dõi bốc hơi, cân nhắc tưới/phun sương');
    }

    if (latest.soil_moisture !== null && latest.soil_moisture < DEFAULTS.soil_low) {
      alerts.push({
        level: 'high',
        code: 'SOIL_DRY',
        message: `Đất khô: ${latest.soil_moisture}%`
      });
      actions.push('Kích hoạt tưới theo zone');
    }

    if (latest.light !== null && latest.light < DEFAULTS.light_low) {
      alerts.push({
        level: 'low',
        code: 'LOW_LIGHT',
        message: `Ánh sáng thấp: ${latest.light} lux`
      });
    }

    if (latest.battery !== null && latest.battery < DEFAULTS.battery_low) {
      alerts.push({
        level: 'high',
        code: 'LOW_BATTERY',
        message: `Pin yếu: ${latest.battery}V`
      });
      actions.push('Chuyển sang chế độ tiết kiệm năng lượng');
    }

    const anomalyScore = computeSimpleAnomalyScore_(readings, context);

    return {
      ok: true,
      anomaly_score: anomalyScore,
      alerts,
      suggested_actions: unique_(actions),
      summary: alerts.length
        ? `${alerts.length} cảnh báo, cần theo dõi`
        : 'Thông số ổn định'
    };
  }

  function indexLatestByType_(readings) {
    const out = { temperature: null, humidity: null, soil_moisture: null, light: null, battery: null };
    if (!Array.isArray(readings)) return out;

    readings.forEach(r => {
      const type = String(r.sensor_type || r.type || '').toLowerCase();
      const val = Number(r.value);
      if (isNaN(val)) return;

      if (type.includes('temp')) out.temperature = val;
      else if (type.includes('humid') || type.includes('hum')) out.humidity = val;
      else if (type.includes('soil') || type.includes('moisture')) out.soil_moisture = val;
      else if (type.includes('light') || type.includes('lux')) out.light = val;
      else if (type.includes('battery') || type.includes('volt')) out.battery = val;
    });

    return out;
  }

  function computeSimpleAnomalyScore_(readings, context) {
    if (!Array.isArray(readings) || readings.length < 3) return 0;
    let score = 0;
    const values = readings.map(r => Number(r.value)).filter(v => !isNaN(v));
    if (values.length < 3) return 0;

    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const variance = values.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / values.length;
    const stdev = Math.sqrt(variance);

    if (stdev > 8) score += 35;
    if (stdev > 15) score += 30;

    const last = values[values.length - 1];
    const prev = values[values.length - 2];
    if (Math.abs(last - prev) > 10) score += 25;

    if (context.offline_hours && context.offline_hours > 1) score += 15;
    return Math.min(score, 100);
  }

  function unique_(arr) {
    return [...new Set(arr)];
  }

  return { analyzeLatestReadings };
})();

function _renderQrHtml(params) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const qrId = String((params && params.qr) || '').trim();
  
  if (!qrId) {
    return HtmlService.createHtmlOutput(
      `<h3>QR không hợp lệ</h3><p>Thiếu tham số qr.</p>`
    );
  }

  const dynamicSheet = ss.getSheetByName(CFG.SHEETS.DYNAMIC_QR);
  const batchInfoSheet = ss.getSheetByName(CFG.SHEETS.BATCH_INFO);
  const manualSheet = ss.getSheetByName(CFG.SHEETS.MANUAL_EVENTS);
  const sensorSheet = ss.getSheetByName(CFG.SHEETS.BATCH_SHEET);
  const mediaSheet = ss.getSheetByName(CFG.SHEETS.BATCH_MEDIA);

  if (!dynamicSheet || !batchInfoSheet) {
    return HtmlService.createHtmlOutput(
      `<h3>Thiếu dữ liệu truy xuất</h3><p>Chưa có sheet DynamicQR hoặc BatchInfo.</p>`
    );
  }

  const dynamicRows = dynamicSheet.getDataRange().getValues();
  if (dynamicRows.length < 2) {
    return HtmlService.createHtmlOutput(
      `<h3>Chưa có QR động</h3><p>Không tìm thấy dữ liệu QR.</p>`
    );
  }

  const dHeaders = dynamicRows[0].map(h => String(h).trim().toLowerCase());
  const qrCol = dHeaders.indexOf('qr_id');
  const batchCol = dHeaders.indexOf('batch_id');
  const createdCol = dHeaders.indexOf('created_at');
  const metaCol = dHeaders.indexOf('metadata');

  let batchId = '';
  let createdAt = '';
  let qrMeta = {};

  for (let i = 1; i < dynamicRows.length; i++) {
    if (String(dynamicRows[i][qrCol]).trim() === qrId) {
      batchId = String(dynamicRows[i][batchCol] || '').trim();
      createdAt = createdCol >= 0 ? dynamicRows[i][createdCol] : '';
      if (metaCol >= 0 && dynamicRows[i][metaCol]) {
        try {
          qrMeta = JSON.parse(dynamicRows[i][metaCol]);
        } catch (e) {}
      }
      break;
    }
  }

  if (!batchId) {
    return HtmlService.createHtmlOutput(
      `<h3>Không tìm thấy batch</h3><p>QR này chưa liên kết với batch nào.</p>`
    );
  }

  const batchRows = batchInfoSheet.getDataRange().getValues();
  const bHeaders = batchRows[0].map(h => String(h).trim().toLowerCase());
  const bIdCol = bHeaders.indexOf('batch_id');

  let batch = null;
  for (let i = 1; i < batchRows.length; i++) {
    if (String(batchRows[i][bIdCol]).trim() === batchId) {
      batch = {};
      bHeaders.forEach((h, idx) => batch[h] = batchRows[i][idx]);
      break;
    }
  }

  if (!batch) {
    return HtmlService.createHtmlOutput(
      `<h3>Không tìm thấy thông tin batch</h3><p>Batch ID: ${batchId}</p>`
    );
  }

  const manualEvents = loadRowsByBatch_(manualSheet, batchId, 'batch_id', 'timestamp');
  const sensorEvents = loadRowsByBatch_(sensorSheet, batchId, 'batch_id', 'event_ts');
  const mediaItems = loadRowsByBatch_(mediaSheet, batchId, 'batch_id', 'created_at');

  const sensorChartData = buildSensorChartData_(sensorEvents);
  const eventTimeline = buildEventTimeline_(manualEvents);
  const mediaHtml = buildMediaHtml_(mediaItems);

  const blockchainTx = String(batch['blockchain_tx'] || batch['blockchain_tx_hash'] || '').trim();
  const blockchainLink = blockchainTx
    ? `https://hashscan.io/hedera/mainnet/transaction/${encodeURIComponent(blockchainTx)}`
    : '';

  const cropType = esc_(batch['crop_type'] || batch['type'] || '');
  const zone = esc_(batch['zone'] || '');
  const startDate = esc_(batch['start_date'] || '');
  const endDate = esc_(batch['end_date'] || '');
  const status = esc_(batch['status'] || '');
  const qrCreated = esc_(createdAt || '');
  const batchName = esc_(batch['batch_name'] || batchId);

  const html = `
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Truy xuất nguồn gốc - ${batchName}</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    body { background: #f7f9fb; }
    .hero {
      background: linear-gradient(135deg, #0f7a3a, #1ea65a);
      color: #fff;
      border-radius: 18px;
      padding: 24px;
      box-shadow: 0 10px 30px rgba(0,0,0,.08);
    }
    .card-soft {
      border: 0;
      border-radius: 18px;
      box-shadow: 0 8px 24px rgba(0,0,0,.06);
    }
    .badge-soft {
      background: rgba(255,255,255,.18);
      border: 1px solid rgba(255,255,255,.25);
      color: #fff;
      font-weight: 600;
    }
    .meta-label { color: #667085; font-size: .88rem; }
    .meta-value { font-weight: 700; color: #101828; }
    .timeline-item {
      border-left: 2px solid #d0d5dd;
      padding-left: 14px;
      margin-left: 8px;
      margin-bottom: 14px;
      position: relative;
    }
    .timeline-item::before {
      content: '';
      width: 10px;
      height: 10px;
      background: #1ea65a;
      border-radius: 50%;
      position: absolute;
      left: -6px;
      top: 6px;
    }
    .media-thumb {
      width: 100%;
      height: 170px;
      object-fit: cover;
      border-radius: 14px;
      background: #e9ecef;
    }
    @media print {
      .no-print { display: none !important; }
      body { background: #fff; }
      .card-soft, .hero { box-shadow: none !important; }
    }
  </style>
</head>
<body>
<div class="container py-4">
  <div class="hero mb-4">
    <div class="d-flex flex-wrap justify-content-between align-items-start gap-3">
      <div>
        <div class="mb-2"><span class="badge badge-soft">EcoSynTech Traceability</span></div>
        <h1 class="h3 mb-2">${batchName}</h1>
        <div class="opacity-75">Mã batch: <strong>${batchId}</strong> • QR: <strong>${qrId}</strong></div>
      </div>
      <div class="text-end">
        <div class="meta-label text-white-50">Trạng thái</div>
        <div class="h5 mb-1">${status || 'active'}</div>
        <div class="meta-label text-white-50">QR tạo lúc</div>
        <div class="small">${qrCreated || '-'}</div>
      </div>
    </div>
  </div>

  <div class="row g-3 mb-3">
    <div class="col-md-3"><div class="card card-soft p-3"><div class="meta-label">Loại cây</div><div class="meta-value">${cropType || '-'}</div></div></div>
    <div class="col-md-3"><div class="card card-soft p-3"><div class="meta-label">Khu vực</div><div class="meta-value">${zone || '-'}</div></div></div>
    <div class="col-md-3"><div class="card card-soft p-3"><div class="meta-label">Ngày bắt đầu</div><div class="meta-value">${startDate || '-'}</div></div></div>
    <div class="col-md-3"><div class="card card-soft p-3"><div class="meta-label">Ngày kết thúc</div><div class="meta-value">${endDate || '-'}</div></div></div>
  </div>

  <div class="card card-soft mb-3">
    <div class="card-body">
      <ul class="nav nav-tabs" id="traceTabs" role="tablist">
        <li class="nav-item" role="presentation">
          <button class="nav-link active" data-bs-toggle="tab" data-bs-target="#overview" type="button">Tổng quan</button>
        </li>
        <li class="nav-item" role="presentation">
          <button class="nav-link" data-bs-toggle="tab" data-bs-target="#sensor" type="button">Dữ liệu cảm biến</button>
        </li>
        <li class="nav-item" role="presentation">
          <button class="nav-link" data-bs-toggle="tab" data-bs-target="#timeline" type="button">Nhật ký canh tác</button>
        </li>
        <li class="nav-item" role="presentation">
          <button class="nav-link" data-bs-toggle="tab" data-bs-target="#media" type="button">Hình ảnh</button>
        </li>
        <li class="nav-item" role="presentation">
          <button class="nav-link" data-bs-toggle="tab" data-bs-target="#verify" type="button">Xác thực</button>
        </li>
      </ul>

      <div class="tab-content pt-3">
        <div class="tab-pane fade show active" id="overview">
          <div class="row g-3">
            <div class="col-lg-7">
              <div class="p-3 bg-light rounded-4">
                <h5 class="mb-3">Tóm tắt lô hàng</h5>
                <p class="mb-2"><strong>Batch ID:</strong> ${batchId}</p>
                <p class="mb-2"><strong>Loại cây:</strong> ${cropType || '-'}</p>
                <p class="mb-2"><strong>Khu vực:</strong> ${zone || '-'}</p>
                <p class="mb-2"><strong>Trạng thái:</strong> ${status || '-'}</p>
                <p class="mb-2"><strong>Số sự kiện:</strong> ${manualEvents.length}</p>
                <p class="mb-0"><strong>Số ảnh/video:</strong> ${mediaItems.length}</p>
              </div>
            </div>
            <div class="col-lg-5">
              <div class="p-3 border rounded-4">
                <h5 class="mb-3">Ghi chú truy xuất</h5>
                <div class="small text-secondary">
                  QR này hiển thị dữ liệu vận hành của lô hàng từ lúc gieo trồng đến hiện tại.
                  Nếu có blockchain transaction, hệ thống sẽ hiển thị thêm liên kết xác thực.
                </div>
                <div class="mt-3">
                  <button class="btn btn-outline-success btn-sm no-print" onclick="window.print()">In / Lưu PDF</button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="tab-pane fade" id="sensor">
          <canvas id="sensorChart" height="110"></canvas>
        </div>

        <div class="tab-pane fade" id="timeline">
          ${eventTimeline || '<div class="text-secondary">Chưa có nhật ký canh tác.</div>'}
        </div>

        <div class="tab-pane fade" id="media">
          ${mediaHtml || '<div class="text-secondary">Chưa có hình ảnh / video.</div>'}
        </div>

        <div class="tab-pane fade" id="verify">
          ${
            blockchainLink
              ? `<div class="alert alert-success">
                   <strong>Blockchain hash:</strong> ${esc_(blockchainTx)}<br>
                   <a href="${blockchainLink}" target="_blank" rel="noopener">Mở giao dịch xác thực</a>
                 </div>`
              : `<div class="alert alert-warning">Chưa có giao dịch blockchain cho batch này.</div>`
          }
          <div class="small text-secondary">
            Metadata QR: ${esc_(JSON.stringify(qrMeta))}
          </div>
        </div>
      </div>
    </div>
  </div>
</div>

<script>
const sensorData = ${JSON.stringify(sensorChartData)};
const ctx = document.getElementById('sensorChart');
if (ctx && sensorData.labels.length) {
  new Chart(ctx, {
    type: 'line',
    data: {
      labels: sensorData.labels,
      datasets: sensorData.datasets
    },
    options: {
      responsive: true,
      plugins: { legend: { display: true } },
      scales: { y: { beginAtZero: false } }
    }
  });
}
</script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>`;

  return HtmlService.createHtmlOutput(html).setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function loadRowsByBatch_(sheet, batchId, batchField, sortField) {
  if (!sheet) return [];
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return [];

  const headers = data[0].map(h => String(h).trim().toLowerCase());
  const bCol = headers.indexOf(batchField.toLowerCase());
  const sCol = sortField ? headers.indexOf(sortField.toLowerCase()) : -1;

  const rows = [];
  for (let i = 1; i < data.length; i++) {
    if (String(data[i][bCol]).trim() === String(batchId).trim()) {
      const row = {};
      headers.forEach((h, idx) => row[h] = data[i][idx]);
      rows.push(row);
    }
  }

  if (sCol >= 0) {
    rows.sort((a, b) => String(a[sortField.toLowerCase()] || '').localeCompare(String(b[sortField.toLowerCase()] || '')));
  }
  return rows;
}

function buildSensorChartData_(rows) {
  const labels = [];
  const temp = [];
  const hum = [];
  const soil = [];

  rows.forEach(r => {
    const ts = r.event_ts || r.timestamp || r.received_at || '';
    const type = String(r.sensor_type || '').toLowerCase();
    const val = Number(r.value);
    if (!ts || isNaN(val)) return;

    if (type.includes('temp') || type === 'temperature') {
      labels.push(ts);
      temp.push(val);
    } else if (type.includes('hum') || type === 'humidity') {
      hum.push(val);
    } else if (type.includes('soil') || type.includes('moisture')) {
      soil.push(val);
    }
  });

  const datasets = [];
  if (temp.length) datasets.push({ label: 'Nhiệt độ', data: temp, tension: 0.35 });
  if (hum.length) datasets.push({ label: 'Độ ẩm', data: hum, tension: 0.35 });
  if (soil.length) datasets.push({ label: 'Độ ẩm đất', data: soil, tension: 0.35 });

  return { labels: labels.length ? labels : rows.map((_, i) => `#${i+1}`), datasets };
}

function buildEventTimeline_(rows) {
  if (!rows.length) return '';
  const items = rows.map(r => {
    const ts = esc_(r.timestamp || r.event_ts || '');
    const type = esc_(r.event_type || r.type || '');
    const op = esc_(r.operator || r.created_by || '');
    const note = esc_(r.notes || r.note || '');
    const materials = esc_(r.materials || r.material || '');
    return `
      <div class="timeline-item">
        <div class="fw-bold">${type}</div>
        <div class="small text-secondary">${ts} • ${op}</div>
        <div>${materials}</div>
        <div class="text-muted small">${note}</div>
      </div>`;
  });
  return items.join('');
}

function buildMediaHtml_(rows) {
  if (!rows.length) return '';
  const cols = rows.map(r => {
    const url = String(r.url || r.file_url || r.media_url || '').trim();
    const caption = esc_(r.caption || r.note || r.media_type || '');
    if (!url) return '';
    const isImage = /\.(jpg|jpeg|png|webp|gif)$/i.test(url) || url.includes('drive.google.com');
    return `
      <div class="col-md-4 mb-3">
        <div class="card card-soft h-100">
          ${isImage ? `<img src="${esc_(url)}" class="media-thumb" alt="${caption}">`
                    : `<div class="p-4 text-center bg-light rounded-4">VIDEO / FILE</div>`}
          <div class="card-body">
            <div class="small text-secondary">${caption || 'Media'}</div>
            <a href="${esc_(url)}" target="_blank" rel="noopener">Mở tệp</a>
          </div>
        </div>
      </div>`;
  }).join('');
  return `<div class="row">${cols}</div>`;
}

function esc_(s) {
  s = String(s === undefined || s === null ? '' : s);
  return s.replace(/&/g,'&amp;')
          .replace(/</g,'&lt;')
          .replace(/>/g,'&gt;')
          .replace(/"/g,'&quot;')
          .replace(/'/g,'&#039;');
}

function processLatestFarmData(batchId, readings) {
  const result = AdvisoryEngine.analyzeLatestReadings(readings, { batchId: batchId });

  if (result.alerts.length) {
    EventModule.logEvent(batchId, 'ai_alert', result, 'advisory_engine');
  }

  return result;
}

const SmartControlEngine = (() => {
  const rules = [];
  const ruleStates = {};
  const COOLDOWN_MS = 5000;
  const HYSTERESIS_DEFAULT = 2.0;

  function loadRules() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(CFG.SHEETS.RULES);
    if (!sheet) return;

    const data = sheet.getDataRange().getValues();
    const headers = data[0].map(h => String(h).trim().toLowerCase());
    rules.length = 0;

    for (let i = 1; i < data.length; i++) {
      const row = {};
      headers.forEach((h, idx) => row[h] = data[i][idx]);
      if (row.enabled === true || row.enabled === 'TRUE') {
        rules.push({
          id: row.rule_id || `rule_${i}`,
          name: row.name || '',
          sensor: row.sensor || '',
          operator: row.operator || '>',
          threshold: Number(row.threshold) || 0,
          hysteresis: Number(row.hysteresis) || HYSTERESIS_DEFAULT,
          action: row.action || '',
          enabled: true,
          zone: row.zone || 'default'
        });
      }
    }
  }

  function evaluateRules(readings) {
    const results = [];
    const now = Date.now();

    readings.forEach(reading => {
      const sensorType = String(reading.sensor_type || reading.type || '').toLowerCase();
      const value = Number(reading.value);
      const zone = reading.zone || 'default';

      rules.filter(r => r.sensor.toLowerCase() === sensorType && r.zone === zone).forEach(rule => {
        let triggered = false;
        const state = ruleStates[rule.id] || { triggeredAtMs: 0, active: false, lastValue: NaN };

        switch (rule.operator) {
          case '>': triggered = value > rule.threshold; break;
          case '<': triggered = value < rule.threshold; break;
          case '>=': triggered = value >= rule.threshold; break;
          case '<=': triggered = value <= rule.threshold; break;
          case '==': triggered = Math.abs(value - rule.threshold) < 0.01; break;
        }

        if (triggered && !state.active) {
          if (now - state.triggeredAtMs > COOLDOWN_MS) {
            results.push({
              rule: rule,
              action: rule.action,
              value: value,
              threshold: rule.threshold,
              timestamp: now
            });
            state.active = true;
            state.triggeredAtMs = now;
          }
        } else if (!triggered && state.active) {
          if (Math.abs(value - rule.threshold) > rule.hysteresis) {
            state.active = false;
          }
        }

        state.lastValue = value;
        ruleStates[rule.id] = state;
      });
    });

    return results;
  }

  function executeAction(action, params) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const logSheet = ss.getSheetByName('ActionLog');
    if (!logSheet) return;

    const now = new Date();
    logSheet.appendRow([
      now.toISOString(),
      action,
      JSON.stringify(params),
      'pending'
    ]);
  }

  return { loadRules, evaluateRules, executeAction };
})();

function doGet() {
  return HtmlService.createTemplateFromFile('index')
    .evaluate()
    .setTitle('EcoSynTech GAS V6.5')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function doPost(e) {
  return handleWebhook(e);
}

function handleWebhook(e) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = ss.getName();
  const payload = e.parameter;
  const postData = e.postData;

  if (postData && postData.contents) {
    try {
      const data = JSON.parse(postData.contents);
      if (data.sensor_data) {
        const sensorSheet = ss.getSheetByName(CFG.SHEETS.BATCH_SHEET) || ss.insertSheet(CFG.SHEETS.BATCH_SHEET);
        sensorSheet.appendRow([
          new Date().toISOString(),
          data.device_id || '',
          data.sensor_type || '',
          data.value || '',
          data.zone || 'default',
          data.batch_id || ''
        ]);
      }

      if (data.commands) {
        const results = SmartControlEngine.evaluateRules(data.readings || []);
        results.forEach(r => {
          SmartControlEngine.executeAction(r.action, r);
        });
      }
    } catch (err) {
      return ContentService.createTextOutput(JSON.stringify({ error: err.message }))
        .setMimeType(ContentService.MimeType.JSON);
    }
  }

  return ContentService.createTextOutput(JSON.stringify({ success: true, version: CFG.VERSION }))
    .setMimeType(ContentService.MimeType.JSON);
}

function getApiEndpoints() {
  return {
    version: CFG.VERSION,
    endpoints: {
      qr: '/qr?qr=QR_ID',
      webhook: '/',
      sensor: '/sensor',
      command: '/command'
    }
  };
}
