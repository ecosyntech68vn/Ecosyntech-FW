/**
 * EcoSynTech GAS V7.0 - ULTIMATE EDITION
 * Version: 7.0.0
 * 
 * Kế thừa đầy đủ từ V6.5 và nâng cấp toàn diện:
 * - QR Code Rendering hoàn chỉnh
 * - Advisory Engine (AI nhẹ) nâng cao
 * - Smart Control Engine với hysteresis & cooldown
 * - Telegram Bot Integration
 * - Firmware Communication Protocol
 * - Bidirectional Data Flow
 * - Device Health Monitoring
 * - Real-time Notifications
 */

const CFG = {
  VERSION: '7.0.0',
  BUILD: '2026.04.09',
  ENVIRONMENT: 'production',
  
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
    CONFIG: 'Config',
    TELEGRAM_LOG: 'TelegramLog',
    ACTION_LOG: 'ActionLog',
    SENSOR_CACHE: 'SensorCache',
    DEVICE_METRICS: 'DeviceMetrics',
    FIRMWARE_LOGS: 'FirmwareLogs'
  },

  TELEGRAM: {
    BOT_TOKEN: '',
    ALLOWED_CHATS: [],
    NOTIFY_ON_ALERT: true,
    NOTIFY_ON_COMMAND: false,
    NOTIFY_ON_STATUS: true
  },

  ADVISORY_DEFAULTS: {
    temp_high: 38.0,
    temp_low: 16.0,
    temp_critical: 42.0,
    hum_low: 45.0,
    hum_high: 90.0,
    soil_low: 30.0,
    soil_high: 80.0,
    light_low: 5000,
    light_high: 80000,
    battery_low: 3.5,
    battery_critical: 3.2,
    co2_high: 1000,
    ph_low: 5.5,
    ph_high: 7.5
  },

  FIRMWARE: {
    PROTOCOL_VERSION: '1.0',
    COMMAND_TIMEOUT_MS: 30000,
    MAX_RETRY: 3,
    HEARTBEAT_INTERVAL_SEC: 300,
    COMMAND_QUEUE_SIZE: 100
  },

  CACHE: {
    ENABLE: true,
    TTL_MS: 60000,
    MAX_SIZE: 500
  }
};

const CACHE_STORE = CacheService.getScriptCache();
const PROPERTIES = PropertiesService.getScriptProperties();

function getConfig() {
  const telegramToken = PROPERTIES.getProperty('TELEGRAM_BOT_TOKEN');
  const allowedChats = PROPERTIES.getProperty('TELEGRAM_ALLOWED_CHATS');
  
  return {
    ...CFG,
    TELEGRAM: {
      ...CFG.TELEGRAM,
      BOT_TOKEN: telegramToken || CFG.TELEGRAM.BOT_TOKEN,
      ALLOWED_CHATS: allowedChats ? allowedChats.split(',') : CFG.TELEGRAM.ALLOWED_CHATS
    }
  };
}

// ==================== UTILITY FUNCTIONS ====================

function esc_(s) {
  s = String(s === undefined || s === null ? '' : s);
  return s.replace(/&/g,'&amp;')
          .replace(/</g,'&lt;')
          .replace(/>/g,'&gt;')
          .replace(/"/g,'&quot;')
          .replace(/'/g,'&#039;');
}

function generateId(prefix = 'id') {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
}

function getSheetData(sheetName, useCache = true) {
  const cacheKey = `sheet_${sheetName}`;
  
  if (useCache && CFG.CACHE.ENABLE) {
    const cached = CACHE_STORE.get(cacheKey);
    if (cached) return JSON.parse(cached);
  }
  
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) return null;
  
  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return { headers: [], rows: [] };
  
  const headers = data[0].map(h => String(h).trim().toLowerCase());
  const rows = [];
  for (let i = 1; i < data.length; i++) {
    const row = {};
    headers.forEach((h, idx) => row[h] = data[i][idx]);
    rows.push(row);
  }
  
  const result = { headers, rows };
  
  if (useCache && CFG.CACHE.ENABLE) {
    CACHE_STORE.put(cacheKey, JSON.stringify(result), CFG.CACHE.TTL_MS / 1000);
  }
  
  return result;
}

function clearSheetCache(sheetName) {
  if (sheetName) {
    CACHE_STORE.remove(`sheet_${sheetName}`);
  } else {
    CACHE_STORE.removeAll(CACHE_STORE.getKeys().filter(k => k.startsWith('sheet_')));
  }
}

function logToSheet(sheetName, data) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    sheet = ss.insertSheet(sheetName);
  }
  
  if (Array.isArray(data[0])) {
    sheet.getRange(sheet.getLastRow() + 1, 1, data.length, data[0].length).setValues(data);
  } else {
    sheet.appendRow(data);
  }
  
  clearSheetCache(sheetName);
}

// ==================== QR CODE RENDERING ====================

function _renderQrHtml(params) {
  const qrId = String((params && params.qr) || '').trim();
  
  if (!qrId) {
    return HtmlService.createHtmlOutput(
      `<h3>QR không hợp lệ</h3><p>Thiếu tham số qr.</p>`
    );
  }

  const dynamicSheet = getSheetData(CFG.SHEETS.DYNAMIC_QR, false);
  const batchInfoSheet = getSheetData(CFG.SHEETS.BATCH_INFO, false);
  
  if (!dynamicSheet || !batchInfoSheet) {
    return HtmlService.createHtmlOutput(
      `<h3>Thiếu dữ liệu truy xuất</h3><p>Chưa có sheet DynamicQR hoặc BatchInfo.</p>`
    );
  }

  const qrRow = dynamicSheet.rows.find(r => 
    String(r.qr_id || '').trim() === qrId
  );

  if (!qrRow) {
    return HtmlService.createHtmlOutput(
      `<h3>Không tìm thấy QR</h3><p>Mã QR: ${qrId}</p>`
    );
  }

  const batchId = String(qrRow.batch_id || '').trim();
  const createdAt = qrRow.created_at || '';
  let qrMeta = {};
  try {
    qrMeta = qrRow.metadata ? JSON.parse(qrRow.metadata) : {};
  } catch (e) {}

  const batch = batchInfoSheet.rows.find(r => 
    String(r.batch_id || '').trim() === batchId
  );

  if (!batch) {
    return HtmlService.createHtmlOutput(
      `<h3>Không tìm thấy batch</h3><p>Batch ID: ${batchId}</p>`
    );
  }

  const manualEvents = getRowsByBatch(CFG.SHEETS.MANUAL_EVENTS, batchId);
  const sensorEvents = getRowsByBatch(CFG.SHEETS.BATCH_SHEET, batchId);
  const mediaItems = getRowsByBatch(CFG.SHEETS.BATCH_MEDIA, batchId);

  const sensorChartData = buildSensorChartData(sensorEvents);
  const eventTimeline = buildEventTimeline(manualEvents);
  const mediaHtml = buildMediaHtml(mediaItems);

  const blockchainTx = String(batch.blockchain_tx || batch.blockchain_tx_hash || '').trim();
  const blockchainLink = blockchainTx
    ? `https://hashscan.io/hedera/mainnet/transaction/${encodeURIComponent(blockchainTx)}`
    : '';

  const cropType = esc_(batch.crop_type || batch.type || '');
  const zone = esc_(batch.zone || '');
  const startDate = esc_(batch.start_date || '');
  const endDate = esc_(batch.end_date || '');
  const status = esc_(batch.status || '');
  const qrCreated = esc_(createdAt || '');
  const batchName = esc_(batch.batch_name || batchId);

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
    :root { --primary: #0f7a3a; --secondary: #1ea65a; }
    body { background: #f7f9fb; font-family: 'Segoe UI', sans-serif; }
    .hero { background: linear-gradient(135deg, var(--primary), var(--secondary)); color: #fff; border-radius: 18px; padding: 24px; box-shadow: 0 10px 30px rgba(0,0,0,.08); }
    .card-soft { border: 0; border-radius: 18px; box-shadow: 0 8px 24px rgba(0,0,0,.06); }
    .badge-soft { background: rgba(255,255,255,.18); border: 1px solid rgba(255,255,255,.25); color: #fff; font-weight: 600; }
    .meta-label { color: #667085; font-size: .88rem; }
    .meta-value { font-weight: 700; color: #101828; }
    .timeline-item { border-left: 2px solid #d0d5dd; padding-left: 14px; margin-left: 8px; margin-bottom: 14px; position: relative; }
    .timeline-item::before { content: ''; width: 10px; height: 10px; background: var(--secondary); border-radius: 50%; position: absolute; left: -6px; top: 6px; }
    .media-thumb { width: 100%; height: 170px; object-fit: cover; border-radius: 14px; background: #e9ecef; }
    .status-badge { padding: 4px 12px; border-radius: 20px; font-size: 0.85rem; }
    .status-active { background: #d1fae5; color: #065f46; }
    .status-harvested { background: #fef3c7; color: #92400e; }
    .status-completed { background: #dbeafe; color: #1e40af; }
    @media print { .no-print { display: none !important; } body { background: #fff; } .card-soft, .hero { box-shadow: none !important; } }
  </style>
</head>
<body>
<div class="container py-4">
  <div class="hero mb-4">
    <div class="d-flex flex-wrap justify-content-between align-items-start gap-3">
      <div>
        <div class="mb-2"><span class="badge badge-soft">EcoSynTech V7.0 Traceability</span></div>
        <h1 class="h3 mb-2">${batchName}</h1>
        <div class="opacity-75">Mã batch: <strong>${batchId}</strong> • QR: <strong>${qrId}</strong></div>
      </div>
      <div class="text-end">
        <div class="meta-label text-white-50">Trạng thái</div>
        <div class="h5 mb-1"><span class="status-badge status-${status || 'active'}">${status || 'active'}</span></div>
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
        <li class="nav-item"><button class="nav-link active" data-bs-toggle="tab" data-bs-target="#overview" type="button">Tổng quan</button></li>
        <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#sensor" type="button">Dữ liệu cảm biến</button></li>
        <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#timeline" type="button">Nhật ký canh tác</button></li>
        <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#media" type="button">Hình ảnh</button></li>
        <li class="nav-item"><button class="nav-link" data-bs-toggle="tab" data-bs-target="#verify" type="button">Xác thực</button></li>
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
                  Dữ liệu được đồng bộ từ thiết bị IoT và cập nhật real-time.
                </div>
                <div class="mt-3">
                  <button class="btn btn-outline-success btn-sm no-print" onclick="window.print()">In / Lưu PDF</button>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div class="tab-pane fade" id="sensor">
          ${sensorChartData.labels.length ? `<canvas id="sensorChart" height="110"></canvas>` : '<div class="text-secondary">Chưa có dữ liệu cảm biến.</div>'}
        </div>
        <div class="tab-pane fade" id="timeline">
          ${eventTimeline || '<div class="text-secondary">Chưa có nhật ký canh tác.</div>'}
        </div>
        <div class="tab-pane fade" id="media">
          ${mediaHtml || '<div class="text-secondary">Chưa có hình ảnh / video.</div>'}
        </div>
        <div class="tab-pane fade" id="verify">
          ${blockchainLink
            ? `<div class="alert alert-success"><strong>Blockchain hash:</strong> ${esc_(blockchainTx)}<br><a href="${blockchainLink}" target="_blank">Mở giao dịch xác thực</a></div>`
            : `<div class="alert alert-warning">Chưa có giao dịch blockchain cho batch này.</div>`
          }
          <div class="small text-secondary">Metadata QR: ${esc_(JSON.stringify(qrMeta))}</div>
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
    data: { labels: sensorData.labels, datasets: sensorData.datasets },
    options: { responsive: true, plugins: { legend: { display: true } }, scales: { y: { beginAtZero: false } } }
  });
}
</script>
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>`;

  return HtmlService.createHtmlOutput(html).setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function getRowsByBatch(sheetName, batchId) {
  const data = getSheetData(sheetName, false);
  if (!data || !data.rows) return [];
  
  return data.rows.filter(r => 
    String(r.batch_id || '').trim() === String(batchId).trim()
  );
}

function buildSensorChartData(rows) {
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
      labels.push(ts.substring(0, 16));
      temp.push(val);
    } else if (type.includes('hum') || type === 'humidity') hum.push(val);
    else if (type.includes('soil') || type.includes('moisture')) soil.push(val);
  });

  const datasets = [];
  if (temp.length) datasets.push({ label: 'Nhiệt độ (°C)', data: temp, borderColor: '#f97316', backgroundColor: 'rgba(249,115,22,0.1)', tension: 0.35, fill: true });
  if (hum.length) datasets.push({ label: 'Độ ẩm (%)', data: hum, borderColor: '#06b6d4', backgroundColor: 'rgba(6,182,212,0.1)', tension: 0.35, fill: true });
  if (soil.length) datasets.push({ label: 'Độ ẩm đất (%)', data: soil, borderColor: '#8b5cf6', backgroundColor: 'rgba(139,92,246,0.1)', tension: 0.35, fill: true });

  return { labels: labels.length ? labels : rows.map((_, i) => `#${i+1}`), datasets };
}

function buildEventTimeline(rows) {
  if (!rows.length) return '';
  return rows.slice(-20).map(r => {
    const ts = esc_(r.timestamp || r.event_ts || '');
    const type = esc_(r.event_type || r.type || '');
    const op = esc_(r.operator || r.created_by || '');
    const note = esc_(r.notes || r.note || '');
    const materials = esc_(r.materials || r.material || '');
    return `<div class="timeline-item"><div class="fw-bold">${type}</div><div class="small text-secondary">${ts} • ${op}</div><div>${materials}</div><div class="text-muted small">${note}</div></div>`;
  }).join('');
}

function buildMediaHtml(rows) {
  if (!rows.length) return '';
  return rows.slice(-9).map(r => {
    const url = String(r.url || r.file_url || r.media_url || '').trim();
    const caption = esc_(r.caption || r.note || r.media_type || '');
    if (!url) return '';
    const isImage = /\.(jpg|jpeg|png|webp|gif)$/i.test(url) || url.includes('drive.google.com');
    return `<div class="col-md-4 mb-3"><div class="card card-soft h-100">${isImage ? `<img src="${esc_(url)}" class="media-thumb" alt="${caption}">` : `<div class="p-4 text-center bg-light rounded-4">🎬 VIDEO</div>`}<div class="card-body"><div class="small text-secondary">${caption || 'Media'}</div><a href="${esc_(url)}" target="_blank">Mở tệp</a></div></div></div>`;
  }).join('');
}

// ==================== ADVISORY ENGINE (AI NHẸ) ====================

const AdvisoryEngine = (() => {
  const DEFAULTS = CFG.ADVISORY_DEFAULTS;

  function analyzeLatestReadings(readings, context = {}) {
    const alerts = [];
    const actions = [];
    const info = [];
    const latest = indexLatestByType(readings);

    if (latest.temperature !== null) {
      if (latest.temperature >= DEFAULTS.temp_critical) {
        alerts.push({ level: 'critical', code: 'TEMP_CRITICAL', message: `Nhiệt độ nguy hiểm: ${latest.temperature}°C` });
        actions.push('Tắt thiết bị, kiểm tra ngay');
      } else if (latest.temperature >= DEFAULTS.temp_high) {
        alerts.push({ level: 'high', code: 'TEMP_HIGH', message: `Nhiệt độ cao: ${latest.temperature}°C` });
        actions.push('Bật quạt / tăng thông gió');
      } else if (latest.temperature <= DEFAULTS.temp_low) {
        alerts.push({ level: 'medium', code: 'TEMP_LOW', message: `Nhiệt độ thấp: ${latest.temperature}°C` });
        actions.push('Bật sưởi / giảm thông gió');
      }
    }

    if (latest.humidity !== null) {
      if (latest.humidity < DEFAULTS.hum_low) {
        alerts.push({ level: 'medium', code: 'HUM_LOW', message: `Độ ẩm không khí thấp: ${latest.humidity}%` });
        actions.push('Phun sương / tưới');
      } else if (latest.humidity > DEFAULTS.hum_high) {
        alerts.push({ level: 'high', code: 'HUM_HIGH', message: `Độ ẩm quá cao: ${latest.humidity}%` });
        actions.push('Tăng thông gió');
      }
    }

    if (latest.soil_moisture !== null) {
      if (latest.soil_moisture < DEFAULTS.soil_low) {
        alerts.push({ level: 'high', code: 'SOIL_DRY', message: `Đất khô: ${latest.soil_moisture}%` });
        actions.push('Kích hoạt tưới');
      } else if (latest.soil_moisture > DEFAULTS.soil_high) {
        alerts.push({ level: 'medium', code: 'SOIL_WET', message: `Đất quá ẩm: ${latest.soil_moisture}%` });
        actions.push('Tắt tưới, kiểm tra thoát nước');
      }
    }

    if (latest.light !== null) {
      if (latest.light < DEFAULTS.light_low) {
        alerts.push({ level: 'low', code: 'LOW_LIGHT', message: `Ánh sáng thấp: ${latest.light} lux` });
        info.push('Cân nhắc bổ sung đèn grow light');
      } else if (latest.light > DEFAULTS.light_high) {
        alerts.push({ level: 'medium', code: 'HIGH_LIGHT', message: `Ánh sáng cao: ${latest.light} lux` });
        info.push('Cân nhắc che lưới shading');
      }
    }

    if (latest.battery !== null) {
      if (latest.battery < DEFAULTS.battery_critical) {
        alerts.push({ level: 'critical', code: 'CRITICAL_BATTERY', message: `Pin nguy hiểm: ${latest.battery}V` });
        actions.push('Khẩn cấp: Thay pin hoặc kết nguồn');
      } else if (latest.battery < DEFAULTS.battery_low) {
        alerts.push({ level: 'high', code: 'LOW_BATTERY', message: `Pin yếu: ${latest.battery}V` });
        actions.push('Chuyển chế độ tiết kiệm');
      }
    }

    if (latest.co2 !== null && latest.co2 > DEFAULTS.co2_high) {
      alerts.push({ level: 'medium', code: 'HIGH_CO2', message: `CO2 cao: ${latest.co2} ppm` });
      actions.push('Tăng thông gió');
    }

    if (latest.ph !== null) {
      if (latest.ph < DEFAULTS.ph_low) {
        alerts.push({ level: 'high', code: 'PH_LOW', message: `pH quá axit: ${latest.ph}` });
        actions.push('Điều chỉnh pH');
      } else if (latest.ph > DEFAULTS.ph_high) {
        alerts.push({ level: 'high', code: 'PH_HIGH', message: `pH quá kiềm: ${latest.ph}` });
        actions.push('Điều chỉnh pH');
      }
    }

    const anomalyScore = computeAnomalyScore(readings, context);
    const trend = computeTrend(readings);

    return {
      ok: true,
      timestamp: new Date().toISOString(),
      anomaly_score: anomalyScore,
      trend: trend,
      alerts,
      suggested_actions: [...new Set(actions)],
      info_messages: info,
      summary: alerts.length ? `${alerts.length} cảnh báo` : 'Thông số ổn định'
    };
  }

  function indexLatestByType(readings) {
    const out = { temperature: null, humidity: null, soil_moisture: null, light: null, battery: null, co2: null, ph: null };
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
      else if (type.includes('co2')) out.co2 = val;
      else if (type.includes('ph')) out.ph = val;
    });

    return out;
  }

  function computeAnomalyScore(readings, context) {
    if (!Array.isArray(readings) || readings.length < 3) return 0;
    let score = 0;
    const values = readings.map(r => Number(r.value)).filter(v => !isNaN(v));
    if (values.length < 3) return 0;

    const mean = values.reduce((a, b) => a + b, 0) / values.length;
    const variance = values.reduce((s, v) => s + Math.pow(v - mean, 2), 0) / values.length;
    const stdev = Math.sqrt(variance);

    if (stdev > 8) score += 35;
    if (stdev > 15) score += 30;
    if (Math.abs(values[values.length - 1] - values[values.length - 2]) > 10) score += 25;
    if (context.offline_hours && context.offline_hours > 1) score += 15;

    return Math.min(score, 100);
  }

  function computeTrend(readings) {
    if (!Array.isArray(readings) || readings.length < 5) return 'stable';
    const values = readings.slice(-10).map(r => Number(r.value)).filter(v => !isNaN(v));
    if (values.length < 5) return 'stable';

    const first = values.slice(0, Math.floor(values.length / 2));
    const second = values.slice(Math.floor(values.length / 2));
    const diff = (second.reduce((a, b) => a + b, 0) / second.length) - (first.reduce((a, b) => a + b, 0) / first.length);
    const threshold = first.reduce((a, b) => a + b, 0) / first.length * 0.1;

    if (diff > threshold) return 'increasing';
    if (diff < -threshold) return 'decreasing';
    return 'stable';
  }

  return { analyzeLatestReadings, DEFAULTS };
})();

// ==================== SMART CONTROL ENGINE ====================

const SmartControlEngine = (() => {
  const rules = [];
  const ruleStates = {};
  const COOLDOWN_MS = 5000;
  const HYSTERESIS_DEFAULT = 2.0;

  function loadRules() {
    const data = getSheetData(CFG.SHEETS.RULES, false);
    if (!data || !data.rows) return;

    rules.length = 0;
    data.rows.forEach((row, i) => {
      if (row.enabled === true || row.enabled === 'TRUE' || row.enabled === 'true') {
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
    });
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

        if (triggered && !state.active && now - state.triggeredAtMs > COOLDOWN_MS) {
          results.push({
            rule: rule,
            action: rule.action,
            value: value,
            threshold: rule.threshold,
            timestamp: now
          });
          state.active = true;
          state.triggeredAtMs = now;
        } else if (!triggered && state.active && Math.abs(value - rule.threshold) > rule.hysteresis) {
          state.active = false;
        }

        state.lastValue = value;
        ruleStates[rule.id] = state;
      });
    });

    return results;
  }

  function executeAction(action, params) {
    logToSheet(CFG.SHEETS.ACTION_LOG, [
      new Date().toISOString(),
      action,
      JSON.stringify(params),
      'pending',
      params.rule?.id || ''
    ]);
  }

  return { loadRules, evaluateRules, executeAction };
})();

// ==================== TELEGRAM BOT ====================

const TelegramBot = (() => {
  const config = getConfig();
  const BOT_TOKEN = config.TELEGRAM.BOT_TOKEN;
  const API_URL = BOT_TOKEN ? `https://api.telegram.org/bot${BOT_TOKEN}` : null;

  function sendMessage(chatId, text, parseMode = 'HTML') {
    if (!API_URL) return { ok: false, error: 'Bot token not configured' };
    try {
      const response = UrlFetchApp.fetch(API_URL + '/sendMessage', {
        method: 'POST',
        contentType: 'application/json',
        payload: JSON.stringify({ chat_id: chatId, text: text, parse_mode: parseMode })
      });
      return JSON.parse(response.getContentText());
    } catch (err) {
      return { ok: false, error: err.message };
    }
  }

  function sendAlert(chatId, alert) {
    const icons = { critical: '🚨', high: '🔴', medium: '🟡', low: '🔵' };
    const text = `${icons[alert.level] || '⚠️'} <b>${alert.code}</b>\n${alert.message}`;
    return sendMessage(chatId, text);
  }

  function handleCommand(command, chatId, userId) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const commands = {
      '/start': () => sendMessage(chatId, `<b>🌱 EcoSynTech Bot V7.0</b>\n\nHệ thống IoT nông nghiệp thông minh\n\nCommands:\n/status - Trạng thái hệ thống\n/sensors - Dữ liệu cảm biến\n/alerts - Cảnh báo hiện tại\n/batches - Lô hàng đang hoạt động\n/devices - Thiết bị\n/rules - Quy tắc điều khiển\n/help - Trợ giúp`),
      
      '/status': () => {
        const devices = getSheetData(CFG.SHEETS.DEVICES);
        const metrics = getSheetData(CFG.SHEETS.DEVICE_METRICS);
        const online = devices?.rows?.filter(r => r.status === 'online').length || 0;
        const total = devices?.rows?.length || 0;
        return sendMessage(chatId, `📊 <b>System Status</b>\n\nThiết bị: ${online}/${total} online\nVersion: ${CFG.VERSION}\nBuild: ${CFG.BUILD}`);
      },
      
      '/sensors': () => {
        const sensors = getSheetData(CFG.SHEETS.BATCH_SHEET, false);
        if (!sensors?.rows?.length) return sendMessage(chatId, 'Chưa có dữ liệu cảm biến');
        const last = sensors.rows[sensors.rows.length - 1];
        return sendMessage(chatId, `📡 <b>Latest Sensors</b>\n\nLoại: ${last.sensor_type}\nGiá trị: ${last.value}\nThời gian: ${last.timestamp}`);
      },
      
      '/alerts': () => {
        const alerts = getSheetData(CFG.SHEETS.ALERTS);
        const active = alerts?.rows?.filter(r => r.status !== 'acknowledged') || [];
        if (!active.length) return sendMessage(chatId, '✅ Không có cảnh báo');
        let text = `⚠️ <b>Cảnh báo (${active.length})</b>\n\n`;
        active.slice(0, 5).forEach(r => text += `• ${r.code}: ${r.message}\n`);
        return sendMessage(chatId, text);
      },
      
      '/batches': () => {
        const batches = getSheetData(CFG.SHEETS.BATCH_INFO);
        const active = batches?.rows?.filter(r => r.status === 'active') || [];
        if (!active.length) return sendMessage(chatId, 'Không có lô hàng hoạt động');
        let text = `🌱 <b>Active Batches</b>\n\n`;
        active.slice(0, 5).forEach(r => text += `• ${r.batch_name} (${r.zone})\n`);
        return sendMessage(chatId, text);
      },
      
      '/devices': () => {
        const devices = getSheetData(CFG.SHEETS.DEVICES);
        if (!devices?.rows?.length) return sendMessage(chatId, 'Chưa có thiết bị');
        let text = `📱 <b>Thiết bị</b>\n\n`;
        devices.rows.slice(0, 5).forEach(r => text += `• ${r.device_id}: ${r.status}\n`);
        return sendMessage(chatId, text);
      },
      
      '/rules': () => {
        const rules = getSheetData(CFG.SHEETS.RULES);
        const enabled = rules?.rows?.filter(r => r.enabled === true || r.enabled === 'TRUE') || [];
        if (!enabled.length) return sendMessage(chatId, 'Không có quy tắc hoạt động');
        let text = `⚙️ <b>Active Rules</b>\n\n`;
        enabled.slice(0, 5).forEach(r => text += `• ${r.name}: ${r.sensor} ${r.operator} ${r.threshold}\n`);
        return sendMessage(chatId, text);
      },
      
      '/help': () => sendMessage(chatId, `<b>📖 Help</b>\n\n/start - Bắt đầu\n/status - Trạng thái\n/sensors - Cảm biến\n/alerts - Cảnh báo\n/batches - Lô hàng\n/devices - Thiết bị\n/rules - Quy tắc`)
    };

    const handler = commands[command];
    return handler ? handler() : sendMessage(chatId, `Unknown: ${command}`);
  }

  function broadcastAlert(alert) {
    const config = getConfig();
    if (!config.TELEGRAM.NOTIFY_ON_ALERT) return;
    config.TELEGRAM.ALLOWED_CHATS.forEach(chatId => sendAlert(chatId, alert));
  }

  return { sendMessage, sendAlert, handleCommand, broadcastAlert };
})();

// ==================== FIRMWARE COMMUNICATION ====================

const FirmwareProtocol = (() => {
  function buildCommandPayload(command, params = {}) {
    const nonce = generateId('cmd');
    return {
      _nonce: nonce,
      _ts: Math.floor(Date.now() / 1000),
      _did: CFG.FIRMWARE.PROTOCOL_VERSION,
      commands: [{
        command: command,
        command_id: nonce,
        params: params
      }]
    };
  }

  function parseDevicePayload(payload) {
    try {
      const data = JSON.parse(payload);
      return {
        device_id: data.device_id || data._did,
        timestamp: data.timestamp || data._ts,
        sensors: data.sensors || data.sensor_data || [],
        status: data.status,
        metrics: data.metrics || {}
      };
    } catch (e) {
      return null;
    }
  }

  function registerDevice(deviceId, deviceInfo) {
    logToSheet(CFG.SHEETS.DEVICES, [
      deviceId,
      deviceInfo.name || deviceId,
      'online',
      new Date().toISOString(),
      JSON.stringify(deviceInfo)
    ]);
  }

  function updateDeviceMetrics(deviceId, metrics) {
    logToSheet(CFG.SHEETS.DEVICE_METRICS, [
      deviceId,
      new Date().toISOString(),
      metrics.free_heap || 0,
      metrics.wifi_rssi || 0,
      metrics.uptime_sec || 0,
      metrics.health_score || 100,
      JSON.stringify(metrics)
    ]);
  }

  function processSensorDataFromDevice(deviceId, sensorData) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(CFG.SHEETS.BATCH_SHEET);
    if (!sheet) sheet = ss.insertSheet(CFG.SHEETS.BATCH_SHEET);

    sensorData.forEach(sensor => {
      sheet.appendRow([
        new Date().toISOString(),
        deviceId,
        sensor.type || sensor.sensor_type,
        sensor.value,
        sensor.zone || 'default',
        sensor.batch_id || 'default'
      ]);
    });

    clearSheetCache(CFG.SHEETS.BATCH_SHEET);

    if (sensorData.length > 0) {
      const readings = sensorData.map(s => ({ sensor_type: s.type || s.sensor_type, value: s.value, zone: s.zone }));
      const analysis = AdvisoryEngine.analyzeLatestReadings(readings);
      
      if (analysis.alerts.length > 0) {
        logToSheet(CFG.SHEETS.ALERTS, [
          new Date().toISOString(),
          deviceId,
          analysis.alerts[0].code,
          analysis.alerts[0].message,
          analysis.alerts[0].level,
          'pending',
          JSON.stringify(analysis)
        ]);
        
        TelegramBot.broadcastAlert(analysis.alerts[0]);
      }
    }

    return { success: true, processed: sensorData.length };
  }

  return { buildCommandPayload, parseDevicePayload, registerDevice, updateDeviceMetrics, processSensorDataFromDevice };
})();

// ==================== WEBHOOK HANDLERS ====================

function doGet() {
  const config = getConfig();
  return HtmlService.createHtmlOutput(
    `<!DOCTYPE html>
    <html>
    <head><meta charset="UTF-8"><title>EcoSynTech GAS V7.0</title></head>
    <body style="font-family: sans-serif; padding: 40px; text-align: center;">
      <h1>🌱 EcoSynTech GAS V7.0</h1>
      <p>Version: ${config.VERSION}</p>
      <p>Build: ${config.BUILD}</p>
      <hr>
      <p><a href="/qr?qr=DEMO">QR Demo</a></p>
    </body>
    </html>`
  ).setTitle('EcoSynTech V7.0');
}

function doPost(e) {
  const postData = e.postData;
  
  if (postData && postData.contents) {
    try {
      const data = JSON.parse(postData.contents);
      
      if (data.sensor_data || data.sensors) {
        const sensors = data.sensor_data || data.sensors;
        const deviceId = data.device_id || data._did;
        FirmwareProtocol.processSensorDataFromDevice(deviceId, sensors);
      }
      
      if (data.device_metrics) {
        const deviceId = data.device_id || data._did;
        FirmwareProtocol.updateDeviceMetrics(deviceId, data.device_metrics);
      }
      
      if (data.register) {
        FirmwareProtocol.registerDevice(data.device_id, data);
      }
      
      if (data.readings && data.commands) {
        SmartControlEngine.loadRules();
        const results = SmartControlEngine.evaluateRules(data.readings);
        results.forEach(r => SmartControlEngine.executeAction(r.action, r));
      }
      
      if (data.callback_query) {
        return telegramWebhook(e);
      }
      
    } catch (err) {
      return ContentService.createTextOutput(JSON.stringify({ error: err.message })).setMimeType(ContentService.MimeType.JSON);
    }
  }

  return ContentService.createTextOutput(JSON.stringify({ 
    success: true, 
    version: CFG.VERSION,
    build: CFG.BUILD,
    protocol: CFG.FIRMWARE.PROTOCOL_VERSION
  })).setMimeType(ContentService.MimeType.JSON);
}

function telegramWebhook(e) {
  try {
    const update = JSON.parse(e.postData.contents);
    if (update.message) {
      const chatId = update.message.chat.id;
      const text = update.message.text;
      TelegramBot.handleCommand(text, chatId, update.message.from.id);
      logToSheet(CFG.SHEETS.TELEGRAM_LOG, [new Date().toISOString(), chatId, text, 'OK']);
    }
    return ContentService.createTextOutput('OK');
  } catch (err) {
    return ContentService.createTextOutput('Error: ' + err.message);
  }
}

// ==================== API ENDPOINTS ====================

function getApiInfo() {
  return {
    version: CFG.VERSION,
    build: CFG.BUILD,
    protocol: CFG.FIRMWARE.PROTOCOL_VERSION,
    endpoints: {
      qr: '/qr?qr=QR_ID',
      webhook: '/',
      telegram: '/telegram',
      sensor: 'POST sensor data',
      command: 'POST commands'
    }
  };
}

function getSystemStatus() {
  const devices = getSheetData(CFG.SHEETS.DEVICES);
  const batches = getSheetData(CFG.SHEETS.BATCH_INFO);
  const alerts = getSheetData(CFG.SHEETS.ALERTS);
  
  return {
    version: CFG.VERSION,
    timestamp: new Date().toISOString(),
    devices: {
      total: devices?.rows?.length || 0,
      online: devices?.rows?.filter(r => r.status === 'online').length || 0
    },
    batches: {
      total: batches?.rows?.length || 0,
      active: batches?.rows?.filter(r => r.status === 'active').length || 0
    },
    alerts: {
      total: alerts?.rows?.length || 0,
      active: alerts?.rows?.filter(r => r.status !== 'acknowledged').length || 0
    }
  };
}

function getBatchTraceability(batchId) {
  const batchInfo = getSheetData(CFG.SHEETS.BATCH_INFO, false);
  const batch = batchInfo?.rows?.find(r => String(r.batch_id).trim() === String(batchId).trim());
  
  if (!batch) return { error: 'Batch not found' };
  
  const sensors = getRowsByBatch(CFG.SHEETS.BATCH_SHEET, batchId);
  const events = getRowsByBatch(CFG.SHEETS.MANUAL_EVENTS, batchId);
  const media = getRowsByBatch(CFG.SHEETS.BATCH_MEDIA, batchId);
  
  return {
    batch: batch,
    sensors: sensors,
    events: events,
    media: media,
    sensor_summary: buildSensorChartData(sensors)
  };
}

// ==================== ADMIN FUNCTIONS ====================

function createDefaultSheets() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = Object.values(CFG.SHEETS);
  
  sheets.forEach(name => {
    if (!ss.getSheetByName(name)) {
      ss.insertSheet(name);
    }
  });
  
  return { success: true, sheets: sheets };
}

function runDiagnostics() {
  const issues = [];
  
  const devices = getSheetData(CFG.SHEETS.DEVICES);
  if (!devices?.rows?.length) issues.push('No devices registered');
  
  const batches = getSheetData(CFG.SHEETS.BATCH_INFO);
  if (!batches?.rows?.length) issues.push('No batches found');
  
  const config = getConfig();
  if (!config.TELEGRAM.BOT_TOKEN || config.TELEGRAM.BOT_TOKEN === 'YOUR_TELEGRAM_BOT_TOKEN') {
    issues.push('Telegram bot not configured');
  }
  
  return {
    version: CFG.VERSION,
    issues: issues,
    timestamp: new Date().toISOString()
  };
}
