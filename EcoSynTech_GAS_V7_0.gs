/**
 * EcoSynTech GAS V7.0 - Telegram Bot + Advanced AI + Full Integration
 * Version: 7.0.0
 * Features:
 * - Telegram Bot Integration
 * - Advanced Advisory Engine (AI nhẹ)
 * - Real-time notifications
 * - Command handling via Telegram
 * - Multi-device support
 */

const CFG = {
  VERSION: '7.0.0',
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
    TELEGRAM_LOG: 'TelegramLog'
  },
  TELEGRAM: {
    BOT_TOKEN: 'YOUR_TELEGRAM_BOT_TOKEN',
    ALLOWED_CHATS: []
  },
  ADVISORY_DEFAULTS: {
    temp_high: 38.0,
    temp_low: 16.0,
    hum_low: 45.0,
    hum_high: 90.0,
    soil_low: 30.0,
    soil_high: 80.0,
    light_low: 5000,
    light_high: 80000,
    battery_low: 3.5,
    battery_critical: 3.2
  }
};

const TelegramBot = (() => {
  const BOT_TOKEN = PropertiesService.getScriptProperties().getProperty('TELEGRAM_BOT_TOKEN') || CFG.TELEGRAM.BOT_TOKEN;
  const API_URL = `https://api.telegram.org/bot${BOT_TOKEN}`;

  function sendMessage(chatId, text, parseMode = 'HTML') {
    if (!BOT_TOKEN || BOT_TOKEN === 'YOUR_TELEGRAM_BOT_TOKEN') {
      console.log('[TELEGRAM] Bot token not configured');
      return { ok: false, error: 'Bot token not configured' };
    }

    try {
      const url = `${API_URL}/sendMessage`;
      const payload = {
        chat_id: chatId,
        text: text,
        parse_mode: parseMode
      };

      const response = UrlFetchApp.fetch(url, {
        method: 'POST',
        contentType: 'application/json',
        payload: JSON.stringify(payload)
      });

      return JSON.parse(response.getContentText());
    } catch (err) {
      console.error('[TELEGRAM] Error sending message:', err);
      return { ok: false, error: err.message };
    }
  }

  function sendPhoto(chatId, photoUrl, caption) {
    if (!BOT_TOKEN || BOT_TOKEN === 'YOUR_TELEGRAM_BOT_TOKEN') {
      return { ok: false, error: 'Bot token not configured' };
    }

    try {
      const url = `${API_URL}/sendPhoto`;
      const payload = {
        chat_id: chatId,
        photo: photoUrl,
        caption: caption
      };

      const response = UrlFetchApp.fetch(url, {
        method: 'POST',
        contentType: 'application/json',
        payload: JSON.stringify(payload)
      });

      return JSON.parse(response.getContentText());
    } catch (err) {
      console.error('[TELEGRAM] Error sending photo:', err);
      return { ok: false, error: err.message };
    }
  }

  function sendAlert(chatId, alert) {
    const icon = alert.level === 'high' ? '🔴' : alert.level === 'medium' ? '🟡' : '🔵';
    const text = `${icon} <b>${alert.code}</b>\n${alert.message}`;
    return sendMessage(chatId, text);
  }

  function handleCommand(command, chatId, userId) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    switch (command) {
      case '/start':
        return sendMessage(chatId, 
          `<b>Welcome to EcoSynTech Bot!</b>\n\n` +
          `Commands:\n` +
          `/status - System status\n` +
          `/sensors - Latest sensor data\n` +
          `/alerts - Active alerts\n` +
          `/batches - Active batches\n` +
          `/help - Show this help`
        );

      case '/status':
        const deviceSheet = ss.getSheetByName(CFG.SHEETS.DEVICES);
        if (deviceSheet) {
          const data = deviceSheet.getDataRange().getValues();
          const online = data.slice(1).filter(r => r[2] === 'online').length;
          return sendMessage(chatId, `📊 <b>System Status</b>\n\nOnline devices: ${online}/${data.length - 1}`);
        }
        return sendMessage(chatId, 'No devices configured');

      case '/sensors':
        const sensorSheet = ss.getSheetByName(CFG.SHEETS.BATCH_SHEET);
        if (sensorSheet) {
          const data = sensorSheet.getDataRange().getValues();
          const lastRow = data[data.length - 1];
          return sendMessage(chatId, 
            `📡 <b>Latest Sensors</b>\n\n` +
            `Type: ${lastRow[2]}\n` +
            `Value: ${lastRow[3]}`
          );
        }
        return sendMessage(chatId, 'No sensor data');

      case '/alerts':
        const alertSheet = ss.getSheetByName(CFG.SHEETS.ALERTS);
        if (alertSheet) {
          const data = alertSheet.getDataRange().getValues();
          const active = data.slice(1).filter(r => r[3] !== 'acknowledged');
          if (active.length === 0) return sendMessage(chatId, '✅ No active alerts');
          
          let text = `⚠️ <b>Active Alerts (${active.length})</b>\n\n`;
          active.slice(0, 5).forEach(r => {
            text += `${r[1]}: ${r[2]}\n`;
          });
          return sendMessage(chatId, text);
        }
        return sendMessage(chatId, 'No alerts');

      case '/batches':
        const batchSheet = ss.getSheetByName(CFG.SHEETS.BATCH_INFO);
        if (batchSheet) {
          const data = batchSheet.getDataRange().getValues();
          const active = data.slice(1).filter(r => r[4] === 'active');
          if (active.length === 0) return sendMessage(chatId, 'No active batches');
          
          let text = `🌱 <b>Active Batches</b>\n\n`;
          active.slice(0, 5).forEach(r => {
            text += `${r[0]}: ${r[1]} (${r[5]})\n`;
          });
          return sendMessage(chatId, text);
        }
        return sendMessage(chatId, 'No batches');

      case '/help':
        return sendMessage(chatId,
          `<b>EcoSynTech Bot Commands</b>\n\n` +
          `/start - Start bot\n` +
          `/status - System status\n` +
          `/sensors - Latest readings\n` +
          `/alerts - Active alerts\n` +
          `/batches - Active batches\n` +
          `/help - Show this help`
        );

      default:
        return sendMessage(chatId, `Unknown command: ${command}\nUse /help for available commands`);
    }
  }

  function logTelegramEvent(chatId, command, response) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let sheet = ss.getSheetByName(CFG.SHEETS.TELEGRAM_LOG);
    if (!sheet) {
      sheet = ss.insertSheet(CFG.SHEETS.TELEGRAM_LOG);
      sheet.appendRow(['timestamp', 'chat_id', 'command', 'response']);
    }
    sheet.appendRow([new Date().toISOString(), chatId, command, JSON.stringify(response)]);
  }

  return { sendMessage, sendPhoto, sendAlert, handleCommand, logTelegramEvent };
})();

const AdvisoryEngineV7 = (() => {
  const DEFAULTS = CFG.ADVISORY_DEFAULTS;
  const ALERT_HISTORY = [];

  function analyzeLatestReadings(readings, context) {
    context = context || {};
    const alerts = [];
    const actions = [];
    const info = [];
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
        actions.push('Bật sưởi / giảm thông gió');
      }
    }

    if (latest.humidity !== null) {
      if (latest.humidity < DEFAULTS.hum_low) {
        alerts.push({
          level: 'medium',
          code: 'HUM_LOW',
          message: `Độ ẩm không khí thấp: ${latest.humidity}%`
        });
        actions.push('Theo dõi bốc hơi, cân nhắc tưới/phun sương');
      }
      if (latest.humidity > DEFAULTS.hum_high) {
        alerts.push({
          level: 'high',
          code: 'HUM_HIGH',
          message: `Độ ẩm quá cao: ${latest.humidity}%`
        });
        actions.push('Tăng thông gió, kiểm soát bệnh nấm');
      }
    }

    if (latest.soil_moisture !== null) {
      if (latest.soil_moisture < DEFAULTS.soil_low) {
        alerts.push({
          level: 'high',
          code: 'SOIL_DRY',
          message: `Đất khô: ${latest.soil_moisture}%`
        });
        actions.push('Kích hoạt tưới theo zone');
      }
      if (latest.soil_moisture > DEFAULTS.soil_high) {
        alerts.push({
          level: 'medium',
          code: 'SOIL_WET',
          message: `Đất quá ẩm: ${latest.soil_moisture}%`
        });
        actions.push('Tắt tưới, kiểm tra thoát nước');
      }
    }

    if (latest.light !== null) {
      if (latest.light < DEFAULTS.light_low) {
        alerts.push({
          level: 'low',
          code: 'LOW_LIGHT',
          message: `Ánh sáng thấp: ${latest.light} lux`
        });
        info.push('Cân nhắc bổ sung đèn grow light');
      }
      if (latest.light > DEFAULTS.light_high) {
        alerts.push({
          level: 'medium',
          code: 'HIGH_LIGHT',
          message: `Ánh sáng quá cao: ${latest.light} lux`
        });
        info.push('Cân nhắc che lưới shading');
      }
    }

    if (latest.battery !== null) {
      if (latest.battery < DEFAULTS.battery_low) {
        alerts.push({
          level: 'high',
          code: 'LOW_BATTERY',
          message: `Pin yếu: ${latest.battery}V`
        });
        actions.push('Chuyển sang chế độ tiết kiệm năng lượng');
      }
      if (latest.battery < DEFAULTS.battery_critical) {
        alerts.push({
          level: 'critical',
          code: 'CRITICAL_BATTERY',
          message: `Pin nguy hiểm: ${latest.battery}V`
        });
        actions.push('Khẩn cấp: Thay pin hoặc kết nguồn điện');
      }
    }

    const anomalyScore = computeSimpleAnomalyScore_(readings, context);
    const trend = computeTrend_(readings);

    return {
      ok: true,
      anomaly_score: anomalyScore,
      trend: trend,
      alerts,
      suggested_actions: unique_(actions),
      info_messages: info,
      summary: alerts.length
        ? `${alerts.length} cảnh báo, cần theo dõi`
        : 'Thông số ổn định'
    };
  }

  function indexLatestByType_(readings) {
    const out = { 
      temperature: null, 
      humidity: null, 
      soil_moisture: null, 
      light: null, 
      battery: null,
      co2: null,
      ph: null
    };
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

  function computeTrend_(readings) {
    if (!Array.isArray(readings) || readings.length < 5) return 'stable';
    
    const values = readings.slice(-10).map(r => Number(r.value)).filter(v => !isNaN(v));
    if (values.length < 5) return 'stable';

    const first = values.slice(0, Math.floor(values.length / 2));
    const second = values.slice(Math.floor(values.length / 2));
    
    const firstAvg = first.reduce((a, b) => a + b, 0) / first.length;
    const secondAvg = second.reduce((a, b) => a + b, 0) / second.length;
    
    const diff = secondAvg - firstAvg;
    const threshold = firstAvg * 0.1;

    if (diff > threshold) return 'increasing';
    if (diff < -threshold) return 'decreasing';
    return 'stable';
  }

  function unique_(arr) {
    return [...new Set(arr)];
  }

  function getHistoricalAlerts() {
    return ALERT_HISTORY.slice(-50);
  }

  function clearAlertHistory() {
    ALERT_HISTORY.length = 0;
  }

  return { 
    analyzeLatestReadings, 
    getHistoricalAlerts, 
    clearAlertHistory,
    DEFAULTS: DEFAULTS
  };
})();

function telegramWebhook(e) {
  try {
    const update = JSON.parse(e.postData.contents);
    
    if (update.message) {
      const chatId = update.message.chat.id;
      const userId = update.message.from.id;
      const text = update.message.text;
      
      console.log(`[TELEGRAM] Message from ${chatId}: ${text}`);
      
      let command = text;
      const response = TelegramBot.handleCommand(command, chatId, userId);
      TelegramBot.logTelegramEvent(chatId, command, response);
    }
    
    return ContentService.createTextOutput('OK');
  } catch (err) {
    console.error('[TELEGRAM] Webhook error:', err);
    return ContentService.createTextOutput('Error: ' + err.message);
  }
}

function _renderQrHtmlV7(params) {
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
        try { qrMeta = JSON.parse(dynamicRows[i][metaCol]); } catch (e) {}
      }
      break;
    }
  }

  if (!batchId) {
    return HtmlService.createHtmlOutput(`<h3>Không tìm thấy batch</h3><p>QR này chưa liên kết với batch nào.</p>`);
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
    return HtmlService.createHtmlOutput(`<h3>Không tìm thấy thông tin batch</h3><p>Batch ID: ${batchId}</p>`);
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
    .card-soft { border: 0; border-radius: 18px; box-shadow: 0 8px 24px rgba(0,0,0,.06); }
    .badge-soft { background: rgba(255,255,255,.18); border: 1px solid rgba(255,255,255,.25); color: #fff; font-weight: 600; }
    .meta-label { color: #667085; font-size: .88rem; }
    .meta-value { font-weight: 700; color: #101828; }
    .timeline-item { border-left: 2px solid #d0d5dd; padding-left: 14px; margin-left: 8px; margin-bottom: 14px; position: relative; }
    .timeline-item::before { content: ''; width: 10px; height: 10px; background: #1ea65a; border-radius: 50%; position: absolute; left: -6px; top: 6px; }
    .media-thumb { width: 100%; height: 170px; object-fit: cover; border-radius: 14px; background: #e9ecef; }
    .advisory-box { background: linear-gradient(135deg, #fff3cd, #ffeaa7); border-left: 4px solid #ffc107; }
    .alert-critical { background: #ffebee; border-left: 4px solid #f44336; }
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
                </div>
                <div class="mt-3">
                  <button class="btn btn-outline-success btn-sm no-print" onclick="window.print()">In / Lưu PDF</button>
                </div>
              </div>
            </div>
          </div>
        </div>
        <div class="tab-pane fade" id="sensor"><canvas id="sensorChart" height="110"></canvas></div>
        <div class="tab-pane fade" id="timeline">${eventTimeline || '<div class="text-secondary">Chưa có nhật ký canh tác.</div>'}</div>
        <div class="tab-pane fade" id="media">${mediaHtml || '<div class="text-secondary">Chưa có hình ảnh / video.</div>'}</div>
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
  new Chart(ctx, { type: 'line', data: { labels: sensorData.labels, datasets: sensorData.datasets }, options: { responsive: true, plugins: { legend: { display: true } }, scales: { y: { beginAtZero: false } } } });
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
  if (sCol >= 0) rows.sort((a, b) => String(a[sortField.toLowerCase()] || '').localeCompare(String(b[sortField.toLowerCase()] || '')));
  return rows;
}

function buildSensorChartData_(rows) {
  const labels = [], temp = [], hum = [], soil = [];
  rows.forEach(r => {
    const ts = r.event_ts || r.timestamp || r.received_at || '';
    const type = String(r.sensor_type || '').toLowerCase();
    const val = Number(r.value);
    if (!ts || isNaN(val)) return;
    if (type.includes('temp') || type === 'temperature') { labels.push(ts); temp.push(val); }
    else if (type.includes('hum') || type === 'humidity') hum.push(val);
    else if (type.includes('soil') || type.includes('moisture')) soil.push(val);
  });
  const datasets = [];
  if (temp.length) datasets.push({ label: 'Nhiệt độ', data: temp, tension: 0.35 });
  if (hum.length) datasets.push({ label: 'Độ ẩm', data: hum, tension: 0.35 });
  if (soil.length) datasets.push({ label: 'Độ ẩm đất', data: soil, tension: 0.35 });
  return { labels: labels.length ? labels : rows.map((_, i) => `#${i+1}`), datasets };
}

function buildEventTimeline_(rows) {
  if (!rows.length) return '';
  return rows.map(r => {
    const ts = esc_(r.timestamp || r.event_ts || '');
    const type = esc_(r.event_type || r.type || '');
    const op = esc_(r.operator || r.created_by || '');
    const note = esc_(r.notes || r.note || '');
    return `<div class="timeline-item"><div class="fw-bold">${type}</div><div class="small text-secondary">${ts} • ${op}</div><div class="text-muted small">${note}</div></div>`;
  }).join('');
}

function buildMediaHtml_(rows) {
  if (!rows.length) return '';
  return rows.map(r => {
    const url = String(r.url || r.file_url || r.media_url || '').trim();
    const caption = esc_(r.caption || r.note || r.media_type || '');
    if (!url) return '';
    const isImage = /\.(jpg|jpeg|png|webp|gif)$/i.test(url) || url.includes('drive.google.com');
    return `<div class="col-md-4 mb-3"><div class="card card-soft h-100">${isImage ? `<img src="${esc_(url)}" class="media-thumb" alt="${caption}">` : `<div class="p-4 text-center bg-light rounded-4">VIDEO / FILE</div>`}<div class="card-body"><div class="small text-secondary">${caption || 'Media'}</div><a href="${esc_(url)}" target="_blank">Mở tệp</a></div></div></div>`;
  }).join('');
}

function esc_(s) {
  s = String(s === undefined || s === null ? '' : s);
  return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#039;');
}

function processLatestFarmDataV7(batchId, readings, notifyTelegram = false) {
  const result = AdvisoryEngineV7.analyzeLatestReadings(readings, { batchId: batchId });

  if (result.alerts.length) {
    EventModule.logEvent(batchId, 'ai_alert', result, 'advisory_engine_v7');
    
    if (notifyTelegram) {
      const chatIds = CFG.TELEGRAM.ALLOWED_CHATS;
      chatIds.forEach(chatId => {
        result.alerts.forEach(alert => {
          TelegramBot.sendAlert(chatId, alert);
        });
      });
    }
  }

  return result;
}

function doGet() {
  return HtmlService.createTemplateFromFile('index')
    .evaluate()
    .setTitle('EcoSynTech GAS V7.0')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

function doPost(e) {
  if (e.postData && e.postData.contents && e.postData.contents.includes('"callback_query"')) {
    return telegramWebhook(e);
  }
  return handleWebhook(e);
}

function handleWebhook(e) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
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

      if (data.commands && data.readings) {
        const SmartControlEngine = getSmartControlEngine();
        const results = SmartControlEngine.evaluateRules(data.readings);
        results.forEach(r => SmartControlEngine.executeAction(r.action, r));
      }
    } catch (err) {
      return ContentService.createTextOutput(JSON.stringify({ error: err.message }))
        .setMimeType(ContentService.MimeType.JSON);
    }
  }

  return ContentService.createTextOutput(JSON.stringify({ success: true, version: CFG.VERSION }))
    .setMimeType(ContentService.MimeType.JSON);
}

function getSmartControlEngine() {
  const CFG_RULES = CFG.SHEETS;
  const rules = [];
  const ruleStates = {};
  const COOLDOWN_MS = 5000;
  const HYSTERESIS_DEFAULT = 2.0;

  function loadRules() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(CFG_RULES.RULES);
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
            results.push({ rule: rule, action: rule.action, value: value, threshold: rule.threshold, timestamp: now });
            state.active = true;
            state.triggeredAtMs = now;
          }
        } else if (!triggered && state.active) {
          if (Math.abs(value - rule.threshold) > rule.hysteresis) state.active = false;
        }
        state.lastValue = value;
        ruleStates[rule.id] = state;
      });
    });
    return results;
  }

  function executeAction(action, params) {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let logSheet = ss.getSheetByName('ActionLog');
    if (!logSheet) logSheet = ss.insertSheet('ActionLog');
    logSheet.appendRow([new Date().toISOString(), action, JSON.stringify(params), 'pending']);
  }

  return { loadRules, evaluateRules, executeAction };
}

function getApiEndpoints() {
  return {
    version: CFG.VERSION,
    telegram_bot: CFG.TELEGRAM.BOT_TOKEN !== 'YOUR_TELEGRAM_BOT_TOKEN',
    endpoints: {
      qr: '/qr?qr=QR_ID',
      webhook: '/',
      sensor: '/sensor',
      command: '/command',
      telegram: '/telegram'
    }
  };
}
