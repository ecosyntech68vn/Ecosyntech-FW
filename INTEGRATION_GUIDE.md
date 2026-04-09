# EcoSynTech GAS V7.0 ULTIMATE - Integration Guide

## Tổng quan kiến trúc hệ thống

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        ECO SYNTECH V7.0 SYSTEM                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌──────────────┐     HTTPS/WebSocket      ┌───────────────────────┐  │
│  │   ESP32      │ ◄────────────────────────► │   Google Apps Script  │  │
│  │  Firmware    │                           │   (GAS V7.0)          │  │
│  │  V8.5.0      │                           │                       │  │
│  │              │ ◄──── MQTT (optional) ───► │  ┌───────────────┐   │  │
│  │ Sensors      │                           │  │ QR Renderer   │   │  │
│  │ Relays       │                           │  │ Advisory AI   │   │  │
│  │ Control      │                           │  │ Smart Control │   │  │
│  └──────────────┘                           │  │ Telegram Bot  │   │  │
│                                             │  └───────────────┘   │  │
│                                             └───────────────────────┘  │
│                                                     │                  │
│                                              ┌──────▼──────┐          │
│                                              │ Google      │          │
│                                              │ Sheets      │          │
│                                              └─────────────┘          │
└─────────────────────────────────────────────────────────────────────────┘
```

## Giao thức giao tiếp Firmware ↔ GAS

### 1. Gửi dữ liệu cảm biến (Firmware → GAS)

**Endpoint:** `POST https://script.google.com/macros/s/{SCRIPT_ID}/exec`

**Payload từ Firmware:**
```json
{
  "sensor_data": [
    { "type": "temperature", "value": 28.5, "zone": "zone1" },
    { "type": "humidity", "value": 72, "zone": "zone1" },
    { "type": "soil_moisture", "value": 45, "zone": "zone1" }
  ],
  "device_id": "ECOSYNTECH0001",
  "batch_id": "BATCH001",
  "timestamp": "2026-04-09T10:30:00Z",
  "_nonce": "abc123...",
  "_ts": 1715333400,
  "_did": "ECOSYNTECH0001"
}
```

**Xử lý tại GAS:**
- Lưu vào sheet `Batchsheet`
- Chạy Advisory Engine để phân tích
- Gửi Telegram alert nếu có cảnh báo
- Ghi log vào sheet `Alerts`

### 2. Nhận lệnh điều khiển (GAS → Firmware)

**GAS gửi command qua:**
- **MQTT:** Topic `ecosyntech/{device_id}/command`
- **HTTP:** Query `get_pending_commands`

**Command Payload:**
```json
{
  "payload": {
    "commands": [
      {
        "command": "relay1_on",
        "command_id": "cmd_123456",
        "params": { "duration": 300 }
      }
    ],
    "_nonce": "nonce123",
    "_ts": 1715333400,
    "_did": "ECOSYNTECH0001"
  },
  "signature": "hmac_sha256_signature"
}
```

### 3. Cập nhật cấu hình (GAS → Firmware)

**Config Payload:**
```json
{
  "payload": {
    "config": {
      "post_interval_sec": 1800,
      "sensor_interval_sec": 30,
      "deep_sleep_enabled": true
    },
    "config_version": 5,
    "_nonce": "nonce456",
    "_ts": 1715333400,
    "_did": "ECOSYNTECH0001"
  },
  "signature": "hmac_sha256_signature"
}
```

## Data Flow Diagram

```
FIRMWARE (ESP32)                    GAS V7.0                       GOOGLE SHEETS
     │                                  │                                 │
     │──── sensor_data + signature ────►│                                 │
     │                                  │──── Validate signature ───────►│
     │                                  │                                 │
     │                                  │──── Write to Batchsheet ─────►│
     │                                  │                                 │
     │                                  │──── Advisory Engine ──────────►│
     │                                  │                                 │
     │◄──── Response + commands ◄───────│                                 │
     │                                  │                                 │
     │──── Execute commands ───────────►│                                 │
     │    (relay, reboot, etc.)          │                                 │
     │                                  │                                 │
     │                                  │──── Log to ActionLog ─────────►│
     │                                  │                                 │
     │                                  │──── Alert to Telegram ───────►│
```

## API Endpoints

| Method | Endpoint | Mô tả |
|--------|----------|-------|
| POST | `/exec` | Webhook chính, nhận sensor data, commands |
| GET | `/exec?qr={id}` | Render QR traceability page |
| POST | `/exec` (telegram) | Telegram webhook handler |

## Sheet Structure

### BatchInfo
| batch_id | batch_name | crop_type | zone | start_date | end_date | status | blockchain_tx |
|----------|------------|-----------|------|------------|----------|--------|---------------|
| BATCH001 | Rau cải xanh | Cải xanh | Zone1 | 2026-01-01 | 2026-03-31 | active | 0x123... |

### Batchsheet
| timestamp | device_id | sensor_type | value | zone | batch_id |
|-----------|-----------|-------------|-------|------|----------|
| 2026-04-09T10:00:00Z | ECOSYNTECH0001 | temperature | 28.5 | zone1 | BATCH001 |

### Devices
| device_id | name | status | last_seen | metadata |
|-----------|------|--------|-----------|-----------|
| ECOSYNTECH0001 | Thiết bị 1 | online | 2026-04-09T10:00:00Z | {...} |

### Alerts
| timestamp | device_id | code | message | level | status | details |
|-----------|-----------|------|---------|-------|--------|---------|

### TelegramLog
| timestamp | chat_id | command | response |
|-----------|---------|---------|----------|

### ActionLog
| timestamp | action | params | status | rule_id |
|-----------|--------|---------|--------|----------|

## Telegram Bot Commands

| Command | Mô tả |
|---------|-------|
| /start | Khởi động bot |
| /status | Xem trạng thái hệ thống |
| /sensors | Dữ liệu cảm biến mới nhất |
| /alerts | Các cảnh báo đang hoạt động |
| /batches | Danh sách lô hàng active |
| /devices | Danh sách thiết bị |
| /rules | Quy tắc điều khiển đang hoạt động |
| /help | Trợ giúp |

## Testing Checklist

### Test 1: Gửi dữ liệu cảm biến
```bash
curl -X POST "https://script.google.com/macros/s/{SCRIPT_ID}/exec" \
  -H "Content-Type: application/json" \
  -d '{
    "sensor_data": [
      {"type": "temperature", "value": 28.5, "zone": "zone1"},
      {"type": "humidity", "value": 72, "zone": "zone1"}
    ],
    "device_id": "ECOSYNTECH0001",
    "batch_id": "TEST001"
  }'
```
**Expected:** `{ success: true, processed: 2 }`

### Test 2: QR Code Rendering
```
https://script.google.com/macros/s/{SCRIPT_ID}/exec?qr=QR001
```
**Expected:** HTML page với batch info, sensor chart, timeline

### Test 3: Advisory Engine
```javascript
const readings = [
  { sensor_type: 'temperature', value: 42 },
  { sensor_type: 'humidity', value: 30 },
  { sensor_type: 'soil_moisture', value: 25 }
];
const result = AdvisoryEngine.analyzeLatestReadings(readings);
console.log(result.alerts); // Should show TEMP_HIGH, HUM_LOW, SOIL_DRY
```

### Test 4: Smart Control Rules
```javascript
SmartControlEngine.loadRules();
const readings = [{ sensor_type: 'temperature', value: 40, zone: 'zone1' }];
const actions = SmartControlEngine.evaluateRules(readings);
console.log(actions); // Should show triggered actions
```

### Test 5: Telegram Bot
Gửi tin nhắn `/start` đến bot
**Expected:** Welcome message với command list

### Test 6: Firmware Integration
1. Nạp firmware V8.5.0 vào ESP32
2. Cấu hình webhook URL trong firmware
3. Kiểm tra serial log
4. Verify data appears in sheets

## Cấu hình Telegram Bot

1. Tạo bot qua @BotFather
2. Lấy Bot Token
3. Đặt trong Script Properties:
   - Key: `TELEGRAM_BOT_TOKEN`
   - Value: `{your_bot_token}`
4. Set webhook:
```
https://api.telegram.org/bot{TOKEN}/setWebhook?url=https://script.google.com/macros/s/{SCRIPT_ID}/exec
```
5. Thêm chat IDs vào `TELEGRAM_ALLOWED_CHATS`

## Security

- **HMAC-SHA256** signature cho tất cả payloads
- **Nonce** chống replay attack
- **Timestamp validation** với 20 phút window
- **API Key** xác thực thiết bị

## Version Information

- **GAS V7.0 Ultimate**: Version 7.0.0, Build 2026.04.09
- **Firmware**: Version 8.5.0
- **Protocol**: 1.0
