# EcoSynTech Firmware V8.5.0 - Và GAS V6.5 / V7.0

## Tổng quan các tính năng đã nâng cấp/bổ sung

### 1. Firmware ESP32 (V8.5.0)

**Đã có trong bản cũ:**
- Watchdog 30 giây, tự động reset khi treo
- Giám sát WiFi/MQTT với reconnect + backoff
- Deep sleep thông minh
- Lưu backlog khi offline
- OTA update
- Bảo mật: HMAC, nonce, replay protection
- MQTT với TLS

**Đã bổ sung:**
- **Device Health Score** - Tính toán điểm sức khỏe thiết bị (0-100)
- **Auto-recovery** - Tự động restart khi:
  - Health score < 30
  - WiFi fail >= 10 lần liên tiếp
  - MQTT fail >= 10 lần liên tiếp
  - Heap < 30KB
- **Logging** - Ghi log sự kiện auto_recover

### 2. GAS V6.5 (Smart Control + QR Traceability)

**Tính năng:**
- **Smart Control Engine** với rule CRUD, hysteresis, cooldown
- **QR Code Rendering** hoàn chỉnh cho truy xuất nguồn gốc
  - Đọc từ DynamicQR → BatchInfo → Batchsheet/ManualEvents/BatchMedia
  - Hiển thị HTML với tabs: Tổng quan, Dữ liệu cảm biến, Timeline, Media, Xác thực
  - Biểu đồ Chart.js
  - Blockchain hash verification
- **Advisory Engine (AI nhẹ)**
  - Cảnh báo nhiệt độ cao/thấp
  - Cảnh báo độ ẩm không khí/đất
  - Cảnh báo ánh sáng, pin yếu
  - Tính anomaly score
  - Gợi ý hành động

### 3. GAS V7.0 (Telegram Bot + Advanced AI)

**Bổ sung thêm V6.5:**
- **Telegram Bot Integration**
  - Commands: /start, /status, /sensors, /alerts, /batches, /help
  - Gửi alert qua Telegram
  - Log Telegram events
- **Enhanced Advisory Engine**
  - Thêm cảnh báo độ ẩm cao, đất quá ẩm, ánh sáng cao
  - Thêm critical battery alert
  - Tính trend (tăng/giảm/ổn định)
  - Thêm info messages
- **Webhook handling** với sensor data + command execution

## Cách sử dụng

### Firmware ESP32

Nap firmware V8.5.0 vào ESP32 theo hướng dẫn trong file:
- `HƯỚNG DẪN NẠP CODE FIRMWARE V8.5.0 CHO THIẾT BỊ IOT EPS32.docx`

### GAS V6.5

1. Mở Google Apps Script project của bạn
2. Copy nội dung file `EcoSynTech_GAS_V6_5.gs` vào Code.gs
3. Đảm bảo các sheet tồn tại:
   - BatchInfo
   - DynamicQR  
   - ManualEvents
   - Batchsheet
   - BatchMedia
   - Rules
4. Deploy web app

### GAS V7.0

1. Mở Google Apps Script project của bạn
2. Copy nội dung file `EcoSynTech_GAS_V7_0.gs` vào Code.gs
3. Cấu hình Telegram Bot:
   - Lấy Bot Token từ @BotFather
   - Đặt token trong PropertiesService hoặc biến CFG
   - Thêm chat IDs vào CFG.TELEGRAM.ALLOWED_CHATS
4. Cấu hình Telegram Webhook:
   - URL: `https://script.google.com/macros/s/{SCRIPT_ID}/exec`
   - Set webhook: `https://api.telegram.org/bot{TOKEN}/setWebhook?url={WEBHOOK_URL}`

## Mapping QR

Để hiển thị trang QR, truy cập:
```
https://script.google.com/macros/s/{SCRIPT_ID}/exec?qr={QR_ID}
```

## Cấu trúc dữ liệu sheet

### BatchInfo
| batch_id | batch_name | crop_type | zone | start_date | end_date | status | blockchain_tx |
|----------|------------|-----------|------|------------|----------|--------|---------------|

### DynamicQR  
| qr_id | batch_id | created_at | metadata |
|-------|----------|------------|----------|

### Batchsheet
| timestamp | device_id | sensor_type | value | zone | batch_id |
|-----------|-----------|-------------|-------|------|----------|

### ManualEvents
| batch_id | event_type | timestamp | operator | notes | materials |
|----------|------------|-----------|----------|-------|-----------|

### BatchMedia
| batch_id | url | media_type | caption | created_at |
|----------|-----|-----------|---------|------------|

## Version History

- **V8.5.0** - Firmware với auto-recovery và health score
- **V6.5** - GAS với QR rendering và Advisory Engine  
- **V7.0** - GAS với Telegram Bot và Advanced AI

## License

MIT - EcoSynTech 2026
