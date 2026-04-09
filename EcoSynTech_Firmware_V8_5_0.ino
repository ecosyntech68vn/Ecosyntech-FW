/*
EcoSynTech Firmware V8.5.0 PRO-X SECURE + MQTT + WATCHDOG
- Thêm watchdog, tự động phục hồi, tối ưu nông nghiệp
- Bảo mật HTTPS + MQTT với nonce, HMAC, replay protection
- Quản lý năng lượng thông minh (deep sleep khi không hoạt động)
- Tương thích ESP32, hỗ trợ SD, SPIFFS, cảm biến DS18B20, DHT22, ADS1115, độ ẩm đất, pin
*/

#include <Arduino.h>
#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <HTTPClient.h>
#include <PubSubClient.h>
#include <SPI.h>
#include <SD.h>
#include <SPIFFS.h>
#include <Preferences.h>
#include <ArduinoJson.h>
#include <WiFiManager.h>
#include <time.h>
#include <mbedtls/md.h>
#include <mbedtls/sha256.h>
#include <Update.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <DHT.h>
#include <Wire.h>
#include <Adafruit_ADS1X15.h>
#include <esp_task_wdt.h>
#include <vector>
#include <map>
#include <algorithm>
#include <math.h>

// ============================ Version & Watchdog ============================
#define FW_VERSION "8.5.0"
#define WATCHDOG_TIMEOUT_SEC 30          // 30 giây watchdog
#define MAX_CONSECUTIVE_FAILS 10         // số lần fail liên tiếp để reboot
#define TZ_OFFSET_SECONDS (7 * 3600)

// ============================ Defaults ============================
#define DEFAULT_HMAC_SECRET "CEOTAQUANGTHUAN_TADUYANH_CTYTNHHDUYANH_ECOSYNTECH_2026"
#define DEFAULT_API_KEY "TADUYANH09082012_ECOSYNTECH_2026"
#define DEFAULT_WEBHOOK_URL "https://script.google.com/macros/s/AKfycbypoJdDXqnBAWwkuQGRZvaNG0OigGlfA_STVaYLA0cSMxkCQK5WMF2WbuyEDa_0xCbrQg/exec"
#define DEFAULT_MQTT_BROKER "mqtt.ecosyntech.com"
#define DEFAULT_MQTT_PORT 8883
#define DEFAULT_MQTT_USER "ecosyntech"
#define DEFAULT_MQTT_PASS "change_me"

// ============================ Pinout (EcoSynTech 5.3 PRO-X) ============================
#define SD_CS_PIN 5
#define PIN_SD_SCK 18
#define PIN_SD_MOSI 23
#define PIN_SD_MISO 19
#define PIN_SDA 21
#define PIN_SCL 22
#define ADS1115_ADDR 0x48
#define PIN_DHT22_DATA 33
#define PIN_DS18B20_DATA 32
#define PIN_SOIL_ADC 36
#define PIN_BATTERY_ADC 35
#define RELAY_PIN1 26
#define RELAY_PIN2 27
#define RELAY_PIN3 25
#define RELAY_PIN4 14
#define RELAY_ACTIVE_LEVEL HIGH
#define RELAY_INACTIVE_LEVEL LOW

// ============================ Timing ============================
#define SENSOR_INTERVAL_MS (60UL * 1000UL)
#define HEARTBEAT_INTERVAL_MS (60UL * 60UL * 1000UL)
#define BATCH_SYNC_INTERVAL_MS (6UL * 60UL * 60UL * 1000UL)
#define BACKLOG_RETRY_INTERVAL_MS (2UL * 60UL * 1000UL)
#define OTA_CHECK_INTERVAL_MS (12UL * 60UL * 60UL * 1000UL)
#define CLEANUP_INTERVAL_MS (24UL * 60UL * 60UL * 1000UL)
#define COMMAND_CHECK_INTERVAL_MS (10UL * 1000UL)
#define WIFI_RECONNECT_MIN_MS (30UL * 1000UL)
#define RULE_COOLDOWN_MS (5000UL)
#define MAX_HANDLED_COMMANDS 100
#define MAX_EVENT_LOG_SIZE 8192
#define MAX_BACKLOG_FILES_SPIFFS 2000
#define MAX_BACKLOG_BYTES_SD (50UL * 1024UL * 1024UL)
#define COMMAND_TTL_MS (7UL * 24UL * 60UL * 60UL * 1000UL)
#define REPLAY_WINDOW_SEC 1200UL
#define NONCE_EXPIRY_SEC 1200UL
#define RATE_LIMIT_PER_DEVICE 20UL
#define LOW_BATTERY_THRESHOLD_V 3.5f
#define CRITICAL_BATTERY_THRESHOLD_V 3.2f
#define TEMP_ANOMALY_THRESHOLD_C 40.0f
#define SOIL_DRY_THRESHOLD_PERCENT 30.0f
#define INVALID_SENSOR_VALUE (-999.0f)
#define DEFAULT_DEEP_SLEEP_SEC 3600UL
#define MIN_INTERVAL_SEC 60UL
static constexpr float BATTERY_DIVIDER_RATIO = (100000.0f + 33000.0f) / 33000.0f;

// ============================ Files ============================
#define BACKLOG_DIR "/backlog"
#define ARCHIVE_DIR "/archive"

// ============================ Cấu trúc dữ liệu ============================
struct DynamicConfig {
    uint32_t post_interval_sec = 3600;
    uint32_t sensor_interval_sec = 60;
    bool deep_sleep_enabled = false;
    bool ml_anomaly_enabled = false;
    uint32_t max_data_age_days = 180;
    uint32_t batch_sync_interval_sec = 21600;
    uint32_t ota_check_interval_sec = 43200;
    uint32_t cleanup_interval_sec = 86400;
    uint32_t config_version = 0;
    bool mqtt_enabled = true;
    char mqtt_broker[128] = DEFAULT_MQTT_BROKER;
    uint16_t mqtt_port = DEFAULT_MQTT_PORT;
    char mqtt_user[64] = DEFAULT_MQTT_USER;
    char mqtt_pass[64] = DEFAULT_MQTT_PASS;
    bool mqtt_tls = true;
};

struct Batch {
    String batch_id;
    String type;
    String start_date;
    String end_date;
    String status;
    String zone;
    bool force_send = false;
};

struct SensorReading {
    String sensor_type;
    float value = NAN;
    String unit;
    String sensor_id;
    String zone;
    String local_time;
    String event_ts;
};

struct ControlRule {
    String zone;
    String sensor;
    float minVal = 0;
    float maxVal = 0;
    float hysteresis = 0;
    unsigned long durationSec = 0;
    String action;
};

struct RuleState {
    unsigned long triggeredAtMs = 0;
    bool active = false;
    float lastValue = NAN;
    unsigned long lastActionMs = 0;
};

struct ConfigStatic {
    char deviceId[48] = "ECOSYNTECH0001";
    char deviceName[48] = "EcoSynTech_PROX";
    char webhookUrl[256] = DEFAULT_WEBHOOK_URL;
    char getBatchUrl[256] = DEFAULT_WEBHOOK_URL;
    char otaUrl[256] = DEFAULT_WEBHOOK_URL;
    char hmacSecret[128] = DEFAULT_HMAC_SECRET;
    char apiKey[80] = DEFAULT_API_KEY;
    bool sdEnabled = true;
    bool deepSleepEnabled = false;
    uint32_t deepSleepSec = DEFAULT_DEEP_SLEEP_SEC;
    uint32_t pushIntervalSec = 3600;
} CFG;

enum class AppState { BOOT, WIFI_SETUP, TIME_SYNC, LOAD_CONFIG, RUNNING, DEGRADED_OFFLINE, OTA_PENDING, MAINTENANCE };

struct AppMetrics {
    uint32_t sensorReadCount = 0;
    uint32_t heartbeatCount = 0;
    uint32_t postSuccessCount = 0;
    uint32_t postFailCount = 0;
    uint32_t commandExecCount = 0;
    uint32_t otaSuccessCount = 0;
    uint32_t otaFailCount = 0;
    uint32_t wifiReconnectCount = 0;
    uint32_t backlogSendCount = 0;
    uint32_t backlogFailCount = 0;
    uint32_t lastKnownFreeHeap = 0;
    uint32_t mqttPublishCount = 0;
    uint32_t mqttReconnectCount = 0;
    uint32_t consecutiveWifiFails = 0;   // Thêm đếm fail liên tiếp
    uint32_t consecutiveMqttFails = 0;
};

struct NonceRecord {
    String nonce;
    uint32_t timestamp;
};

// ============================ Globals ============================
Preferences prefs;
DynamicConfig DYN_CFG;
AppState APP_STATE = AppState::BOOT;
AppMetrics METRICS;
OneWire oneWire(PIN_DS18B20_DATA);
DallasTemperature ds18b20(&oneWire);
DHT dht(PIN_DHT22_DATA, DHT22);
Adafruit_ADS1115 ads(ADS1115_ADDR);
WiFiServer debugServer(80);
bool sdReady = false;
bool maintenanceMode = false;
volatile bool otaPending = false;
bool wifiEverConnected = false;
bool adsReady = false;
unsigned long lastSensorMs = 0, lastHeartbeatMs = 0, lastBatchSyncMs = 0, lastBacklogRetryMs = 0;
unsigned long lastOTAcheckMs = 0, lastCleanupMs = 0, lastConfigFetchMs = 0, lastCommandCheckMs = 0;
unsigned long lastWiFiReconnectAttemptMs = 0, lastNonceCleanupMs = 0, lastActivityMs = 0, bootMs = 0;
std::vector<Batch> activeBatches;
std::vector<ControlRule> controlRules;
std::map<String, RuleState> ruleStates;
std::vector<String> handledCommands;
String LAST_SENSOR_PAYLOAD = "";
uint32_t LAST_SENSOR_PAYLOAD_AT = 0;
bool LAST_SENSOR_PAYLOAD_VALID = false;
unsigned long outboundWindowStartMs = 0;
uint32_t outboundWindowCount = 0;
uint32_t eventLogWriteCount = 0;
std::vector<NonceRecord> nonceList;
const size_t MAX_NONCES = 100;

// MQTT
WiFiClientSecure mqttSecureClient;
WiFiClient mqttPlainClient;
PubSubClient mqttClient;
unsigned long lastMqttReconnectAttempt = 0;
bool mqttConnected = false;

// ============================ Forward declarations ============================
bool allowOutboundRequest();
bool extractJsonTimestamp(JsonVariantConst v, uint32_t& tsOut);
bool isWithinReplayWindow(JsonVariantConst v);
bool isCommandExpired(JsonObjectConst cmd);
void logEvent(const String& entityId, const String& eventType, const String& payload, const String& source);
void reportCommandResult(const String& commandId, const String& commandName, const String& status, const String& note);
void captureMediaStub();
bool postToBlockchain(const String& snapshotPayload, const String& deviceId);
void cleanupArchiveData();
String serializeJsonWithoutSig(JsonDocument &doc);
bool sendWrappedJSON(const String &rawPayloadJson);
bool fetchWithRetry(const String& url, String& payload, int maxRetries, uint32_t timeoutMs = 10000);
bool isNonceValid(const String& deviceId, const String& nonce, uint32_t ts);
void rememberNonce(const String& nonce, uint32_t ts);
void cleanupNonces();
void rememberCommandId(const String& commandId);
bool wasCommandHandled(const String& commandId);
void mqttCallback(char* topic, byte* payload, unsigned int length);
void mqttPublishSensorData(const String& payload);
void mqttPublishHeartbeat();
void mqttSubscribeCommands();
void mqttReconnect();
void processMqtt();
void executeLocalCommand(const String& command, JsonVariant params, const String& commandId);
bool checkOTA();
void enterDeepSleepIfNeeded();

// ============================ Tiện ích ============================
static inline bool wifiConnected() { return WiFi.status() == WL_CONNECTED; }
static inline bool isTimeValid() { return time(nullptr) >= 1600000000; }
void touchActivity() { lastActivityMs = millis(); }

void copyStringSafe(char* dst, size_t dstSize, const String& src) {
    if (!dst || dstSize == 0) return;
    snprintf(dst, dstSize, "%s", src.c_str());
}

String bytesToHex(const unsigned char *buf, size_t len) {
    static const char hex[] = "0123456789abcdef";
    String s; s.reserve(len * 2);
    for (size_t i = 0; i < len; i++) {
        s += hex[(buf[i] >> 4) & 0x0F];
        s += hex[buf[i] & 0x0F];
    }
    return s;
}

String generateNonce() {
    char nonce[33];
    for (int i = 0; i < 16; i++) {
        uint8_t b = (uint8_t)(esp_random() & 0xFF);
        snprintf(nonce + i * 2, sizeof(nonce) - (i * 2), "%02x", b);
    }
    nonce[32] = '\0';
    return String(nonce);
}

String genPayloadId() {
    uint32_t a = (uint32_t)esp_random();
    uint32_t b = millis();
    char buf[40];
    snprintf(buf, sizeof(buf), "%08X%08X", a, b);
    return String(buf);
}

String iso8601UTC(time_t t) {
    struct tm tm;
    gmtime_r(&t, &tm);
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
    return String(buf);
}

String iso8601Local(time_t t) {
    time_t localt = t + TZ_OFFSET_SECONDS;
    struct tm tm;
    gmtime_r(&localt, &tm);
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S+0700", &tm);
    return String(buf);
}

String getCurrentUTCTimestamp() { return iso8601UTC(time(nullptr)); }

String maskSecret(const String& s) {
    if (s.length() <= 8) return s;
    return s.substring(0,4) + "..." + s.substring(s.length()-4);
}

String computeSha256(const String& input) {
    uint8_t hash[32];
    mbedtls_sha256_context ctx;
    mbedtls_sha256_init(&ctx);
    mbedtls_sha256_starts_ret(&ctx, 0);
    mbedtls_sha256_update_ret(&ctx, (const uint8_t*)input.c_str(), input.length());
    mbedtls_sha256_finish_ret(&ctx, hash);
    mbedtls_sha256_free(&ctx);
    return bytesToHex(hash, 32);
}

String computeHmacSha256(const String& message, const String& key) {
    const mbedtls_md_info_t* md_info = mbedtls_md_info_from_type(MBEDTLS_MD_SHA256);
    if (!md_info) return "";
    unsigned char out[32];
    mbedtls_md_context_t ctx;
    mbedtls_md_init(&ctx);
    if (mbedtls_md_setup(&ctx, md_info, 1) != 0) {
        mbedtls_md_free(&ctx);
        return "";
    }
    bool ok = (mbedtls_md_hmac_starts(&ctx, (const unsigned char*)key.c_str(), key.length()) == 0) &&
              (mbedtls_md_hmac_update(&ctx, (const unsigned char*)message.c_str(), message.length()) == 0) &&
              (mbedtls_md_hmac_finish(&ctx, out) == 0);
    mbedtls_md_free(&ctx);
    if (!ok) return "";
    return bytesToHex(out, 32);
}

String canonicalStringify(JsonVariantConst v) {
    if (v.isNull()) return "null";
    if (v.is<bool>()) return v.as<bool>() ? "true" : "false";
    if (v.is<int>() || v.is<long>() || v.is<unsigned int>() || v.is<unsigned long>() || v.is<float>() || v.is<double>()) {
        double n = v.as<double>();
        if (isnan(n) || isinf(n)) return "null";
        String s = String(n, 6);
        while (s.indexOf('.') >= 0 && (s.endsWith("0") || s.endsWith("."))) {
            if (s.endsWith(".")) { s.remove(s.length()-1); break; }
            s.remove(s.length()-1);
        }
        return s;
    }
    if (v.is<const char*>() || v.is<String>()) {
        String s = v.as<String>();
        s.replace("\\", "\\\\");
        s.replace("\"", "\\\"");
        s.replace("\n", "\\n");
        s.replace("\r", "\\r");
        s.replace("\t", "\\t");
        return "\"" + s + "\"";
    }
    if (v.is<JsonArrayConst>()) {
        String out = "[";
        JsonArrayConst arr = v.as<JsonArrayConst>();
        for (size_t i = 0; i < arr.size(); i++) {
            if (i) out += ",";
            out += canonicalStringify(arr[i]);
        }
        out += "]";
        return out;
    }
    if (v.is<JsonObjectConst>()) {
        JsonObjectConst obj = v.as<JsonObjectConst>();
        std::vector<String> keys;
        for (JsonPairConst kv : obj) keys.push_back(String(kv.key().c_str()));
        std::sort(keys.begin(), keys.end());
        String out = "{";
        for (size_t i = 0; i < keys.size(); i++) {
            if (i) out += ",";
            out += "\"" + keys[i] + "\":";
            out += canonicalStringify(obj[keys[i].c_str()]);
        }
        out += "}";
        return out;
    }
    return "null";
}

String joinPath(const String& dir, const String& name) {
    if (name.startsWith("/")) return name;
    if (dir.endsWith("/")) return dir + name;
    return dir + "/" + name;
}

String currentBatchId() {
    if (activeBatches.empty()) return "default_batch";
    for (const auto& b : activeBatches)
        if (b.force_send || String(b.status).equalsIgnoreCase("active"))
            return b.batch_id;
    return activeBatches[0].batch_id.length() ? activeBatches[0].batch_id : "default_batch";
}

String currentZone() {
    if (activeBatches.empty()) return "default";
    for (const auto& b : activeBatches)
        if (b.force_send || String(b.status).equalsIgnoreCase("active"))
            return b.zone.length() ? b.zone : "default";
    return activeBatches[0].zone.length() ? activeBatches[0].zone : "default";
}

// ============================ Cấu hình & lưu trữ ============================
void normalizeConfig() {
    if (CFG.deepSleepSec < MIN_INTERVAL_SEC) CFG.deepSleepSec = MIN_INTERVAL_SEC;
    if (CFG.pushIntervalSec < MIN_INTERVAL_SEC) CFG.pushIntervalSec = MIN_INTERVAL_SEC;
    if (DYN_CFG.post_interval_sec < MIN_INTERVAL_SEC) DYN_CFG.post_interval_sec = CFG.pushIntervalSec;
    if (DYN_CFG.sensor_interval_sec < MIN_INTERVAL_SEC) DYN_CFG.sensor_interval_sec = 60;
    if (DYN_CFG.batch_sync_interval_sec < MIN_INTERVAL_SEC) DYN_CFG.batch_sync_interval_sec = 21600;
    if (DYN_CFG.ota_check_interval_sec < MIN_INTERVAL_SEC) DYN_CFG.ota_check_interval_sec = 43200;
    if (DYN_CFG.cleanup_interval_sec < MIN_INTERVAL_SEC) DYN_CFG.cleanup_interval_sec = 86400;
    if (DYN_CFG.max_data_age_days == 0) DYN_CFG.max_data_age_days = 180;
}

void saveConfig() {
    normalizeConfig();
    prefs.begin("ecofarm", false);
    prefs.putBytes("config", &CFG, sizeof(CFG));
    prefs.putUInt("post_int", DYN_CFG.post_interval_sec);
    prefs.putUInt("sens_int", DYN_CFG.sensor_interval_sec);
    prefs.putBool("deep_sl", DYN_CFG.deep_sleep_enabled);
    prefs.putBool("ml_anom", DYN_CFG.ml_anomaly_enabled);
    prefs.putUInt("max_age", DYN_CFG.max_data_age_days);
    prefs.putUInt("batch_sync", DYN_CFG.batch_sync_interval_sec);
    prefs.putUInt("ota_check", DYN_CFG.ota_check_interval_sec);
    prefs.putUInt("cleanup_int", DYN_CFG.cleanup_interval_sec);
    prefs.putUInt("cfg_ver", DYN_CFG.config_version);
    prefs.putBool("mqtt_en", DYN_CFG.mqtt_enabled);
    prefs.putString("mqtt_broker", String(DYN_CFG.mqtt_broker));
    prefs.putUShort("mqtt_port", DYN_CFG.mqtt_port);
    prefs.putString("mqtt_user", String(DYN_CFG.mqtt_user));
    prefs.putString("mqtt_pass", String(DYN_CFG.mqtt_pass));
    prefs.putBool("mqtt_tls", DYN_CFG.mqtt_tls);
    prefs.end();
}

void loadStoredConfig() {
    prefs.begin("ecofarm", true);
    size_t sz = prefs.getBytes("config", &CFG, sizeof(CFG));
    if (sz != sizeof(CFG)) Serial.println("[CFG] Using defaults for static config");
    else Serial.println("[CFG] Loaded static config from NVS");
    DYN_CFG.post_interval_sec = prefs.getUInt("post_int", CFG.pushIntervalSec);
    DYN_CFG.sensor_interval_sec = prefs.getUInt("sens_int", 60);
    DYN_CFG.deep_sleep_enabled = prefs.getBool("deep_sl", CFG.deepSleepEnabled);
    DYN_CFG.ml_anomaly_enabled = prefs.getBool("ml_anom", false);
    DYN_CFG.max_data_age_days = prefs.getUInt("max_age", 180);
    DYN_CFG.batch_sync_interval_sec = prefs.getUInt("batch_sync", 21600);
    DYN_CFG.ota_check_interval_sec = prefs.getUInt("ota_check", 43200);
    DYN_CFG.cleanup_interval_sec = prefs.getUInt("cleanup_int", 86400);
    DYN_CFG.config_version = prefs.getUInt("cfg_ver", 0);
    DYN_CFG.mqtt_enabled = prefs.getBool("mqtt_en", true);
    String broker = prefs.getString("mqtt_broker", DEFAULT_MQTT_BROKER);
    strncpy(DYN_CFG.mqtt_broker, broker.c_str(), sizeof(DYN_CFG.mqtt_broker)-1);
    DYN_CFG.mqtt_port = prefs.getUShort("mqtt_port", DEFAULT_MQTT_PORT);
    String user = prefs.getString("mqtt_user", DEFAULT_MQTT_USER);
    strncpy(DYN_CFG.mqtt_user, user.c_str(), sizeof(DYN_CFG.mqtt_user)-1);
    String pass = prefs.getString("mqtt_pass", DEFAULT_MQTT_PASS);
    strncpy(DYN_CFG.mqtt_pass, pass.c_str(), sizeof(DYN_CFG.mqtt_pass)-1);
    DYN_CFG.mqtt_tls = prefs.getBool("mqtt_tls", true);
    prefs.end();
    normalizeConfig();
}

void saveHandledCommands() {
    String joined;
    for (size_t i = 0; i < handledCommands.size(); i++) {
        if (i) joined += ",";
        joined += handledCommands[i];
    }
    prefs.begin("ecofarm_cmd", false);
    prefs.putString("handled_ids", joined);
    prefs.end();
}

void loadHandledCommands() {
    handledCommands.clear();
    prefs.begin("ecofarm_cmd", true);
    String raw = prefs.getString("handled_ids", "");
    prefs.end();
    if (raw.length() == 0) return;
    int start = 0;
    while (start < raw.length()) {
        int sep = raw.indexOf(',', start);
        if (sep < 0) sep = raw.length();
        String id = raw.substring(start, sep);
        id.trim();
        if (id.length()) handledCommands.push_back(id);
        start = sep + 1;
    }
}

bool wasCommandHandled(const String& commandId) {
    if (commandId.length() == 0) return true;
    for (const auto& id : handledCommands) if (id == commandId) return true;
    return false;
}

void rememberCommandId(const String& commandId) {
    if (commandId.length() == 0) return;
    if (wasCommandHandled(commandId)) return;
    handledCommands.push_back(commandId);
    if (handledCommands.size() > MAX_HANDLED_COMMANDS)
        handledCommands.erase(handledCommands.begin());
    saveHandledCommands();
}

// ============================ Nonce ============================
bool isNonceValid(const String& deviceId, const String& nonce, uint32_t ts) {
    if (nonce.length() == 0 || ts == 0) return false;
    uint32_t nowTs = (uint32_t)time(nullptr);
    if (nowTs < ts || (nowTs - ts) > NONCE_EXPIRY_SEC) return false;
    for (const auto& record : nonceList) if (record.nonce == nonce) return false;
    if (wasCommandHandled(nonce)) return false;
    return true;
}

void rememberNonce(const String& nonce, uint32_t ts) {
    if (nonce.length() == 0) return;
    uint32_t nowTs = (uint32_t)time(nullptr);
    for (auto it = nonceList.begin(); it != nonceList.end(); ) {
        if (nowTs - it->timestamp > NONCE_EXPIRY_SEC) it = nonceList.erase(it);
        else ++it;
    }
    nonceList.push_back({nonce, nowTs});
    if (nonceList.size() > MAX_NONCES) nonceList.erase(nonceList.begin());
}

void cleanupNonces() {
    uint32_t nowTs = (uint32_t)time(nullptr);
    for (auto it = nonceList.begin(); it != nonceList.end(); ) {
        if (nowTs - it->timestamp > NONCE_EXPIRY_SEC) it = nonceList.erase(it);
        else ++it;
    }
}

// ============================ Lưu trữ (SD/SPIFFS) ============================
File openStorageFile(const String& path, bool writeMode) {
    if (sdReady && CFG.sdEnabled) return SD.open(path, writeMode ? FILE_WRITE : FILE_READ);
    return SPIFFS.open(path, writeMode ? FILE_WRITE : FILE_READ);
}

bool openStorageWrite(const String& path, const String& content) {
    File f = openStorageFile(path, true);
    if (!f) return false;
    f.print(content);
    f.flush();
    f.close();
    return true;
}

bool openStorageRemove(const String& path) {
    if (sdReady && CFG.sdEnabled) return SD.remove(path);
    return SPIFFS.remove(path);
}

uint32_t getStorageUsedBytes() {
    uint32_t total = 0;
    File dir = (sdReady && CFG.sdEnabled) ? SD.open(BACKLOG_DIR) : SPIFFS.open(BACKLOG_DIR);
    if (!dir) return 0;
    while (true) {
        File entry = dir.openNextFile();
        if (!entry) break;
        if (!entry.isDirectory()) total += entry.size();
        entry.close();
    }
    dir.close();
    return total;
}

uint32_t getBacklogCount() {
    uint32_t count = 0;
    File dir = (sdReady && CFG.sdEnabled) ? SD.open(BACKLOG_DIR) : SPIFFS.open(BACKLOG_DIR);
    if (!dir) return 0;
    while (true) {
        File entry = dir.openNextFile();
        if (!entry) break;
        if (!entry.isDirectory()) count++;
        entry.close();
    }
    dir.close();
    return count;
}

bool backlogHasRoom() {
    if (sdReady && CFG.sdEnabled) return getStorageUsedBytes() < MAX_BACKLOG_BYTES_SD;
    return getBacklogCount() < MAX_BACKLOG_FILES_SPIFFS;
}

String makeBacklogPath() {
    uint32_t ts = isTimeValid() ? (uint32_t)time(nullptr) : (uint32_t)(millis() / 1000UL);
    return String(BACKLOG_DIR) + "/" + String(ts) + "_" + genPayloadId() + ".json";
}

bool saveToOfflineStorage(const String& body) {
    if (sdReady && CFG.sdEnabled) {
        if (getStorageUsedBytes() > MAX_BACKLOG_BYTES_SD) return false;
    } else {
        if (getBacklogCount() >= MAX_BACKLOG_FILES_SPIFFS) return false;
    }
    String path = makeBacklogPath();
    bool ok = openStorageWrite(path, body);
    Serial.println(ok ? ("[STORAGE] Saved offline: " + path) : ("[STORAGE] Save failed: " + path));
    return ok;
}

bool parseBacklogTimestamp(const String& path, uint32_t& tsOut) {
    String name = path;
    int slash = name.lastIndexOf('/');
    if (slash >= 0) name = name.substring(slash + 1);
    int underscore = name.indexOf('_');
    if (underscore <= 0) return false;
    String tsStr = name.substring(0, underscore);
    tsOut = (uint32_t)tsStr.toInt();
    return tsOut > 0;
}

static bool resetOutboundWindowIfNeeded() {
    unsigned long nowMs = millis();
    if (outboundWindowStartMs == 0 || (nowMs - outboundWindowStartMs) >= 60000UL) {
        outboundWindowStartMs = nowMs;
        outboundWindowCount = 0;
    }
    return true;
}

bool allowOutboundRequest() {
    resetOutboundWindowIfNeeded();
    if (outboundWindowCount >= RATE_LIMIT_PER_DEVICE) {
        Serial.println("[RATE] outbound limit reached");
        return false;
    }
    outboundWindowCount++;
    return true;
}

bool extractJsonTimestamp(JsonVariantConst v, uint32_t& tsOut) {
    tsOut = 0;
    if (v.isNull()) return false;
    if (v.is<JsonObjectConst>()) {
        JsonObjectConst obj = v.as<JsonObjectConst>();
        if (obj.containsKey("timestamp")) tsOut = obj["timestamp"] | 0UL;
        else if (obj.containsKey("ts")) tsOut = obj["ts"] | 0UL;
        else if (obj.containsKey("created_at")) tsOut = obj["created_at"] | 0UL;
        else if (obj.containsKey("event_ts")) tsOut = obj["event_ts"] | 0UL;
        else return false;
    } else if (v.is<unsigned long>() || v.is<long>() || v.is<int>() || v.is<uint32_t>()) {
        tsOut = v.as<uint32_t>();
    } else if (v.is<const char*>() || v.is<String>()) {
        String s = v.as<String>();
        tsOut = (uint32_t)s.toInt();
    } else return false;
    return tsOut > 0;
}

bool isWithinReplayWindow(JsonVariantConst v) {
    uint32_t ts = 0;
    if (!extractJsonTimestamp(v, ts)) return true;
    if (!isTimeValid()) return true;
    uint32_t nowTs = (uint32_t)time(nullptr);
    if (nowTs < ts) return false;
    return (nowTs - ts) <= REPLAY_WINDOW_SEC;
}

bool isCommandExpired(JsonObjectConst cmd) {
    uint32_t ts = 0;
    if (!extractJsonTimestamp(cmd, ts)) return false;
    if (!isTimeValid()) return false;
    uint32_t nowTs = (uint32_t)time(nullptr);
    if (nowTs < ts) return true;
    return (nowTs - ts) > (COMMAND_TTL_MS / 1000UL);
}

String eventLogPath(const String& tag) {
    uint32_t ts = isTimeValid() ? (uint32_t)time(nullptr) : (uint32_t)(millis() / 1000UL);
    return String(ARCHIVE_DIR) + "/" + String(ts) + "_" + tag + "_" + genPayloadId() + ".log";
}

void logEvent(const String& entityId, const String& eventType, const String& payload, const String& source) {
    DynamicJsonDocument doc(512);
    doc["ts"] = getCurrentUTCTimestamp();
    doc["entity_id"] = entityId;
    doc["event_type"] = eventType;
    doc["source"] = source;
    String payloadSafe = payload;
    if (payloadSafe.length() > MAX_EVENT_LOG_SIZE) payloadSafe = payloadSafe.substring(0, MAX_EVENT_LOG_SIZE);
    doc["payload"] = payloadSafe;
    String line;
    serializeJson(doc, line);
    line += "\n";
    File f = openStorageFile(eventLogPath(eventType), true);
    if (f) {
        f.print(line);
        f.close();
        eventLogWriteCount++;
    }
}

void cleanupArchiveData() {
    File dir = (sdReady && CFG.sdEnabled) ? SD.open(ARCHIVE_DIR) : SPIFFS.open(ARCHIVE_DIR);
    if (!dir) return;
    std::vector<String> paths;
    uint32_t nowEpoch = (uint32_t)time(nullptr);
    bool timeValid = isTimeValid();
    while (true) {
        File entry = dir.openNextFile();
        if (!entry) break;
        if (!entry.isDirectory()) {
            String path = String(ARCHIVE_DIR) + "/" + String(entry.name());
            if (entry.size() > 250 * 1024) openStorageRemove(path);
            else if (timeValid) {
                uint32_t fileTs = 0;
                if (parseBacklogTimestamp(path, fileTs)) {
                    uint32_t ageDays = (nowEpoch > fileTs) ? ((nowEpoch - fileTs) / 86400UL) : 0;
                    if (ageDays > DYN_CFG.max_data_age_days) {
                        openStorageRemove(path);
                        entry.close();
                        continue;
                    }
                }
                paths.push_back(path);
            } else paths.push_back(path);
        }
        entry.close();
    }
    dir.close();
    if (paths.size() > MAX_BACKLOG_FILES_SPIFFS) {
        std::sort(paths.begin(), paths.end());
        size_t extra = paths.size() - MAX_BACKLOG_FILES_SPIFFS;
        for (size_t i = 0; i < extra; i++) openStorageRemove(paths[i]);
    }
}

// ============================ MQTT ============================
void mqttCallback(char* topic, byte* payload, unsigned int length) {
    String message;
    for (unsigned int i = 0; i < length; i++) message += (char)payload[i];
    Serial.printf("[MQTT] Received on %s: %s\n", topic, message.c_str());
    DynamicJsonDocument doc(2048);
    if (deserializeJson(doc, message) != DeserializationError::Ok) {
        Serial.println("[MQTT] Invalid JSON");
        return;
    }
    if (!doc.containsKey("payload") || !doc.containsKey("signature")) {
        Serial.println("[MQTT] Missing payload/signature");
        return;
    }
    JsonObject payloadObj = doc["payload"].as<JsonObject>();
    String signature = doc["signature"].as<String>();
    String canonicalPayload = canonicalStringify(payloadObj.as<JsonVariantConst>());
    String expectedSig = computeHmacSha256(canonicalPayload, String(CFG.hmacSecret));
    if (signature != expectedSig) {
        Serial.println("[MQTT] HMAC invalid");
        return;
    }
    String nonce = payloadObj["_nonce"] | "";
    uint32_t ts = payloadObj["_ts"] | 0;
    String did = payloadObj["_did"] | "";
    if (did != CFG.deviceId) {
        Serial.println("[MQTT] Wrong device ID");
        return;
    }
    if (!isNonceValid(did, nonce, ts)) {
        Serial.println("[MQTT] Nonce invalid or replay");
        return;
    }
    if (payloadObj.containsKey("commands") && payloadObj["commands"].is<JsonArray>()) {
        for (JsonObject cmd : payloadObj["commands"].as<JsonArray>()) {
            String command = cmd["command"] | "";
            String commandId = cmd["command_id"] | "";
            JsonVariant params = cmd["params"];
            if (commandId.length() && !wasCommandHandled(commandId)) {
                executeLocalCommand(command, params, commandId);
            }
        }
    }
    rememberNonce(nonce, ts);
}

void mqttPublishSensorData(const String& payload) {
    if (!mqttConnected) return;
    String topic = String("ecosyntech/") + CFG.deviceId + "/sensor";
    if (mqttClient.publish(topic.c_str(), payload.c_str())) {
        METRICS.mqttPublishCount++;
        Serial.println("[MQTT] Sensor data published");
    } else {
        Serial.println("[MQTT] Publish failed");
        METRICS.consecutiveMqttFails++;
    }
}

void mqttPublishHeartbeat() {
    if (!mqttConnected) return;
    String topic = String("ecosyntech/") + CFG.deviceId + "/heartbeat";
    // buildHeartbeatPayload defined later
    String hb = buildHeartbeatPayload();   // forward declaration resolved later
    if (mqttClient.publish(topic.c_str(), hb.c_str())) {
        Serial.println("[MQTT] Heartbeat published");
    } else {
        METRICS.consecutiveMqttFails++;
    }
}

void mqttSubscribeCommands() {
    if (!mqttConnected) return;
    String topic = String("ecosyntech/") + CFG.deviceId + "/command";
    if (mqttClient.subscribe(topic.c_str())) {
        Serial.printf("[MQTT] Subscribed to %s\n", topic.c_str());
    } else {
        Serial.println("[MQTT] Subscribe failed");
    }
}

void mqttReconnect() {
    if (!DYN_CFG.mqtt_enabled) return;
    if (mqttConnected) return;
    unsigned long now = millis();
    if (now - lastMqttReconnectAttempt < MQTT_RECONNECT_DELAY_MS) return;
    lastMqttReconnectAttempt = now;
    Serial.println("[MQTT] Attempting reconnect...");
    if (DYN_CFG.mqtt_tls) {
        mqttSecureClient.setInsecure(); // In production, use proper cert
        mqttClient.setClient(mqttSecureClient);
    } else {
        mqttClient.setClient(mqttPlainClient);
    }
    mqttClient.setServer(DYN_CFG.mqtt_broker, DYN_CFG.mqtt_port);
    mqttClient.setCallback(mqttCallback);
    if (mqttClient.connect(CFG.deviceId, DYN_CFG.mqtt_user, DYN_CFG.mqtt_pass)) {
        mqttConnected = true;
        METRICS.mqttReconnectCount++;
        METRICS.consecutiveMqttFails = 0;
        Serial.println("[MQTT] Connected");
        mqttSubscribeCommands();
        mqttClient.publish(String("ecosyntech/") + CFG.deviceId + "/status", "online", true);
    } else {
        mqttConnected = false;
        METRICS.consecutiveMqttFails++;
        Serial.printf("[MQTT] Failed, rc=%d\n", mqttClient.state());
    }
}

void processMqtt() {
    if (!DYN_CFG.mqtt_enabled) return;
    if (!mqttConnected) {
        mqttReconnect();
    } else {
        mqttClient.loop();
    }
}

// ============================ Networking (HTTPS) ============================
String getDeviceSecret() { return String(CFG.hmacSecret); }
String getBaseUrl() { return String(CFG.webhookUrl); }
String getConfigUrl() { return String(CFG.getBatchUrl); }
String getOtaUrl() { return String(CFG.otaUrl); }

String serializeJsonWithoutSig(JsonDocument &doc) {
    if (doc.containsKey("signature")) doc.remove("signature");
    String out;
    serializeJson(doc, out);
    return out;
}

bool sendWrappedJSON(const String &rawPayloadJson) {
    if (!wifiConnected()) {
        METRICS.consecutiveWifiFails++;
        return false;
    }
    if (!allowOutboundRequest()) return false;
    DynamicJsonDocument payloadDoc(4096);
    DeserializationError err = deserializeJson(payloadDoc, rawPayloadJson);
    if (err) {
        Serial.printf("[POST] payload parse failed: %s\n", err.c_str());
        return false;
    }
    String nonce = generateNonce();
    uint32_t ts = (uint32_t)time(nullptr);
    String did = String(CFG.deviceId);
    payloadDoc["_nonce"] = nonce;
    payloadDoc["_ts"] = ts;
    payloadDoc["_did"] = did;
    String canonicalPayload = canonicalStringify(payloadDoc.as<JsonVariantConst>());
    String signature = computeHmacSha256(canonicalPayload, String(CFG.hmacSecret));
    DynamicJsonDocument requestDoc(4096);
    requestDoc["payload"] = payloadDoc;
    requestDoc["signature"] = signature;
    String body;
    serializeJson(requestDoc, body);
    WiFiClientSecure client;
    client.setInsecure();
    HTTPClient http;
    http.setTimeout(15000);
    if (!http.begin(client, CFG.webhookUrl)) return false;
    http.addHeader("Content-Type", "application/json");
    http.addHeader("x-device-id", did);
    http.addHeader("x-fw-version", FW_VERSION);
    http.addHeader("x-api-key", CFG.apiKey);
    http.addHeader("x-timestamp", String(ts));
    int code = http.POST(body);
    String resp = (code > 0) ? http.getString() : "";
    http.end();
    if (code >= 200 && code < 300) {
        DynamicJsonDocument respDoc(1024);
        if (deserializeJson(respDoc, resp) == DeserializationError::Ok) {
            if (respDoc.containsKey("signature")) {
                String serverSig = respDoc["signature"].as<String>();
                String respBody = serializeJsonWithoutSig(respDoc);
                String expected = computeHmacSha256(respBody, String(CFG.hmacSecret));
                if (serverSig != expected) {
                    Serial.println("[POST] server signature mismatch");
                    METRICS.postFailCount++;
                    METRICS.consecutiveWifiFails++;
                    return false;
                }
            }
        }
        METRICS.postSuccessCount++;
        METRICS.consecutiveWifiFails = 0;
        touchActivity();
        return true;
    }
    METRICS.postFailCount++;
    METRICS.consecutiveWifiFails++;
    Serial.printf("[POST] failed code=%d\n", code);
    return false;
}

bool fetchWithRetry(const String& url, String& payload, int maxRetries, uint32_t timeoutMs) {
    for (int attempt = 0; attempt < maxRetries; attempt++) {
        if (!wifiConnected()) {
            delay(500);
            continue;
        }
        WiFiClientSecure client;
        client.setInsecure();
        HTTPClient http;
        http.setTimeout(timeoutMs);
        if (!http.begin(client, url)) {
            delay(250 * (attempt + 1));
            continue;
        }
        int code = http.GET();
        if (code >= 200 && code < 300) {
            payload = http.getString();
            http.end();
            DynamicJsonDocument doc(4096);
            if (deserializeJson(doc, payload) == DeserializationError::Ok) {
                if (doc.containsKey("signature")) {
                    String serverSig = doc["signature"].as<String>();
                    String bodyWithoutSig = serializeJsonWithoutSig(doc);
                    String expected = computeHmacSha256(bodyWithoutSig, String(CFG.hmacSecret));
                    if (serverSig != expected) {
                        Serial.println("[FETCH] server signature invalid");
                        return false;
                    }
                }
            }
            METRICS.consecutiveWifiFails = 0;
            return true;
        }
        http.end();
        if (code == 429) delay(5000UL * (attempt + 1));
        else delay(500 * (attempt + 1));
    }
    METRICS.consecutiveWifiFails++;
    return false;
}

bool postJsonWithRetry(const String& url, const String& body, String& responseBody, int maxRetries) {
    for (int attempt = 0; attempt < maxRetries; attempt++) {
        if (!wifiConnected()) {
            delay(500);
            continue;
        }
        WiFiClientSecure client;
        client.setInsecure();
        HTTPClient http;
        http.setTimeout(12000);
        if (!http.begin(client, url)) {
            delay(250 * (attempt + 1));
            continue;
        }
        http.addHeader("Content-Type", "application/json");
        int code = http.POST(body);
        responseBody = (code > 0) ? http.getString() : "";
        http.end();
        if (code >= 200 && code < 300) {
            METRICS.consecutiveWifiFails = 0;
            return true;
        }
        Serial.printf("[HTTP] POST fail attempt %d code=%d resp=%s\n", attempt + 1, code, responseBody.c_str());
        if (code == 429) delay(5000UL * (attempt + 1));
        else delay(700 * (attempt + 1));
    }
    METRICS.consecutiveWifiFails++;
    return false;
}

void ensureWifiConnection() {
    if (wifiConnected()) {
        METRICS.consecutiveWifiFails = 0;
        return;
    }
    if (millis() - lastWiFiReconnectAttemptMs < WIFI_RECONNECT_MIN_MS) return;
    lastWiFiReconnectAttemptMs = millis();
    WiFi.reconnect();
    METRICS.wifiReconnectCount++;
    METRICS.consecutiveWifiFails++;
    // Nếu fail quá nhiều, reboot để phục hồi
    if (METRICS.consecutiveWifiFails >= MAX_CONSECUTIVE_FAILS) {
        Serial.println("[WARN] Too many WiFi failures, rebooting...");
        delay(100);
        ESP.restart();
    }
}

void syncTime() {
    APP_STATE = AppState::TIME_SYNC;
    configTime(TZ_OFFSET_SECONDS, 0, "pool.ntp.org", "time.google.com", "time.windows.com");
    time_t now = time(nullptr);
    int retries = 0;
    while (now < 1600000000 && retries < 12) {
        delay(1000);
        now = time(nullptr);
        retries++;
        Serial.printf("[NTP] Sync attempt %d\n", retries);
    }
    if (now < 1600000000) {
        Serial.println("[NTP] sync failed");
        APP_STATE = AppState::DEGRADED_OFFLINE;
    } else Serial.println("[NTP] synced: " + iso8601UTC(now));
}

// ============================ Hardware ============================
void setupHardware() {
    pinMode(RELAY_PIN1, OUTPUT); pinMode(RELAY_PIN2, OUTPUT);
    pinMode(RELAY_PIN3, OUTPUT); pinMode(RELAY_PIN4, OUTPUT);
    digitalWrite(RELAY_PIN1, RELAY_INACTIVE_LEVEL);
    digitalWrite(RELAY_PIN2, RELAY_INACTIVE_LEVEL);
    digitalWrite(RELAY_PIN3, RELAY_INACTIVE_LEVEL);
    digitalWrite(RELAY_PIN4, RELAY_INACTIVE_LEVEL);
    Wire.begin(PIN_SDA, PIN_SCL);
    ds18b20.begin();
    dht.begin();
    adsReady = ads.begin(ADS1115_ADDR);
    if (!adsReady) Serial.println("[HW] ADS1115 init failed");
    analogReadResolution(12);
    analogSetPinAttenuation(PIN_BATTERY_ADC, ADC_11db);
    analogSetPinAttenuation(PIN_SOIL_ADC, ADC_11db);
}

bool initStorage() {
    if (CFG.sdEnabled) {
        SPI.begin(PIN_SD_SCK, PIN_SD_MISO, PIN_SD_MOSI, SD_CS_PIN);
        if (SD.begin(SD_CS_PIN)) {
            sdReady = true;
            if (!SD.exists(BACKLOG_DIR)) SD.mkdir(BACKLOG_DIR);
            if (!SD.exists(ARCHIVE_DIR)) SD.mkdir(ARCHIVE_DIR);
            Serial.println("[STORAGE] SD initialized");
            return true;
        }
        Serial.println("[STORAGE] SD init failed");
        sdReady = false;
    }
    if (SPIFFS.begin(true)) {
        if (!SPIFFS.exists(BACKLOG_DIR)) SPIFFS.mkdir(BACKLOG_DIR);
        if (!SPIFFS.exists(ARCHIVE_DIR)) SPIFFS.mkdir(ARCHIVE_DIR);
        Serial.println("[STORAGE] SPIFFS initialized");
        return true;
    }
    Serial.println("[STORAGE] SPIFFS init failed");
    return false;
}

// ============================ Cảm biến ============================
float readBatteryVoltage() {
    uint32_t sum = 0;
    const int samples = 8;
    for (int i = 0; i < samples; i++) {
        sum += analogRead(PIN_BATTERY_ADC);
        delay(2);
    }
    float raw = sum / (float)samples;
    float vadc = (raw / 4095.0f) * 3.3f;
    return vadc * BATTERY_DIVIDER_RATIO;
}

SensorReading makeReading(const String& type, float value, const String& unit, const String& sensorId, const String& zone) {
    SensorReading r;
    r.sensor_type = type;
    r.value = value;
    r.unit = unit;
    r.sensor_id = sensorId;
    r.zone = zone.length() ? zone : currentZone();
    time_t now = time(nullptr);
    r.event_ts = iso8601UTC(now);
    r.local_time = iso8601Local(now);
    return r;
}

SensorReading invalidReading(const String& type, const String& unit, const String& sensorId) {
    return makeReading(type, INVALID_SENSOR_VALUE, unit, sensorId, currentZone());
}

SensorReading readDS18B20() {
    ds18b20.requestTemperatures();
    float temp = ds18b20.getTempCByIndex(0);
    if (temp == DEVICE_DISCONNECTED_C || isnan(temp))
        return invalidReading("temp", "C", "DS18B20_0");
    return makeReading("temp", temp, "C", "DS18B20_0");
}

SensorReading readDHT22Temp() {
    float temp = dht.readTemperature();
    if (isnan(temp)) return invalidReading("temp", "C", "DHT22_0");
    return makeReading("temp", temp, "C", "DHT22_0");
}

SensorReading readDHT22Humidity() {
    float hum = dht.readHumidity();
    if (isnan(hum)) return invalidReading("humidity", "%", "DHT22_0");
    return makeReading("humidity", hum, "%", "DHT22_0");
}

SensorReading readSoilMoisture() {
    int raw = analogRead(PIN_SOIL_ADC);
    float percent = map(raw, 0, 4095, 0, 100);
    if (percent < 0) percent = 0;
    if (percent > 100) percent = 100;
    return makeReading("soil_moisture", percent, "%", "SOIL_0");
}

SensorReading readADSpH() {
    if (!adsReady) return invalidReading("pH", "", "ADS1115_0");
    int16_t adc0 = ads.readADC_SingleEnded(0);
    float voltage = (adc0 * 0.1875f) / 1000.0f;
    float pH = 3.0f * voltage;
    return makeReading("pH", pH, "", "ADS1115_0");
}

SensorReading readADSTDS() {
    if (!adsReady) return invalidReading("tds", "ppm", "ADS1115_1");
    int16_t adc1 = ads.readADC_SingleEnded(1);
    float voltage = (adc1 * 0.1875f) / 1000.0f;
    float tds = voltage * 1000.0f / 2.0f;
    return makeReading("tds", tds, "ppm", "ADS1115_1");
}

SensorReading readADSDO() {
    if (!adsReady) return invalidReading("do", "mg/L", "ADS1115_2");
    int16_t adc2 = ads.readADC_SingleEnded(2);
    float voltage = (adc2 * 0.1875f) / 1000.0f;
    float do_mgL = voltage * 5.0f;
    return makeReading("do", do_mgL, "mg/L", "ADS1115_2");
}

bool isValidReading(const SensorReading& s) {
    return !isnan(s.value) && s.value > (INVALID_SENSOR_VALUE + 1.0f);
}

void processRuleForReading(const SensorReading& reading) {
    for (auto &rule : controlRules) {
        bool sensorMatch = (rule.sensor == reading.sensor_type) || (rule.sensor == reading.sensor_id);
        bool zoneMatch = rule.zone.length() == 0 || rule.zone == reading.zone;
        if (!sensorMatch || !zoneMatch) continue;
        String key = rule.zone + "|" + rule.sensor;
        RuleState& state = ruleStates[key];
        unsigned long nowMs = millis();
        bool outside = reading.value < rule.minVal || reading.value > rule.maxVal;
        bool recoveredBand = (reading.value >= (rule.minVal + rule.hysteresis)) && (reading.value <= (rule.maxVal - rule.hysteresis));
        if (outside) {
            if (!state.active) {
                if (state.triggeredAtMs == 0) state.triggeredAtMs = nowMs;
                unsigned long elapsed = nowMs - state.triggeredAtMs;
                if (rule.durationSec == 0 || elapsed >= (rule.durationSec * 1000UL)) {
                    if (state.lastActionMs == 0 || (nowMs - state.lastActionMs) >= RULE_COOLDOWN_MS) {
                        state.active = true;
                        state.lastValue = reading.value;
                        state.lastActionMs = nowMs;
                        Serial.println("[RULE] Triggered: " + rule.action + " | sensor=" + reading.sensor_type + " value=" + String(reading.value, 2));
                        if (rule.action == "relay1_on") digitalWrite(RELAY_PIN1, RELAY_ACTIVE_LEVEL);
                        else if (rule.action == "relay1_off") digitalWrite(RELAY_PIN1, RELAY_INACTIVE_LEVEL);
                        else if (rule.action == "relay2_on") digitalWrite(RELAY_PIN2, RELAY_ACTIVE_LEVEL);
                        else if (rule.action == "relay2_off") digitalWrite(RELAY_PIN2, RELAY_INACTIVE_LEVEL);
                        else if (rule.action == "relay3_on") digitalWrite(RELAY_PIN3, RELAY_ACTIVE_LEVEL);
                        else if (rule.action == "relay3_off") digitalWrite(RELAY_PIN3, RELAY_INACTIVE_LEVEL);
                        else if (rule.action == "relay4_on") digitalWrite(RELAY_PIN4, RELAY_ACTIVE_LEVEL);
                        else if (rule.action == "relay4_off") digitalWrite(RELAY_PIN4, RELAY_INACTIVE_LEVEL);
                    }
                }
            } else { state.lastValue = reading.value; }
        } else {
            state.triggeredAtMs = 0;
            if (state.active && recoveredBand && (state.lastActionMs == 0 || (nowMs - state.lastActionMs) >= RULE_COOLDOWN_MS)) {
                state.active = false;
                state.lastActionMs = nowMs;
                state.lastValue = reading.value;
                Serial.println("[RULE] Reset: " + rule.action + " | sensor=" + reading.sensor_type);
            }
        }
    }
}

void clearActiveBatches() { activeBatches.clear(); }
void clearRules() { controlRules.clear(); }

bool loadRules() {
    if (!wifiConnected()) return false;
    String url = String(CFG.getBatchUrl) + "?action=get_batch_info&device_id=" + String(CFG.deviceId) + "&api_key=" + String(CFG.apiKey);
    String payload;
    if (!fetchWithRetry(url, payload, 3)) return false;
    DynamicJsonDocument doc(8192);
    if (deserializeJson(doc, payload) != DeserializationError::Ok) {
        Serial.println("[RULES] JSON parse failed");
        return false;
    }
    if (!doc.containsKey("payload") || !doc.containsKey("signature")) {
        Serial.println("[RULES] missing payload/signature");
        return false;
    }
    JsonObject payloadObj = doc["payload"].as<JsonObject>();
    String signature = doc["signature"].as<String>();
    String canonicalPayload = canonicalStringify(payloadObj.as<JsonVariantConst>());
    String expectedSig = computeHmacSha256(canonicalPayload, String(CFG.hmacSecret));
    if (signature != expectedSig) {
        Serial.println("[RULES] HMAC invalid");
        return false;
    }
    String nonce = payloadObj["_nonce"] | "";
    uint32_t ts = payloadObj["_ts"] | 0;
    String did = payloadObj["_did"] | "";
    if (did != CFG.deviceId) {
        Serial.println("[RULES] wrong device id");
        return false;
    }
    if (!isNonceValid(did, nonce, ts)) {
        Serial.println("[RULES] nonce invalid or replay");
        return false;
    }
    clearActiveBatches();
    clearRules();
    if (payloadObj.containsKey("batches") && payloadObj["batches"].is<JsonArray>()) {
        for (JsonObject b : payloadObj["batches"].as<JsonArray>()) {
            Batch batch;
            batch.batch_id = b["batch_id"] | "";
            batch.type = b["type"] | "";
            batch.start_date = b["start_date"] | "";
            batch.end_date = b["end_date"] | "";
            batch.status = b["status"] | "";
            batch.zone = b["zone"] | "";
            batch.force_send = b["force_send"] | false;
            if (batch.batch_id.length()) activeBatches.push_back(batch);
        }
    }
    if (payloadObj.containsKey("config") && payloadObj["config"].is<JsonObject>() && payloadObj["config"]["rules"].is<JsonArray>()) {
        for (JsonObject r : payloadObj["config"]["rules"].as<JsonArray>()) {
            ControlRule rule;
            rule.zone = r["zone"] | "";
            rule.sensor = r["sensor"] | "";
            rule.minVal = r["min"] | 0.0f;
            rule.maxVal = r["max"] | 0.0f;
            rule.hysteresis = r["hysteresis"] | 0.0f;
            rule.durationSec = r["duration_sec"] | 0UL;
            rule.action = r["action"] | "";
            if (rule.sensor.length() && rule.action.length()) controlRules.push_back(rule);
        }
    } else if (payloadObj.containsKey("rules") && payloadObj["rules"].is<JsonArray>()) {
        for (JsonObject r : payloadObj["rules"].as<JsonArray>()) {
            ControlRule rule;
            rule.zone = r["zone"] | "";
            rule.sensor = r["sensor"] | "";
            rule.minVal = r["min"] | 0.0f;
            rule.maxVal = r["max"] | 0.0f;
            rule.hysteresis = r["hysteresis"] | 0.0f;
            rule.durationSec = r["duration_sec"] | 0UL;
            rule.action = r["action"] | "";
            if (rule.sensor.length() && rule.action.length()) controlRules.push_back(rule);
        }
    }
    rememberNonce(nonce, ts);
    Serial.printf("[RULES] batches=%u rules=%u\n", (unsigned)activeBatches.size(), (unsigned)controlRules.size());
    touchActivity();
    return true;
}

void executeLocalCommand(const String& command, JsonVariant params, const String& commandId) {
    if (wasCommandHandled(commandId)) return;
    String note = "ok";
    bool handled = true;
    if (command == "relay1_on") digitalWrite(RELAY_PIN1, RELAY_ACTIVE_LEVEL);
    else if (command == "relay1_off") digitalWrite(RELAY_PIN1, RELAY_INACTIVE_LEVEL);
    else if (command == "relay2_on") digitalWrite(RELAY_PIN2, RELAY_ACTIVE_LEVEL);
    else if (command == "relay2_off") digitalWrite(RELAY_PIN2, RELAY_INACTIVE_LEVEL);
    else if (command == "relay3_on") digitalWrite(RELAY_PIN3, RELAY_ACTIVE_LEVEL);
    else if (command == "relay3_off") digitalWrite(RELAY_PIN3, RELAY_INACTIVE_LEVEL);
    else if (command == "relay4_on") digitalWrite(RELAY_PIN4, RELAY_ACTIVE_LEVEL);
    else if (command == "relay4_off") digitalWrite(RELAY_PIN4, RELAY_INACTIVE_LEVEL);
    else if (command == "reboot") {
        rememberCommandId(commandId);
        delay(300);
        ESP.restart();
        return;
    }
    else if (command == "set_deep_sleep") {
        if (params.is<JsonObject>() && params["enabled"].is<bool>()) {
            DYN_CFG.deep_sleep_enabled = params["enabled"] | false;
            CFG.deepSleepEnabled = DYN_CFG.deep_sleep_enabled;
            saveConfig();
            logEvent(String(CFG.deviceId), "config_change", "{\"key\":\"deep_sleep_enabled\"}", "remote");
        } else { note = "missing_params"; handled = false; }
    }
    else if (command == "set_profile") { note = "profile_stub"; logEvent(String(CFG.deviceId), "profile_stub", "set_profile", "remote"); }
    else if (command == "maintenance_on") maintenanceMode = true;
    else if (command == "maintenance_off") maintenanceMode = false;
    else if (command == "capture_media_stub") captureMediaStub();
    else if (command == "post_blockchain_stub") {
        String snapshot = params.is<JsonObject>() ? String(params["snapshot"] | "") : "";
        postToBlockchain(snapshot, String(CFG.deviceId));
    }
    else { note = "unsupported_command"; handled = false; }
    rememberCommandId(commandId);
    METRICS.commandExecCount++;
    touchActivity();
    DynamicJsonDocument payload(1024);
    payload["event_type"] = "command_result";
    payload["command_id"] = commandId;
    payload["command"] = command;
    payload["status"] = handled ? "success" : "partial";
    payload["note"] = note;
    payload["device_id"] = String(CFG.deviceId);
    payload["fw_version"] = FW_VERSION;
    payload["ts"] = getCurrentUTCTimestamp();
    String payloadJson;
    serializeJson(payload, payloadJson);
    reportCommandResult(commandId, command, handled ? "success" : "partial", note);
    logEvent(String(CFG.deviceId), "command_exec", command + "|" + commandId + "|" + (handled ? "success" : "partial") + "|" + note, "remote");
}

bool fetchCommands() {
    if (!wifiConnected()) return false;
    String url = getConfigUrl() + "?action=get_pending_commands&device_id=" + String(CFG.deviceId) + "&api_key=" + String(CFG.apiKey);
    String payload;
    if (!fetchWithRetry(url, payload, 3)) return false;
    DynamicJsonDocument doc(8192);
    if (deserializeJson(doc, payload) != DeserializationError::Ok) {
        Serial.println("[CMD] JSON parse failed");
        return false;
    }
    if (!doc.containsKey("payload") || !doc.containsKey("signature")) {
        Serial.println("[CMD] missing payload/signature");
        return false;
    }
    JsonObject payloadObj = doc["payload"].as<JsonObject>();
    String signature = doc["signature"].as<String>();
    String canonicalPayload = canonicalStringify(payloadObj.as<JsonVariantConst>());
    String expectedSig = computeHmacSha256(canonicalPayload, String(CFG.hmacSecret));
    if (signature != expectedSig) {
        Serial.println("[CMD] HMAC invalid");
        return false;
    }
    String nonce = payloadObj["_nonce"] | "";
    uint32_t ts = payloadObj["_ts"] | 0;
    String did = payloadObj["_did"] | "";
    if (did != CFG.deviceId) {
        Serial.println("[CMD] wrong device id");
        return false;
    }
    if (!isNonceValid(did, nonce, ts)) {
        Serial.println("[CMD] nonce invalid or replay");
        return false;
    }
    if (!payloadObj.containsKey("commands") || !payloadObj["commands"].is<JsonArray>()) {
        touchActivity();
        rememberNonce(nonce, ts);
        return true;
    }
    for (JsonObject cmd : payloadObj["commands"].as<JsonArray>()) {
        String command = cmd["command"] | "";
        String commandId = cmd["command_id"] | "";
        JsonVariant params = cmd["params"];
        if (commandId.length() == 0) continue;
        if (wasCommandHandled(commandId)) continue;
        if (isCommandExpired(cmd)) {
            logEvent(String(CFG.deviceId), "command_expired", commandId + "|" + command, "remote");
            continue;
        }
        executeLocalCommand(command, params, commandId);
    }
    rememberNonce(nonce, ts);
    touchActivity();
    return true;
}

// ============================ Payloads ============================
String buildHeartbeatPayload() {
    DynamicJsonDocument doc(1024);
    doc["action"] = "heartbeat";
    doc["device_id"] = String(CFG.deviceId);
    doc["fw_version"] = FW_VERSION;
    doc["uptime_sec"] = (uint32_t)(millis() / 1000);
    doc["rssi"] = wifiConnected() ? WiFi.RSSI() : -127;
    doc["battery_v"] = readBatteryVoltage();
    doc["backlog_count"] = getBacklogCount();
    doc["free_heap"] = ESP.getFreeHeap();
    doc["config_version"] = DYN_CFG.config_version;
    doc["post_interval_sec"] = DYN_CFG.post_interval_sec;
    doc["sensor_interval_sec"] = DYN_CFG.sensor_interval_sec;
    doc["deep_sleep_enabled"] = DYN_CFG.deep_sleep_enabled;
    String out;
    serializeJson(doc, out);
    return out;
}

void appendReading(JsonArray readings, const SensorReading& s) {
    if (!isValidReading(s)) return;
    JsonObject o = readings.createNestedObject();
    o["sensor_type"] = s.sensor_type;
    o["value"] = s.value;
    o["unit"] = s.unit;
    o["sensor_id"] = s.sensor_id;
    o["zone"] = s.zone;
    o["event_ts"] = s.event_ts;
    o["local_time"] = s.local_time;
}

String buildSensorPayload() {
    DynamicJsonDocument doc(4096);
    JsonArray readings = doc.createNestedArray("readings");
    time_t now = time(nullptr);
    doc["action"] = "log_data";
    doc["device_id"] = String(CFG.deviceId);
    doc["batch_id"] = currentBatchId();
    doc["zone"] = currentZone();
    doc["uptime"] = (uint32_t)(millis() / 1000);
    doc["rssi"] = wifiConnected() ? WiFi.RSSI() : -127;
    doc["battery_v"] = readBatteryVoltage();
    doc["backlog_count"] = getBacklogCount();
    doc["event_ts"] = iso8601UTC(now);
    doc["local_time"] = iso8601Local(now);
    SensorReading r;
    r = readDS18B20(); appendReading(readings, r); processRuleForReading(r);
    r = readDHT22Temp(); appendReading(readings, r); processRuleForReading(r);
    r = readDHT22Humidity(); appendReading(readings, r); processRuleForReading(r);
    r = readSoilMoisture(); appendReading(readings, r); processRuleForReading(r);
    r = readADSpH(); appendReading(readings, r); processRuleForReading(r);
    r = readADSTDS(); appendReading(readings, r); processRuleForReading(r);
    r = readADSDO(); appendReading(readings, r); processRuleForReading(r);
    float batteryV = readBatteryVoltage();
    if (batteryV < LOW_BATTERY_THRESHOLD_V) {
        doc["alert"] = "low_battery";
        doc["battery_v"] = batteryV;
        logEvent(String(CFG.deviceId), "alert", "low_battery", "system");
    }
    if (batteryV < CRITICAL_BATTERY_THRESHOLD_V) {
        doc["critical_alert"] = "critical_battery";
        logEvent(String(CFG.deviceId), "critical_alert", "critical_battery", "system");
    }
    // Cảnh báo đất khô
    for (JsonObject reading : readings) {
        String type = reading["sensor_type"].as<String>();
        float val = reading["value"] | NAN;
        if (type == "soil_moisture" && !isnan(val) && val < SOIL_DRY_THRESHOLD_PERCENT) {
            doc["alert"] = "soil_dry";
            logEvent(String(CFG.deviceId), "alert", "soil_dry", "system");
        }
    }
    if (DYN_CFG.ml_anomaly_enabled) {
        bool highTemp = false;
        for (JsonObject reading : readings) {
            String sensorType = reading["sensor_type"].as<String>();
            float value = reading["value"] | NAN;
            if (sensorType == "temp" && !isnan(value) && value > TEMP_ANOMALY_THRESHOLD_C) {
                highTemp = true;
                break;
            }
        }
        if (highTemp) doc["anomaly"] = "high_temp";
    }
    String out;
    serializeJson(doc, out);
    return out;
}

void cacheLatestSensorPayload(const String& payload) {
    LAST_SENSOR_PAYLOAD = payload;
    LAST_SENSOR_PAYLOAD_AT = millis();
    LAST_SENSOR_PAYLOAD_VALID = true;
}

bool sendSensorDataNow(const String& payload) {
    bool ok = false;
    if (wifiConnected()) {
        if (sendWrappedJSON(payload)) ok = true;
        else if (backlogHasRoom()) saveToOfflineStorage(payload);
    } else {
        if (backlogHasRoom()) saveToOfflineStorage(payload);
    }
    if (DYN_CFG.mqtt_enabled && mqttConnected) mqttPublishSensorData(payload);
    return ok;
}

void processHeartbeatLoop() {
    String hb = buildHeartbeatPayload();
    METRICS.heartbeatCount++;
    logEvent(String(CFG.deviceId), "heartbeat", hb, "system");
    if (wifiConnected()) {
        if (!sendWrappedJSON(hb)) {
            if (backlogHasRoom()) saveToOfflineStorage(hb);
        }
    } else {
        if (backlogHasRoom()) saveToOfflineStorage(hb);
    }
    if (DYN_CFG.mqtt_enabled && mqttConnected) mqttPublishHeartbeat();
    touchActivity();
}

void processSensorLoop() {
    String payload = buildSensorPayload();
    cacheLatestSensorPayload(payload);
    logEvent(String(CFG.deviceId), "sensor_payload", payload, "system");
    if (!sendSensorDataNow(payload)) Serial.println("[SENSOR] stored offline or dropped (no room)");
    touchActivity();
}

// ============================ OTA ============================
bool checkOTA() {
    if (!wifiConnected()) return false;
    String url = getOtaUrl() + "?action=ota_check&device_id=" + String(CFG.deviceId) + "&api_key=" + String(CFG.apiKey);
    String payload;
    if (!fetchWithRetry(url, payload, 3)) return false;
    DynamicJsonDocument doc(4096);
    if (deserializeJson(doc, payload) != DeserializationError::Ok) return false;
    if (!doc.containsKey("payload") || !doc.containsKey("signature")) return false;
    JsonObject payloadObj = doc["payload"].as<JsonObject>();
    String signature = doc["signature"].as<String>();
    String canonicalPayload = canonicalStringify(payloadObj.as<JsonVariantConst>());
    String expectedSig = computeHmacSha256(canonicalPayload, String(CFG.hmacSecret));
    if (signature != expectedSig) {
        Serial.println("[OTA] HMAC invalid");
        METRICS.otaFailCount++;
        return false;
    }
    String nonce = payloadObj["_nonce"] | "";
    uint32_t ts = payloadObj["_ts"] | 0;
    String did = payloadObj["_did"] | "";
    if (did != CFG.deviceId) {
        Serial.println("[OTA] wrong device id");
        return false;
    }
    if (!isNonceValid(did, nonce, ts)) {
        Serial.println("[OTA] nonce invalid or replay");
        return false;
    }
    JsonObject manifest;
    if (payloadObj.containsKey("manifest") && payloadObj["manifest"].is<JsonObject>())
        manifest = payloadObj["manifest"].as<JsonObject>();
    else manifest = payloadObj.as<JsonObject>();
    bool updateAvailable = manifest["update_available"] | false;
    String version = manifest["version"] | "";
    if (!updateAvailable || version == FW_VERSION) return false;
    String binUrl = manifest["bin_url"] | "";
    String expectedSha256 = manifest["sha256"] | "";
    uint32_t size = manifest["size"] | 0;
    if (binUrl.isEmpty()) return false;
    WiFiClientSecure client;
    client.setInsecure();
    HTTPClient http;
    http.setTimeout(20000);
    if (!http.begin(client, binUrl)) return false;
    int code = http.GET();
    if (code != HTTP_CODE_OK) {
        http.end();
        METRICS.otaFailCount++;
        return false;
    }
    int len = http.getSize();
    if (size > 0 && len > 0 && len != (int)size) {
        Serial.println("[OTA] size mismatch");
        http.end();
        METRICS.otaFailCount++;
        return false;
    }
    if (!Update.begin(len > 0 ? len : UPDATE_SIZE_UNKNOWN)) {
        http.end();
        METRICS.otaFailCount++;
        return false;
    }
    WiFiClient* stream = http.getStreamPtr();
    uint8_t buf[1024];
    size_t written = 0;
    mbedtls_sha256_context ctx;
    mbedtls_sha256_init(&ctx);
    mbedtls_sha256_starts_ret(&ctx, 0);
    unsigned long startedAt = millis();
    while (http.connected() && (len > 0 ? written < (size_t)len : true)) {
        if (millis() - startedAt > 120000UL) {
            Serial.println("[OTA] timeout");
            Update.abort();
            mbedtls_sha256_free(&ctx);
            http.end();
            METRICS.otaFailCount++;
            return false;
        }
        size_t avail = stream->available();
        if (!avail) {
            delay(10);
            continue;
        }
        size_t toRead = min(sizeof(buf), avail);
        int r = stream->readBytes(buf, toRead);
        if (r <= 0) break;
        mbedtls_sha256_update_ret(&ctx, buf, r);
        if (Update.write(buf, r) != (size_t)r) {
            Serial.println("[OTA] write failed");
            Update.abort();
            mbedtls_sha256_free(&ctx);
            http.end();
            METRICS.otaFailCount++;
            return false;
        }
        written += r;
    }
    uint8_t hash[32];
    mbedtls_sha256_finish_ret(&ctx, hash);
    mbedtls_sha256_free(&ctx);
    http.end();
    if (len > 0 && written != (size_t)len) {
        Serial.println("[OTA] incomplete download");
        Update.abort();
        METRICS.otaFailCount++;
        return false;
    }
    String actualSha = bytesToHex(hash, 32);
    if (expectedSha256.length() == 64 && actualSha != expectedSha256) {
        Serial.println("[OTA] sha256 mismatch");
        Update.abort();
        METRICS.otaFailCount++;
        return false;
    }
    if (!Update.end(true)) {
        Serial.println("[OTA] update end failed");
        METRICS.otaFailCount++;
        return false;
    }
    otaPending = true;
    METRICS.otaSuccessCount++;
    Serial.println("[OTA] downloaded, pending reboot");
    touchActivity();
    return true;
}

// ============================ Remote config ============================
bool fetchRemoteConfig() {
    if (!wifiConnected()) return false;
    String url = getConfigUrl() + "?action=get_config&device_id=" + String(CFG.deviceId) + "&api_key=" + String(CFG.apiKey);
    String payload;
    if (!fetchWithRetry(url, payload, 3)) return false;
    DynamicJsonDocument doc(4096);
    if (deserializeJson(doc, payload) != DeserializationError::Ok) {
        Serial.println("[CONFIG] JSON parse failed");
        return false;
    }
    if (!doc.containsKey("payload") || !doc.containsKey("signature")) return false;
    JsonObject payloadObj = doc["payload"].as<JsonObject>();
    String signature = doc["signature"].as<String>();
    String canonicalPayload = canonicalStringify(payloadObj.as<JsonVariantConst>());
    String expectedSig = computeHmacSha256(canonicalPayload, String(CFG.hmacSecret));
    if (signature != expectedSig) {
        Serial.println("[CONFIG] HMAC invalid");
        return false;
    }
    String nonce = payloadObj["_nonce"] | "";
    uint32_t ts = payloadObj["_ts"] | 0;
    String did = payloadObj["_did"] | "";
    if (did != CFG.deviceId) {
        Serial.println("[CONFIG] wrong device id");
        return false;
    }
    if (!isNonceValid(did, nonce, ts)) {
        Serial.println("[CONFIG] nonce invalid or replay");
        return false;
    }
    bool changed = false;
    uint32_t remoteVer = 0;
    if (payloadObj.containsKey("config") && payloadObj["config"].is<JsonObject>()) {
        JsonObject config = payloadObj["config"].as<JsonObject>();
        remoteVer = config["config_version"] | payloadObj["config_version"] | (DYN_CFG.config_version + 1);
        if (remoteVer > DYN_CFG.config_version) {
            DYN_CFG.post_interval_sec = config["post_interval_sec"] | DYN_CFG.post_interval_sec;
            DYN_CFG.sensor_interval_sec = config["sensor_interval_sec"] | DYN_CFG.sensor_interval_sec;
            DYN_CFG.deep_sleep_enabled = config["deep_sleep_enabled"] | DYN_CFG.deep_sleep_enabled;
            DYN_CFG.ml_anomaly_enabled = config["ml_anomaly_enabled"] | DYN_CFG.ml_anomaly_enabled;
            DYN_CFG.max_data_age_days = config["max_data_age_days"] | DYN_CFG.max_data_age_days;
            DYN_CFG.batch_sync_interval_sec = config["batch_sync_interval_sec"] | DYN_CFG.batch_sync_interval_sec;
            DYN_CFG.ota_check_interval_sec = config["ota_check_interval_sec"] | DYN_CFG.ota_check_interval_sec;
            DYN_CFG.cleanup_interval_sec = config["cleanup_interval_sec"] | DYN_CFG.cleanup_interval_sec;
            DYN_CFG.config_version = remoteVer;
            CFG.pushIntervalSec = DYN_CFG.post_interval_sec;
            saveConfig();
            changed = true;
            Serial.println("[CONFIG] Updated v" + String(DYN_CFG.config_version));
        }
        rememberNonce(nonce, ts);
        touchActivity();
        return true;
    }
    remoteVer = payloadObj["config_version"] | (DYN_CFG.config_version + 1);
    if (remoteVer > DYN_CFG.config_version) {
        if (payloadObj.containsKey("post_interval_sec")) {
            DYN_CFG.post_interval_sec = payloadObj["post_interval_sec"] | DYN_CFG.post_interval_sec;
            CFG.pushIntervalSec = DYN_CFG.post_interval_sec;
            changed = true;
        }
        if (payloadObj.containsKey("sensor_interval_sec")) { DYN_CFG.sensor_interval_sec = payloadObj["sensor_interval_sec"] | DYN_CFG.sensor_interval_sec; changed = true; }
        if (payloadObj.containsKey("deep_sleep_enabled")) { DYN_CFG.deep_sleep_enabled = payloadObj["deep_sleep_enabled"] | DYN_CFG.deep_sleep_enabled; changed = true; }
        if (payloadObj.containsKey("ml_anomaly_enabled")) { DYN_CFG.ml_anomaly_enabled = payloadObj["ml_anomaly_enabled"] | DYN_CFG.ml_anomaly_enabled; changed = true; }
        if (payloadObj.containsKey("max_data_age_days")) { DYN_CFG.max_data_age_days = payloadObj["max_data_age_days"] | DYN_CFG.max_data_age_days; changed = true; }
        if (payloadObj.containsKey("batch_sync_interval_sec")) { DYN_CFG.batch_sync_interval_sec = payloadObj["batch_sync_interval_sec"] | DYN_CFG.batch_sync_interval_sec; changed = true; }
        if (payloadObj.containsKey("ota_check_interval_sec")) { DYN_CFG.ota_check_interval_sec = payloadObj["ota_check_interval_sec"] | DYN_CFG.ota_check_interval_sec; changed = true; }
        if (payloadObj.containsKey("cleanup_interval_sec")) { DYN_CFG.cleanup_interval_sec = payloadObj["cleanup_interval_sec"] | DYN_CFG.cleanup_interval_sec; changed = true; }
        DYN_CFG.config_version = remoteVer;
        if (changed) saveConfig();
        Serial.println("[CONFIG] Updated legacy top-level config");
    }
    rememberNonce(nonce, ts);
    touchActivity();
    return true;
}

// ============================ Backlog ============================
bool processOneBacklogPayload(const String& body) { return sendWrappedJSON(body); }

void retryBacklog() {
    if (!wifiConnected()) return;
    uint32_t count = 0;
    const uint32_t maxPerRun = 10;
    File dir = (sdReady && CFG.sdEnabled) ? SD.open(BACKLOG_DIR) : SPIFFS.open(BACKLOG_DIR);
    if (!dir) return;
    while (count < maxPerRun) {
        File entry = dir.openNextFile();
        if (!entry) break;
        if (!entry.isDirectory()) {
            String path = joinPath(BACKLOG_DIR, String(entry.name()));
            File file = (sdReady && CFG.sdEnabled) ? SD.open(path, FILE_READ) : SPIFFS.open(path, FILE_READ);
            if (file) {
                String body = file.readString();
                file.close();
                if (processOneBacklogPayload(body)) {
                    openStorageRemove(path);
                    METRICS.backlogSendCount++;
                    Serial.println("[BACKLOG] sent and deleted: " + path);
                } else {
                    METRICS.backlogFailCount++;
                    Serial.println("[BACKLOG] failed: " + path);
                }
            }
            count++;
        }
        entry.close();
    }
    dir.close();
    touchActivity();
    Serial.printf("[BACKLOG] retry processed %u files\n", (unsigned)count);
}

void cleanupOldData() {
    File dir = (sdReady && CFG.sdEnabled) ? SD.open(BACKLOG_DIR) : SPIFFS.open(BACKLOG_DIR);
    if (!dir) return;
    std::vector<String> paths;
    uint32_t nowEpoch = (uint32_t)time(nullptr);
    bool timeValid = isTimeValid();
    while (true) {
        File entry = dir.openNextFile();
        if (!entry) break;
        if (!entry.isDirectory()) {
            String path = joinPath(BACKLOG_DIR, String(entry.name()));
            if (entry.size() > 250 * 1024) openStorageRemove(path);
            else {
                if (timeValid) {
                    uint32_t fileTs = 0;
                    if (parseBacklogTimestamp(path, fileTs)) {
                        uint32_t ageDays = (nowEpoch > fileTs) ? ((nowEpoch - fileTs) / 86400UL) : 0;
                        if (ageDays > DYN_CFG.max_data_age_days) {
                            openStorageRemove(path);
                            entry.close();
                            continue;
                        }
                    }
                }
                paths.push_back(path);
            }
        }
        entry.close();
    }
    dir.close();
    if (paths.size() > MAX_BACKLOG_FILES_SPIFFS) {
        std::sort(paths.begin(), paths.end());
        size_t extra = paths.size() - MAX_BACKLOG_FILES_SPIFFS;
        for (size_t i = 0; i < extra; i++) openStorageRemove(paths[i]);
    }
    touchActivity();
}

void reportCommandResult(const String& commandId, const String& commandName, const String& status, const String& note) {
    DynamicJsonDocument payload(1024);
    payload["event_type"] = "command_result";
    payload["command_id"] = commandId;
    payload["command"] = commandName;
    payload["status"] = status;
    payload["note"] = note;
    payload["device_id"] = String(CFG.deviceId);
    payload["fw_version"] = FW_VERSION;
    payload["ts"] = getCurrentUTCTimestamp();
    String payloadJson;
    serializeJson(payload, payloadJson);
    if (wifiConnected()) {
        String resp;
        postJsonWithRetry(String(CFG.webhookUrl), payloadJson, resp, 2);
    }
    if (DYN_CFG.mqtt_enabled && mqttConnected) {
        String topic = String("ecosyntech/") + CFG.deviceId + "/result";
        mqttClient.publish(topic.c_str(), payloadJson.c_str());
    }
}

void captureMediaStub() {
    String batchId = activeBatches.size() ? activeBatches[0].batch_id : "default_batch";
    String metaDesc = "Camera capture from " + String(CFG.deviceName);
    DynamicJsonDocument mediaDoc(4096);
    mediaDoc["action"] = "upload_media";
    mediaDoc["batchId"] = batchId;
    mediaDoc["metaDesc"] = metaDesc;
    mediaDoc["device_id"] = String(CFG.deviceId);
    mediaDoc["fw_version"] = FW_VERSION;
    JsonArray files = mediaDoc.createNestedArray("files");
    for (int i = 0; i < 5; i++) {
        JsonObject file = files.createNestedObject();
        file["name"] = "image_" + String(i) + ".jpg";
        file["mimeType"] = "image/jpeg";
        file["data"] = "base64_dummy_data";
    }
    String mediaPayload;
    serializeJson(mediaDoc, mediaPayload);
    logEvent(String(CFG.deviceId), "media_stub", mediaPayload, "local");
    sendWrappedJSON(mediaPayload);
}

bool postToBlockchain(const String& snapshotPayload, const String& deviceId) {
    DynamicJsonDocument doc(1024);
    doc["ts"] = getCurrentUTCTimestamp();
    doc["device_id"] = deviceId;
    doc["payload"] = snapshotPayload;
    doc["rpc"] = "https://dummy-blockchain-rpc";  // Thay bằng RPC thực tế
    String out;
    serializeJson(doc, out);
    String path = String(ARCHIVE_DIR) + "/" + genPayloadId() + ".blockchain.json";
    bool ok = openStorageWrite(path, out);
    if (ok) logEvent(deviceId, "blockchain_stub", out, "local");
    return ok;
}

// ============================ Health & Debug ============================
String buildHealthJson() {
    DynamicJsonDocument doc(1024);
    doc["device_id"] = String(CFG.deviceId);
    doc["fw_version"] = FW_VERSION;
    doc["uptime_sec"] = (uint32_t)(millis() / 1000);
    doc["rssi"] = wifiConnected() ? WiFi.RSSI() : -127;
    doc["battery_v"] = readBatteryVoltage();
    doc["backlog_count"] = getBacklogCount();
    doc["free_heap"] = ESP.getFreeHeap();
    doc["min_free_heap"] = ESP.getMinFreeHeap();
    doc["config_version"] = DYN_CFG.config_version;
    doc["wifi_connected"] = wifiConnected();
    doc["ota_pending"] = otaPending;
    doc["sensor_reads"] = METRICS.sensorReadCount;
    doc["heartbeat_count"] = METRICS.heartbeatCount;
    doc["post_success"] = METRICS.postSuccessCount;
    doc["post_fail"] = METRICS.postFailCount;
    doc["command_exec"] = METRICS.commandExecCount;
    doc["backlog_send"] = METRICS.backlogSendCount;
    doc["backlog_fail"] = METRICS.backlogFailCount;
    doc["event_log_writes"] = eventLogWriteCount;
    doc["push_interval_sec"] = DYN_CFG.post_interval_sec;
    doc["sensor_interval_sec"] = DYN_CFG.sensor_interval_sec;
    doc["deep_sleep_enabled"] = DYN_CFG.deep_sleep_enabled;
    doc["rate_limit_per_device"] = RATE_LIMIT_PER_DEVICE;
    doc["replay_window_sec"] = REPLAY_WINDOW_SEC;
    doc["mqtt_enabled"] = DYN_CFG.mqtt_enabled;
    doc["mqtt_connected"] = mqttConnected;
    String out;
    serializeJson(doc, out);
    return out;
}

void handleDebugServer() {
    WiFiClient client = debugServer.available();
    if (!client) return;
    client.setTimeout(2000);
    String req = client.readStringUntil('\r');
    client.readStringUntil('\n');
    int sp1 = req.indexOf(' ');
    int sp2 = req.indexOf(' ', sp1 + 1);
    String path = "/";
    if (sp1 >= 0 && sp2 > sp1) path = req.substring(sp1 + 1, sp2);
    client.println("HTTP/1.1 200 OK");
    client.println("Content-Type: application/json; charset=utf-8");
    client.println("Connection: close");
    client.println();
    if (path.indexOf("/health") >= 0) client.println(buildHealthJson());
    else {
        DynamicJsonDocument doc(512);
        doc["device"] = String(CFG.deviceName);
        doc["fw_version"] = FW_VERSION;
        doc["uptime_sec"] = (uint32_t)(millis() / 1000);
        doc["rssi"] = wifiConnected() ? WiFi.RSSI() : -127;
        doc["battery_v"] = readBatteryVoltage();
        doc["backlog_count"] = getBacklogCount();
        String out;
        serializeJson(doc, out);
        client.println(out);
    }
    client.stop();
}

// ============================ State machine & Sleep ============================
void updateAppState() {
    if (otaPending) APP_STATE = AppState::OTA_PENDING;
    else if (maintenanceMode) APP_STATE = AppState::MAINTENANCE;
    else if (!wifiConnected() && wifiEverConnected) APP_STATE = AppState::DEGRADED_OFFLINE;
    else APP_STATE = AppState::RUNNING;
}

void enterDeepSleepIfNeeded() {
    if (!DYN_CFG.deep_sleep_enabled || maintenanceMode) return;
    if (CFG.deepSleepSec < MIN_INTERVAL_SEC) return;
    // Kiểm tra xem có lệnh chờ hay không (nếu có command chưa xử lý thì không sleep)
    bool hasPendingCommands = false; // Có thể mở rộng kiểm tra qua MQTT hoặc backlog
    if (hasPendingCommands) return;
    unsigned long idleMs = millis() - lastActivityMs;
    unsigned long targetMs = CFG.deepSleepSec * 1000UL;
    if (idleMs >= targetMs) {
        Serial.println("[SLEEP] entering deep sleep");
        delay(100);
        // Lưu trạng thái cần thiết vào RTC memory (tùy chọn)
        esp_sleep_enable_timer_wakeup((uint64_t)CFG.deepSleepSec * 1000000ULL);
        esp_deep_sleep_start();
    }
}

// ============================ WiFiManager ============================
void setupWiFiManager() {
    WiFi.mode(WIFI_STA);
    WiFi.setAutoReconnect(true);
    WiFiManager wm;
    char deviceIdBuf[48], deviceNameBuf[48], webhookUrlBuf[256], batchUrlBuf[256], hmacSecretBuf[128];
    char otaUrlBuf[256], apiKeyBuf[80], sdEnabledBuf[4], deepSleepEnabledBuf[4], deepSleepSecBuf[12], pushIntervalBuf[12];
    copyStringSafe(deviceIdBuf, sizeof(deviceIdBuf), String(CFG.deviceId));
    copyStringSafe(deviceNameBuf, sizeof(deviceNameBuf), String(CFG.deviceName));
    copyStringSafe(webhookUrlBuf, sizeof(webhookUrlBuf), String(CFG.webhookUrl));
    copyStringSafe(batchUrlBuf, sizeof(batchUrlBuf), String(CFG.getBatchUrl));
    copyStringSafe(hmacSecretBuf, sizeof(hmacSecretBuf), String(CFG.hmacSecret));
    copyStringSafe(otaUrlBuf, sizeof(otaUrlBuf), String(CFG.otaUrl));
    copyStringSafe(apiKeyBuf, sizeof(apiKeyBuf), String(CFG.apiKey));
    copyStringSafe(sdEnabledBuf, sizeof(sdEnabledBuf), CFG.sdEnabled ? "1" : "0");
    copyStringSafe(deepSleepEnabledBuf, sizeof(deepSleepEnabledBuf), DYN_CFG.deep_sleep_enabled ? "1" : "0");
    copyStringSafe(deepSleepSecBuf, sizeof(deepSleepSecBuf), String(CFG.deepSleepSec));
    copyStringSafe(pushIntervalBuf, sizeof(pushIntervalBuf), String(DYN_CFG.post_interval_sec));
    WiFiManagerParameter p_deviceId("deviceid", "Device ID", deviceIdBuf, sizeof(deviceIdBuf));
    WiFiManagerParameter p_deviceName("devicename", "Device Name", deviceNameBuf, sizeof(deviceNameBuf));
    WiFiManagerParameter p_webhookUrl("webhook", "Webhook URL", webhookUrlBuf, sizeof(webhookUrlBuf));
    WiFiManagerParameter p_getBatchUrl("batchurl", "Config/Command URL", batchUrlBuf, sizeof(batchUrlBuf));
    WiFiManagerParameter p_hmacSecret("hmac", "HMAC Secret", hmacSecretBuf, sizeof(hmacSecretBuf));
    WiFiManagerParameter p_otaUrl("ota", "OTA URL", otaUrlBuf, sizeof(otaUrlBuf));
    WiFiManagerParameter p_apiKey("apikey", "API Key", apiKeyBuf, sizeof(apiKeyBuf));
    WiFiManagerParameter p_sdEnabled("sdenabled", "SD Enabled (0/1)", sdEnabledBuf, sizeof(sdEnabledBuf));
    WiFiManagerParameter p_deepSleepEnabled("deepsleep", "Deep Sleep Enabled (0/1)", deepSleepEnabledBuf, sizeof(deepSleepEnabledBuf));
    WiFiManagerParameter p_deepSleepSec("deepsleepsec", "Deep Sleep Seconds", deepSleepSecBuf, sizeof(deepSleepSecBuf));
    WiFiManagerParameter p_pushIntervalSec("pushinterval", "Push Interval Seconds", pushIntervalBuf, sizeof(pushIntervalBuf));
    wm.addParameter(&p_deviceId); wm.addParameter(&p_deviceName); wm.addParameter(&p_webhookUrl);
    wm.addParameter(&p_getBatchUrl); wm.addParameter(&p_hmacSecret); wm.addParameter(&p_otaUrl);
    wm.addParameter(&p_apiKey); wm.addParameter(&p_sdEnabled); wm.addParameter(&p_deepSleepEnabled);
    wm.addParameter(&p_deepSleepSec); wm.addParameter(&p_pushIntervalSec);
    wm.setConfigPortalTimeout(180);
    if (!wm.autoConnect("EcoSynTech-Setup", "password")) {
        Serial.println("[WiFi] Failed to connect, restarting");
        delay(3000);
        ESP.restart();
    }
    copyStringSafe(CFG.deviceId, sizeof(CFG.deviceId), String(p_deviceId.getValue()));
    copyStringSafe(CFG.deviceName, sizeof(CFG.deviceName), String(p_deviceName.getValue()));
    copyStringSafe(CFG.webhookUrl, sizeof(CFG.webhookUrl), String(p_webhookUrl.getValue()));
    copyStringSafe(CFG.getBatchUrl, sizeof(CFG.getBatchUrl), String(p_getBatchUrl.getValue()));
    copyStringSafe(CFG.hmacSecret, sizeof(CFG.hmacSecret), String(p_hmacSecret.getValue()));
    copyStringSafe(CFG.otaUrl, sizeof(CFG.otaUrl), String(p_otaUrl.getValue()));
    copyStringSafe(CFG.apiKey, sizeof(CFG.apiKey), String(p_apiKey.getValue()));
    CFG.sdEnabled = String(p_sdEnabled.getValue()).toInt() != 0;
    DYN_CFG.deep_sleep_enabled = String(p_deepSleepEnabled.getValue()).toInt() != 0;
    CFG.deepSleepEnabled = DYN_CFG.deep_sleep_enabled;
    CFG.deepSleepSec = max((unsigned long)MIN_INTERVAL_SEC, (unsigned long)strtoul(p_deepSleepSec.getValue(), nullptr, 10));
    CFG.pushIntervalSec = max((unsigned long)MIN_INTERVAL_SEC, (unsigned long)strtoul(p_pushIntervalSec.getValue(), nullptr, 10));
    DYN_CFG.post_interval_sec = CFG.pushIntervalSec;
    saveConfig();
    wifiEverConnected = true;
    touchActivity();
    Serial.println("[WiFi] Connected IP: " + WiFi.localIP().toString());
}

// ============================ Setup & Loop ============================
void setup() {
    Serial.begin(115200);
    delay(1000);
    Serial.println();
    Serial.println("EcoSynTech Firmware v" + String(FW_VERSION));
    // Khởi tạo watchdog với timeout 30 giây
    esp_task_wdt_init(WATCHDOG_TIMEOUT_SEC, true);
    esp_task_wdt_add(NULL);
    bootMs = millis();
    lastActivityMs = millis();
    APP_STATE = AppState::BOOT;
    setupHardware();
    loadStoredConfig();
    loadHandledCommands();
    initStorage();
    APP_STATE = AppState::WIFI_SETUP;
    setupWiFiManager();
    syncTime();
    APP_STATE = AppState::LOAD_CONFIG;
    loadRules();
    debugServer.begin();
    // MQTT
    mqttClient.setClient(DYN_CFG.mqtt_tls ? mqttSecureClient : mqttPlainClient);
    mqttClient.setServer(DYN_CFG.mqtt_broker, DYN_CFG.mqtt_port);
    mqttClient.setCallback(mqttCallback);
    mqttReconnect();
    lastSensorMs = millis();
    lastHeartbeatMs = millis();
    lastBatchSyncMs = millis();
    lastBacklogRetryMs = millis();
    lastOTAcheckMs = millis();
    lastCleanupMs = millis();
    lastConfigFetchMs = millis();
    lastCommandCheckMs = millis();
    lastNonceCleanupMs = millis();
    normalizeConfig();
    updateAppState();
}

void loop() {
    // Reset watchdog mỗi vòng lặp
    esp_task_wdt_reset();
    unsigned long nowMs = millis();
    updateAppState();
    if (otaPending) {
        Serial.println("[OTA] rebooting to apply update...");
        delay(1000);
        ESP.restart();
    }
    ensureWifiConnection();
    processMqtt();
    if (nowMs - lastSensorMs >= (DYN_CFG.sensor_interval_sec * 1000UL)) {
        processSensorLoop();
        lastSensorMs = nowMs;
    }
    if (nowMs - lastHeartbeatMs >= (DYN_CFG.post_interval_sec * 1000UL)) {
        processHeartbeatLoop();
        lastHeartbeatMs = nowMs;
    }
    if (nowMs - lastBatchSyncMs >= (DYN_CFG.batch_sync_interval_sec * 1000UL)) {
        loadRules();
        lastBatchSyncMs = nowMs;
    }
    if (nowMs - lastBacklogRetryMs >= BACKLOG_RETRY_INTERVAL_MS) {
        retryBacklog();
        lastBacklogRetryMs = nowMs;
    }
    if (nowMs - lastOTAcheckMs >= (DYN_CFG.ota_check_interval_sec * 1000UL)) {
        checkOTA();
        lastOTAcheckMs = nowMs;
    }
    if (nowMs - lastCleanupMs >= (DYN_CFG.cleanup_interval_sec * 1000UL)) {
        cleanupOldData();
        cleanupArchiveData();
        lastCleanupMs = nowMs;
    }
    if (nowMs - lastConfigFetchMs >= 3600UL * 1000UL) {
        fetchRemoteConfig();
        lastConfigFetchMs = nowMs;
    }
    if (nowMs - lastCommandCheckMs >= COMMAND_CHECK_INTERVAL_MS) {
        fetchCommands();
        lastCommandCheckMs = nowMs;
    }
    if (nowMs - lastNonceCleanupMs >= 3600000UL) {
        cleanupNonces();
        lastNonceCleanupMs = nowMs;
    }
    enterDeepSleepIfNeeded();
    handleDebugServer();
    METRICS.lastKnownFreeHeap = ESP.getFreeHeap();
    delay(50);
}
