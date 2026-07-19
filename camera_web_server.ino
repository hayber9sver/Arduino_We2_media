#include <Seeed_Arduino_SSCMA.h>
#include <WiFi.h>

#include "app_httpd.h"

// ===========================
// Enter your WiFi credentials
// ===========================
const char* ssid     = "";
const char* password = "";

void initSharedBuffer();
void initStatInfo();

void startRemoteProxy(Proto);
void startCameraServer();

void loopRemoteProxy();

void setup() {
    /* 2026-07-12: loopTask (this task - runs loopRemoteProxy(), i.e. all
     * UART reading/parsing of WE2 traffic) is created by the Arduino core at
     * priority 1 (cores/esp32/main.cpp), while esp_http_server's worker
     * tasks (stream_result_handler()/stream_audio_handler(), serving
     * /stream/result and /stream/audio) run at HTTPD_DEFAULT_CONFIG()'s
     * default priority 5. FreeRTOS is strict-priority preemptive on this
     * single-core chip, so a busy httpd worker (no yield point while it has
     * data to send) can keep the CPU almost continuously, starving this task
     * of the time it needs to drain atSerial's RX buffer - the leading
     * explanation for measured AUDIO frame loss under concurrent
     * camera+audio load (see esp32_camera_web_server_bridge memory).
     * Raising this task above the httpd workers means UART draining always
     * preempts them instead of the other way around. NULL = calling task,
     * which is this one (setup() runs inside loopTask). */
    vTaskPrioritySet(NULL, tskIDLE_PRIORITY + 6);

    initSharedBuffer();
    initStatInfo();

    Serial.begin(115200);
    Serial.setDebugOutput(true);
    Serial.println();

    WiFi.begin(ssid, password);
    WiFi.setSleep(false);

    while (WiFi.status() != WL_CONNECTED) {
        delay(500);
        Serial.print(".");
    }
    Serial.println("");
    Serial.println("WiFi connected");

    startRemoteProxy(PROTO_UART);
    initI2CCommandChannel();
    initServoMotor();
    startCameraServer();

    Serial.print("Camera Ready! Use 'http://");
    Serial.print(WiFi.localIP());
    Serial.println("' to connect");
}

void loop() {
    loopRemoteProxy();
    delay(5);
}
