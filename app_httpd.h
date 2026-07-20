#ifndef APP_HTTPD_H
#define APP_HTTPD_H

enum Proto {
    PROTO_UART = 1,
    PROTO_I2C  = 2,
    PROTO_SPI  = 3,
};

// 2026-07-17: additional I2C command channel used alongside PROTO_UART -
// see app_httpd.cpp's own comment above its definition.
void initI2CCommandChannel();

// 2026-07-19: PCA9685 (same I2C bus as initI2CCommandChannel() above) ->
// SG92R servo on channel 0. Must be called AFTER initI2CCommandChannel(),
// which owns Wire.begin() for this bus - see app_httpd.cpp's own comment
// above its definition.
void initServoMotor();

// 2026-07-20: call from loop() (any cadence is fine, e.g. every 5ms - this
// function self-paces via SERVO_RAMP_STEP_DELAY_MS and returns immediately
// as a no-op whenever there's nothing to do). Moves the servo one small step
// closer to whatever /motor/set last requested - see its own comment in
// app_httpd.cpp for why this exists instead of reusing rampServoTo().
void servoRampPoll();

// 2026-07-19: ST7735 128x128 SPI status display - shows WiFi IP and whether
// a /stream/data client is connected. Must be called AFTER startRemoteProxy()
// (shares DISPLAY_DC_PIN/D3 with AI.begin()'s one-shot WE2 reset pulse - see
// app_httpd.cpp's own comment above initDisplay()'s definition), and before
// displayShowIP()/displayShowClientStatus() are ever called (both silently
// no-op until initDisplay() has run).
void initDisplay();
void displayShowIP(const char* ip);
void displayShowClientStatus(bool connected);
// angle < 0 means "no PCA9685 responding" - shown as "--" instead of a
// number. See app_httpd.cpp's own comment above this function's definition.
void displayShowMotorAngle(int angle);

#endif