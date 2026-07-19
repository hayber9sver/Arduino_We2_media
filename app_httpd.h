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

#endif