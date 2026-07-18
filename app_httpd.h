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

#endif