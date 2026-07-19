#pragma once

#include <Arduino.h>
#include <SPI.h>
#include <Adafruit_GFX.h>

// Minimal ST7735 128x128 SPI TFT driver for XIAO ESP32C3 (or any Arduino
// board with SPI.h + Adafruit_GFX). Ported from a Python/spidev driver
// written for an Orange Pi (wiringOP-Python).
//
// CS/DC/RST are driven as plain GPIO (not hardware-auto CS), so any pins can
// be used. SPI SCK/MOSI/MISO can either use the board's default hardware SPI
// pins or be remapped via begin().
//
// Text and shapes come from Adafruit_GFX (setCursor/setTextColor/print,
// drawRect/fillRect/drawCircle/drawLine/fillTriangle, etc.) built on top of
// this class's drawPixel()/fillRect() implementations.
class ST7735_XIAO : public Adafruit_GFX {
public:
    ST7735_XIAO(int8_t cs, int8_t dc, int8_t rst, int16_t w = 128, int16_t h = 128);

    // spiHz: SPI clock. sck/mosi/miso: pass -1 (default) to use the board's
    // default hardware SPI pins, or explicit GPIO numbers to remap them.
    void begin(uint32_t spiHz = 4000000, int8_t sck = -1, int8_t mosi = -1, int8_t miso = -1);

    void setRotation(uint8_t r) override;
    void drawPixel(int16_t x, int16_t y, uint16_t color) override;
    void fillRect(int16_t x, int16_t y, int16_t w, int16_t h, uint16_t color) override;

    // Convenience: draw a text string with a given color in one call.
    // If opaque is true, bg is used as the text's background color.
    void drawText(int16_t x, int16_t y, const char *text, uint16_t color,
                  uint8_t size = 1, uint16_t bg = 0x0000, bool opaque = false);

    static uint16_t color565(uint8_t r, uint8_t g, uint8_t b);

    // If the panel's image is shifted/clipped, adjust these RAM offsets
    // (common values: 0/0, 2/1, 2/3, 0/32) and call begin() again.
    uint8_t colstart = 2;
    uint8_t rowstart = 3;

private:
    int8_t _cs, _dc, _rst;
    SPISettings _spiSettings;

    void hwReset();
    void initPanel();
    void writeCommand(uint8_t cmd);
    void writeData(uint8_t data);
    void setAddrWindow(int16_t x0, int16_t y0, int16_t x1, int16_t y1);

    inline void csLow()  { digitalWrite(_cs, LOW); }
    inline void csHigh() { digitalWrite(_cs, HIGH); }
    inline void dcLow()  { digitalWrite(_dc, LOW); }
    inline void dcHigh() { digitalWrite(_dc, HIGH); }
};
