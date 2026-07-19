#include "ST7735_XIAO.h"

// ST7735 command set (same values/init sequence as the reference Python driver)
#define ST_SWRESET 0x01
#define ST_SLPOUT  0x11
#define ST_INVOFF  0x20
#define ST_DISPON  0x29
#define ST_CASET   0x2A
#define ST_RASET   0x2B
#define ST_RAMWR   0x2C
#define ST_MADCTL  0x36
#define ST_COLMOD  0x3A
#define ST_FRMCTR1 0xB1
#define ST_FRMCTR2 0xB2
#define ST_FRMCTR3 0xB3
#define ST_INVCTR  0xB4
#define ST_PWCTR1  0xC0
#define ST_PWCTR2  0xC1
#define ST_PWCTR3  0xC2
#define ST_PWCTR4  0xC3
#define ST_PWCTR5  0xC4
#define ST_VMCTR1  0xC5
#define ST_GMCTRP1 0xE0
#define ST_GMCTRN1 0xE1

#define MADCTL_MX  0x40
#define MADCTL_MY  0x80
#define MADCTL_MV  0x20
#define MADCTL_BGR 0x08  // this panel's color order is BGR, confirmed on the Python driver

ST7735_XIAO::ST7735_XIAO(int8_t cs, int8_t dc, int8_t rst, int16_t w, int16_t h)
    : Adafruit_GFX(w, h), _cs(cs), _dc(dc), _rst(rst),
      _spiSettings(4000000, MSBFIRST, SPI_MODE0) {}

void ST7735_XIAO::begin(uint32_t spiHz, int8_t sck, int8_t mosi, int8_t miso) {
    _spiSettings = SPISettings(spiHz, MSBFIRST, SPI_MODE0);

    pinMode(_cs, OUTPUT);
    pinMode(_dc, OUTPUT);
    pinMode(_rst, OUTPUT);
    csHigh();

    // SS is always -1: CS is toggled manually so it can be any GPIO.
    if (sck >= 0 && mosi >= 0) {
        SPI.begin(sck, miso, mosi, -1);
    } else {
        SPI.begin();
    }

    hwReset();
    initPanel();
    setRotation(0);
}

void ST7735_XIAO::hwReset() {
    digitalWrite(_rst, HIGH);
    delay(20);
    digitalWrite(_rst, LOW);
    delay(20);
    digitalWrite(_rst, HIGH);
    delay(150);
}

void ST7735_XIAO::writeCommand(uint8_t cmd) {
    SPI.beginTransaction(_spiSettings);
    csLow();
    dcLow();
    SPI.transfer(cmd);
    csHigh();
    SPI.endTransaction();
}

void ST7735_XIAO::writeData(uint8_t data) {
    SPI.beginTransaction(_spiSettings);
    csLow();
    dcHigh();
    SPI.transfer(data);
    csHigh();
    SPI.endTransaction();
}

void ST7735_XIAO::setAddrWindow(int16_t x0, int16_t y0, int16_t x1, int16_t y1) {
    writeCommand(ST_CASET);
    writeData(0x00); writeData((uint8_t)(x0 + colstart));
    writeData(0x00); writeData((uint8_t)(x1 + colstart));
    writeCommand(ST_RASET);
    writeData(0x00); writeData((uint8_t)(y0 + rowstart));
    writeData(0x00); writeData((uint8_t)(y1 + rowstart));
    writeCommand(ST_RAMWR);
}

void ST7735_XIAO::initPanel() {
    writeCommand(ST_SWRESET);
    delay(150);
    writeCommand(ST_SLPOUT);
    delay(500);

    writeCommand(ST_FRMCTR1); writeData(0x01); writeData(0x2C); writeData(0x2D);
    writeCommand(ST_FRMCTR2); writeData(0x01); writeData(0x2C); writeData(0x2D);
    writeCommand(ST_FRMCTR3);
    writeData(0x01); writeData(0x2C); writeData(0x2D);
    writeData(0x01); writeData(0x2C); writeData(0x2D);
    writeCommand(ST_INVCTR); writeData(0x07);
    writeCommand(ST_PWCTR1); writeData(0xA2); writeData(0x02); writeData(0x84);
    writeCommand(ST_PWCTR2); writeData(0xC5);
    writeCommand(ST_PWCTR3); writeData(0x0A); writeData(0x00);
    writeCommand(ST_PWCTR4); writeData(0x8A); writeData(0x2A);
    writeCommand(ST_PWCTR5); writeData(0x8A); writeData(0xEE);
    writeCommand(ST_VMCTR1); writeData(0x0E);
    writeCommand(ST_INVOFF);
    writeCommand(ST_MADCTL); writeData(MADCTL_MX | MADCTL_MY | MADCTL_BGR);
    writeCommand(ST_COLMOD); writeData(0x05); // 16-bit/pixel RGB565

    static const uint8_t gmctrp1[] = {0x02, 0x1C, 0x07, 0x12, 0x37, 0x32, 0x29, 0x2D,
                                       0x29, 0x25, 0x2B, 0x39, 0x00, 0x01, 0x03, 0x10};
    writeCommand(ST_GMCTRP1);
    for (uint8_t v : gmctrp1) writeData(v);

    static const uint8_t gmctrn1[] = {0x03, 0x1D, 0x07, 0x06, 0x2E, 0x2C, 0x29, 0x2D,
                                       0x2E, 0x2E, 0x37, 0x3F, 0x00, 0x00, 0x02, 0x10};
    writeCommand(ST_GMCTRN1);
    for (uint8_t v : gmctrn1) writeData(v);

    writeCommand(0x13); // NORON
    delay(10);
    writeCommand(ST_DISPON);
    delay(100);
}

void ST7735_XIAO::setRotation(uint8_t r) {
    Adafruit_GFX::setRotation(r);

    uint8_t madctl;
    switch (r & 3) {
        case 0:  madctl = MADCTL_MX | MADCTL_MY | MADCTL_BGR; break;
        case 1:  madctl = MADCTL_MY | MADCTL_MV | MADCTL_BGR; break;
        case 2:  madctl = MADCTL_BGR; break;
        default: madctl = MADCTL_MX | MADCTL_MV | MADCTL_BGR; break;
    }
    writeCommand(ST_MADCTL);
    writeData(madctl);
}

void ST7735_XIAO::drawPixel(int16_t x, int16_t y, uint16_t color) {
    if (x < 0 || x >= _width || y < 0 || y >= _height) return;

    setAddrWindow(x, y, x, y);
    SPI.beginTransaction(_spiSettings);
    csLow();
    dcHigh();
    SPI.transfer(color >> 8);
    SPI.transfer(color & 0xFF);
    csHigh();
    SPI.endTransaction();
}

void ST7735_XIAO::fillRect(int16_t x, int16_t y, int16_t w, int16_t h, uint16_t color) {
    if (x < 0) { w += x; x = 0; }
    if (y < 0) { h += y; y = 0; }
    if (x + w > _width)  w = _width  - x;
    if (y + h > _height) h = _height - y;
    if (w <= 0 || h <= 0) return;

    setAddrWindow(x, y, x + w - 1, y + h - 1);

    uint8_t buf[128]; // 64 pixels per chunk
    uint8_t hi = color >> 8, lo = color & 0xFF;
    for (int i = 0; i < 128; i += 2) { buf[i] = hi; buf[i + 1] = lo; }

    uint32_t total = (uint32_t)w * (uint32_t)h;
    SPI.beginTransaction(_spiSettings);
    csLow();
    dcHigh();
    while (total > 0) {
        uint32_t n = total < 64 ? total : 64;
        SPI.writeBytes(buf, n * 2);
        total -= n;
    }
    csHigh();
    SPI.endTransaction();
}

void ST7735_XIAO::drawText(int16_t x, int16_t y, const char *text, uint16_t color,
                            uint8_t size, uint16_t bg, bool opaque) {
    setCursor(x, y);
    setTextSize(size);
    if (opaque) setTextColor(color, bg);
    else setTextColor(color);
    print(text);
}

uint16_t ST7735_XIAO::color565(uint8_t r, uint8_t g, uint8_t b) {
    return ((uint16_t)(r & 0xF8) << 8) | ((uint16_t)(g & 0xFC) << 3) | (b >> 3);
}
