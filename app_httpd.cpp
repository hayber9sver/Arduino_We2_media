// Copyright 2015-2016 Espressif Systems (Shanghai) PTE LTD
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Modified by nullptr, Seeed Technology Inc (c) 2024
//

#include "app_httpd.h"

// 2026-07-11: ArduinoJson.h dropped - app_httpd.cpp itself no longer uses
// it directly (that was only the now-removed BYTETracker-based /stream/
// result post-processing), and Seeed_Arduino_SSCMA.h below already pulls
// it in transitively for the SSCMA class's own internal use.
#include <freertos/FreeRTOS.h>
#include <Seeed_Arduino_SSCMA.h>
#include <Wire.h>
#include <esp_http_server.h>
#include <freertos/semphr.h>
#include <mbedtls/base64.h>
#include <sdkconfig.h>

#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

#if defined(ARDUINO_ARCH_ESP32) && defined(CONFIG_ARDUHAL_ESP_LOG)
    #include <HardwareSerial.h>
    #include <esp32-hal-log.h>
#endif

#define RESULT_TIMEOUT_MS 3000
#define CMD_TIMEOUT_MS    3000

// 2026-07-11: was two branches (ESP32S3 vs everything else, selected via
// CONFIG_IDF_TARGET_ESP32S3) plus a BYTE_TRACKER_ENABLED switch that only
// ever turned on for the S3 branch. This sketch only targets the XIAO
// ESP32C3 in practice, so the S3 branch (and the BYTETracker-based /stream/
// result post-processing it alone enabled - see stream_result_handler(),
// and BYTETracker.cpp/STrack.cpp/kalmanFilter.cpp/lapjv.cpp/utils.cpp/
// dataType.h, all removed) were dead weight: still compiled into every
// build regardless of which branch was live, for zero runtime benefit on
// this board. Collapsed to just the values this board actually needs.
#warning "Server may not work properly due to resource constraints..."
// 2026-07-11: briefly bumped this to 16 to stop sendTaggedCommand()'s
// awaited REPLY from getting evicted by unrelated EVENT traffic - wrong
// call. INVOKE streaming pushes ~8KB JPEG frames through this same ring
// (proxyCallback() mallocs a full copy of every slot's payload), so 16
// slots can balloon to >120KB - well past this chip's ~86KB free heap.
// Confirmed on hardware: malloc started failing ("Failed to allocate
// resp copy...") mid-stream, and every command after that point timed
// out because proxyCallback() was silently dropping everything, not just
// evicting it. Back to 3; the actual eviction-race fix now lives in
// proxyCallback()/sendTaggedCommand() (s_awaited_tag) instead of trying
// to buy headroom with a bigger ring.
#define PTR_BUFFER_SIZE     3
#define COM_BUFFER_SIZE     (1024 * 32)
#define RSP_BUFFER_SIZE     (1024 * 32)
// 2026-07-11: was 32KB - stream_frame_handler() mallocs this whole
// buffer up front just to base64-decode into, and that on top of
// pushPBSlot()'s own ~8KB-per-slot copies (see PTR_BUFFER_SIZE's
// comment) blew this chip's ~55-86KB free heap, confirmed on hardware
// ("Failed to allocate jpeg buffer..." while INVOKE was streaming).
// This is meant for debug preview (confirming bbox overlay looks right,
// not full-res viewing), so shrink to the smallest AT+SENSOR opt_id
// (240x240) case - observed real frames at that resolution are well
// under this, 8KB leaves comfortable margin without the 32KB peak.
#define JPG_BUFFER_SIZE     (1024 * 8)
#define RST_BUFFER_SIZE     (1024 * 4)
#define QRY_BUFFER_SIZE     (1024 * 4)
#define CMD_BUFFER_SIZE     (1024 * 4)

// 2026-07-11: was "HTTPD%.8X@" (14 bytes: 5-byte literal prefix + 8 hex
// digits + '@'). Confirmed on hardware that WE2's UART1 receive path drops
// anything past the first 16 bytes of a burst (its DW_UART RX hardware FIFO
// is 16 bytes deep and out_transport.c currently polls it, not
// interrupt-driven - see out_transport.c/sscma_cam_mic.c's audio_task for
// the other half of this mitigation). The literal "HTTPD" added nothing to
// uniqueness (every tag started with it), so it's dropped entirely; 3 hex
// digits (0-0xFFF, masked from the tick count - see the masking at the
// snprintf call site in sendTaggedCommand()) wrap every ~4s, comfortably
// longer than RESULT_TIMEOUT_MS (3s) so a late/lost reply's tag can't get
// confused with a fresh request's. This shrinks the tag from 14 to 4 bytes,
// so short queries like "SENSOR?"/"ACTION?" now total 16 bytes or under and
// should no longer be truncated - longer bodies (e.g. "SENSOR=1,1,0", or
// anything relayed through /command's base64 passthrough) can still exceed
// 16 bytes on their own and remain at risk until the WE2 side gets a real
// interrupt/DMA-driven RX path.
#define CMD_TAG_FMT_STR "%.3X@"
#define CMD_TAG_SIZE    snprintf(NULL, 0, CMD_TAG_FMT_STR, 0)

#define MSG_IMAGE_KEY   "\"image\": "
#define MSG_COMMA_STR   ", "
#define MSG_QUOTE_STR   "\""
#define MSG_REPLY_STR   "\"type\": 0"
#define MSG_EVENT_STR   "\"type\": 1"
#define MSG_LOGGI_STR   "\"type\": 2"
#define MSG_TERMI_STR   "\r\n"

enum MsgType : uint16_t {
    MSG_TYPE_UNKNOWN = 0,
    MSG_TYPE_REPLY   = 0xff & (1 << 1),
    MSG_TYPE_EVENT   = 0xff & (1 << 2),
    MSG_TYPE_LOGGI   = 0xff & (1 << 3),
    // 2026-07-11: raw AT+ASAMPLE binary PCM frames (WE2's own 0xFF 'S' 'M'
    // 'B' framing, see fetchFramedMessages()) - this sketch doesn't decode
    // audio itself, it just relays the untouched frame bytes over WiFi via
    // stream_audio_handler() for a downstream platform to unpack.
    MSG_TYPE_AUDIO   = 0xff & (1 << 4),
};

#define CMD_SAMPLE_STR "SAMPLE"
#define CMD_INVOKE_STR "INVOKE"

enum CmdType : uint16_t {
    CMD_TYPE_UNKNOWN = 0,
    CMD_TYPE_SAMPLE  = 0xff00 & (1 << 8),
    CMD_TYPE_INVOKE  = 0xff00 & (2 << 8),
    CMD_TYPE_SENSOR  = 0xff00 & (3 << 8),
};

struct PtrBuffer {
    struct Slot {
        size_t   id   = 0;
        uint16_t type = 0;
        void*    data = NULL;
        size_t   size = 0;
        timeval  timestamp;
    };

    SemaphoreHandle_t                 mutex;
    std::deque<std::shared_ptr<Slot>> slots;
    volatile size_t                   id    = 1;
    const size_t                      limit = PTR_BUFFER_SIZE;
};

struct StatInfo {
    size_t            last_frame_id = 0;
    timeval           last_frame_timestamp;
    SemaphoreHandle_t mutex;
};

PtrBuffer PB;

/* 2026-07-11: guards PB's oldest-first eviction (see proxyCallback()) from
 * discarding the one REPLY/LOGGI slot an in-flight sendTaggedCommand() is
 * actually waiting for. Confirmed on hardware: while INVOKE/ASAMPLE keep
 * streaming EVENT frames into the same small ring (PTR_BUFFER_SIZE has to
 * stay small - see its own comment - continuous ~8KB JPEG frames make a
 * bigger ring blow the chip's ~86KB heap), a REPLY sitting behind even a
 * couple of those EVENT slots got evicted before sendTaggedCommand()'s 5ms
 * polling loop ever saw it, so AT+BREAK/AT+ASR kept timing out even though
 * WE2 had genuinely already replied. Single string because this sketch's
 * httpd server is single-threaded (esp_http_server serializes handlers on
 * one task per instance), so only one tag is ever actually in flight from
 * sendTaggedCommand() at a time - always read/written under PB.mutex, same
 * as the slots themselves. */
static char s_awaited_tag[32] = {0};
StatInfo  SI;
SSCMA     AI;

// 2026-07-10: hoisted from startRemoteProxy()'s PROTO_UART case (was a
// function-local static) so pollWe2Uart1Handshake() can also reach the same
// serial object - see that function's comment for why this exists at all.
//
// 2026-07-11: briefly suspected this shared hardware with the sketch's
// Serial console (both HardwareSerial(0)) and moved it to UART1 - wrong call.
// On XIAO ESP32C3 the board's only USB port is the SoC's native USB-CDC/JTAG
// peripheral (confirmed against the board's pinout diagram: D6/D7 are
// labelled "TXD 0"/"RXD 0", i.e. UART0 is deliberately broken out to header
// pins for external wiring, separate hardware from the native-USB console).
// Reverted to UART0/D6-D7 - see sendTaggedCommand()'s raw-byte debug log for
// the actual ongoing investigation into the malformed-command symptom.
#ifdef ESP32
static HardwareSerial atSerial(0);
#else
    #define atSerial Serial1
#endif
static bool s_uart_proto_active = false;
static bool s_we2_uart1_synced  = false;
/* sendTaggedCommand()'s consecutive-timeout counter - see its comment and
 * WE2_RESYNC_TIMEOUT_THRESHOLD below for why this exists. */
static uint32_t s_consecutive_at_timeouts = 0;
/* pollWe2Uart1Handshake()'s s_we2_uart1_synced latch is one-shot: once set,
 * nothing ever clears it except an ESP32 reboot. That's fine as long as WE2
 * and the ESP32 always reboot together, but if WE2 alone reboots/crashes
 * (its own out_transport.c resets its transport choice back to USB-console
 * and starts probing UART1 fresh, exactly like a cold boot), this ESP32
 * never re-arms pollWe2Uart1Handshake() to answer that fresh probe - it
 * already believes it's synced. Result: WE2 waits forever for an echo that
 * never comes, never switches its output to UART1, and every AT command
 * this sketch sends times out permanently until the ESP32 itself is also
 * rebooted. Confirmed on hardware 2026-07-10/11: only a *simultaneous*
 * reboot of both boards ever recovered a wedged link; either board
 * rebooting alone did not. This threshold - N consecutive AT reply
 * timeouts - is the trigger to assume that's what happened and re-arm the
 * probe handshake automatically instead of requiring a manual ESP32 reboot. */
#define WE2_RESYNC_TIMEOUT_THRESHOLD (2)

void initSharedBuffer() { PB.mutex = xSemaphoreCreateMutex(); }

void initStatInfo() {
    SI.mutex                        = xSemaphoreCreateMutex();
    TickType_t ticks                = xTaskGetTickCount();
    SI.last_frame_timestamp.tv_sec  = ticks / configTICK_RATE_HZ;
    SI.last_frame_timestamp.tv_usec = (ticks % configTICK_RATE_HZ) * 1e6 / configTICK_RATE_HZ;
}

void startRemoteProxy(Proto through = PROTO_UART) {
    switch (through) {
    case PROTO_UART: {
        // the esp32 arduino library may have a bug in setRxBufferSize
        // we cannot set the buffer size larger than uint16_t max value
        // a workaround is to modify uartBegin() in
        //     esp32/hardware/esp32/2.0.14/cores/esp32/esp32-hal-uart.c
        atSerial.setRxBufferSize(COM_BUFFER_SIZE);
        // Reverted 2026-07-11 back to the default SSCMA_UART_BAUD (921600) -
        // the real WE2 board only speaks 921600 on this link. (Was
        // temporarily forced to 115200 earlier this session to validate the
        // protocol logic against Orange Pi's ttyS5, which can't generate an
        // accurate 921600 clock - 24MHz source, no integer divisor lands
        // within ~15%+ of it.)
        AI.begin(&atSerial, D3);
        s_uart_proto_active = true;
        break;
    }
    case PROTO_I2C: {
        Wire.setBufferSize(COM_BUFFER_SIZE);
        Wire.begin();
        AI.begin(&Wire, D3);
        break;
    };
    case PROTO_SPI: {
        SPI.begin(SCK, MOSI, MISO, -1);
        AI.begin(&SPI, D1, D0, D3, 15000000);
        break;
    };
    default:
        assert(false && "Unknown proto...");
    }
}

inline uint16_t getMsgType(const char* resp, size_t len) {
    uint16_t type = MSG_TYPE_UNKNOWN;

    if (strnstr(resp, MSG_REPLY_STR, len) != NULL) {
        type |= MSG_TYPE_REPLY;
    } else if (strnstr(resp, MSG_EVENT_STR, len) != NULL) {
        type |= MSG_TYPE_EVENT;
    } else if (strnstr(resp, MSG_LOGGI_STR, len) != NULL) {
        type |= MSG_TYPE_LOGGI;
    } else {
        log_w("Unknown message type...");
    }

    return type;
}

inline uint16_t getCmdType(const char* resp, size_t len) {
    uint16_t type = CMD_TYPE_UNKNOWN;

    if (strnstr(resp, CMD_SAMPLE_STR, len) != NULL) {
        type |= CMD_TYPE_SAMPLE;
    } else if (strnstr(resp, CMD_INVOKE_STR, len) != NULL) {
        type |= CMD_TYPE_INVOKE;
    }

    return type;
}

/* 2026-07-11: extracted from proxyCallback() (straight extraction, no
 * behavior change for that caller) so pushAudioFrame() can share the same
 * malloc+copy+ring-buffer-with-eviction-protection logic instead of
 * duplicating it for raw AT+ASAMPLE binary frames. */
static void pushPBSlot(uint16_t type, const void* data, size_t len) {
    if (!len) {
        return;
    }

    timeval    timestamp;
    TickType_t ticks   = xTaskGetTickCount();
    timestamp.tv_sec   = ticks / configTICK_RATE_HZ;
    timestamp.tv_usec  = (ticks % configTICK_RATE_HZ) * 1e6 / configTICK_RATE_HZ;

    char* copy = (char*)malloc(len);
    if (copy == NULL) {
        log_e("pushPBSlot: failed to allocate copy...");
        return;
    }
    memcpy(copy, data, len);

    size_t           limit  = PB.limit;
    PtrBuffer::Slot* p_slot = (PtrBuffer::Slot*)malloc(sizeof(PtrBuffer::Slot));
    if (p_slot == NULL) {
        log_e("pushPBSlot: failed to allocate slot...");
        free(copy);
        return;
    }

    p_slot->id        = PB.id;
    p_slot->type      = type;
    p_slot->data      = copy;
    p_slot->size      = len;
    p_slot->timestamp = timestamp;

    size_t discarded = 0;
    xSemaphoreTake(PB.mutex, portMAX_DELAY);
    while (PB.slots.size() >= limit) {
        // don't evict the slot an in-flight sendTaggedCommand() is waiting
        // for - evict the next-oldest unprotected one instead. If literally
        // every slot is protected (shouldn't happen: only one tag is ever
        // awaited at a time), fall back to evicting the front anyway rather
        // than growing the ring unboundedly.
        auto victim = PB.slots.begin();
        if (s_awaited_tag[0] != '\0') {
            for (auto it = PB.slots.begin(); it != PB.slots.end(); ++it) {
                if (strnstr((const char*)(*it)->data, s_awaited_tag, (*it)->size) == NULL) {
                    victim = it;
                    break;
                }
            }
        }
        PB.slots.erase(victim);
        discarded += 1;
    }
    PB.slots.emplace_back(std::shared_ptr<PtrBuffer::Slot>(p_slot, [](PtrBuffer::Slot* p) {
        if (p == NULL) {
            return;
        }
        if (p->data != NULL) {
            free(p->data);
            p->data = NULL;
        }
        free(p);
    }));
    xSemaphoreGive(PB.mutex);
    PB.id += 1;

    if (discarded > 0) {
        log_i("Discarded %u old responses...", discarded);
    }

    log_i("Received %u bytes (type=0x%04x)...", len, type);
}

static void proxyCallback(const char* resp, size_t len) {
    if (!len) {
        log_i("Response is empty...");
        return;
    }

    uint16_t type = 0;
    type |= getMsgType(resp, len);
    if (type == MSG_TYPE_UNKNOWN) {
        log_w("proxyCallback: MSG_TYPE_UNKNOWN, raw=%.*s", (int)len, resp);
        return;
    }
    type |= getCmdType(resp, len);

    pushPBSlot(type, resp, len);
}

#define WE2_UART1_PROBE_BYTE (0x16) // matches PROBE_MARKER_BYTE in the WE2 app's out_transport.c

/* 2026-07-10: the WE2 firmware defaults to routing AT-command traffic over
 * its USB port and only switches over to the Grove/pin-header UART1 (what
 * this sketch is wired to when bridging over UART) once it detects a peer
 * that echoes back a single marker byte (0x16, ASCII SYN) it periodically
 * writes - see out_transport.c in the WE2 firmware (sscma_cam_mic app).
 * Seeed_Arduino_SSCMA has no idea about this handshake, so without this the
 * WE2 never reads anything this sketch sends and AI.write()/reads off
 * atSerial silently do nothing forever - confirmed on hardware (a python
 * script standing in for this sketch saw zero bytes on UART1 until it
 * implemented exactly this echo). Reads directly off atSerial, bypassing
 * fetchFramedMessages(), only until the first successful echo: before that
 * point the WE2 sends nothing else on this line (all real AT/JSON traffic
 * stays on its USB port until the switch happens), so there's no risk of
 * stealing bytes fetchFramedMessages() needed. Once synced this becomes a
 * no-op (the WE2 never probes again once switched, per its own
 * out_transport.c comment), handing the line over to fetchFramedMessages()
 * exclusively - no race between the two, since only one of them is ever
 * actually consuming bytes at a time. Not "forever" though: sendTaggedCommand()
 * clears s_we2_uart1_synced back to false after WE2_RESYNC_TIMEOUT_THRESHOLD
 * consecutive AT reply timeouts, re-arming this function - see its comment
 * for why (WE2 rebooting independently of the ESP32 needs exactly this to
 * recover without a manual ESP32 reboot).
 *
 * 2026-07-11: that resync-on-timeout only actually recovers the link if WE2
 * ALSO reboots and starts probing again - but WE2 only probes on ITS OWN
 * cold boot (per out_transport.c, "the WE2 never probes again once
 * switched"). If WE2 never went down at all (the timeouts were caused by
 * something else - e.g. a transient miss, or the sendTaggedCommand()
 * matching bug fixed the same day this comment was written), clearing
 * s_we2_uart1_synced here just deadlocks the link *permanently*: this
 * function goes back to waiting for a 0x16 echo that will never arrive
 * again, while WE2 keeps sending real replies that get silently discarded
 * one byte at a time as "unexpected" below - confirmed on hardware, every
 * single command timed out from that point on for the rest of the session.
 * Fix: treat ANY non-probe byte as proof the peer is alive and already
 * speaking the real protocol (not pre-handshake silence) and hand off to
 * fetchFramedMessages() immediately instead of waiting for an echo that may
 * never come. Only the one byte already consumed here is at risk of being
 * lost - fetchFramedMessages() already tolerates and resyncs past stray
 * unrecognized bytes on its own (see its own comment), so this is safe even
 * if what triggered it turns out to be a stray/garbage byte rather than a
 * real reply. */
static void pollWe2Uart1Handshake() {
    if (s_we2_uart1_synced || !s_uart_proto_active) {
        return;
    }
    while (atSerial.available()) {
        int c = atSerial.read();
        if (c == WE2_UART1_PROBE_BYTE) {
            atSerial.write((uint8_t)WE2_UART1_PROBE_BYTE);
            atSerial.flush();
            s_we2_uart1_synced = true;
            log_i("WE2 UART1 handshake echoed - output should switch over shortly");
            return;
        }
        log_w("Unexpected byte 0x%02x while waiting for WE2 UART1 handshake - treating as evidence "
              "WE2 is already live and handing off to fetchFramedMessages()...", (unsigned)c);
        s_we2_uart1_synced = true;
        return;
    }
}

/* 2026-07-11: AT+ASAMPLE's audio chunks use a custom binary frame (WE2's
 * send_audio_binary_frame(): 4-byte magic 0xFF 'S' 'M' 'B', 4-byte
 * sample_rate, 4-byte payload length, 1-byte channels, 1-byte bits, 2
 * reserved bytes = 16-byte header, then that many bytes of raw PCM, then a
 * 2-byte CRC16 - see send_result.cpp), NOT the "\r{...}\n" JSON framing the
 * Seeed_Arduino_SSCMA library's own AI.fetch()/SSCMA::fetch() assumes for
 * everything on the wire. Confirmed on hardware: once ASAMPLE starts
 * streaming, raw PCM bytes routinely contain the two-byte sequences
 * AI.fetch() searches for ("\r{"/"}\n") by pure chance, desyncing its
 * internal rx_buf bookkeeping - every AT command sent afterwards (including
 * AT+BREAK meant to stop the stream) permanently stopped getting its reply
 * recognized, even though WE2 genuinely replied. Rather than patch the
 * shared library (would affect every other sketch using it), this sketch
 * bypasses AI.fetch() entirely and does its own minimal framing directly on
 * atSerial: recognize+skip whole binary audio frames as opaque bytes (this
 * sketch doesn't consume/play audio - control only, per explicit direction),
 * and hand everything else to the exact same "\r{...}\n" search AI.fetch()
 * used, so proxyCallback()'s parsing of the result is unchanged. */
#define AUDIO_FRAME_MAGIC0      (0xFF)
#define AUDIO_FRAME_MAGIC1      ('S')
#define AUDIO_FRAME_MAGIC2      ('M')
#define AUDIO_FRAME_MAGIC3      ('B')
#define AUDIO_FRAME_HEADER_LEN  (16)
#define AUDIO_FRAME_CRC_LEN     (2)
// generous cap on WE2's per-chunk PCM payload (see pdm_audio.c's chunking) -
// guards against treating a false/corrupted magic match as a real frame
// with a garbage length field and stalling forever waiting for bytes that
// will never come.
#define AUDIO_FRAME_MAX_PAYLOAD (1024 * 16)

#define FETCH_BUF_SIZE (1024 * 32)
static uint8_t s_fetch_buf[FETCH_BUF_SIZE];
static size_t  s_fetch_len = 0;

static void fetchFramedMessages(ResponseCallback cb) {
    while (s_fetch_len < FETCH_BUF_SIZE && atSerial.available()) {
        int c = atSerial.read();
        if (c < 0) {
            break;
        }
        s_fetch_buf[s_fetch_len++] = (uint8_t)c;
    }

    size_t pos = 0;
    while (pos < s_fetch_len) {
        size_t remain = s_fetch_len - pos;

        bool looks_like_audio = remain >= 4 && s_fetch_buf[pos] == AUDIO_FRAME_MAGIC0 &&
                                 s_fetch_buf[pos + 1] == AUDIO_FRAME_MAGIC1 &&
                                 s_fetch_buf[pos + 2] == AUDIO_FRAME_MAGIC2 &&
                                 s_fetch_buf[pos + 3] == AUDIO_FRAME_MAGIC3;
        if (looks_like_audio) {
            if (remain < AUDIO_FRAME_HEADER_LEN) {
                break;  // header itself not fully buffered yet
            }
            uint32_t payload_len;
            memcpy(&payload_len, &s_fetch_buf[pos + 8], sizeof(payload_len));
            if (payload_len > AUDIO_FRAME_MAX_PAYLOAD) {
                // implausible length - almost certainly a false positive
                // (e.g. these 4 bytes happened to appear inside a JSON
                // string), not a real audio frame. Fall through and treat
                // this byte as ordinary/unexpected instead of trusting it.
            } else {
                size_t frame_total = AUDIO_FRAME_HEADER_LEN + payload_len + AUDIO_FRAME_CRC_LEN;
                if (remain < frame_total) {
                    break;  // rest of the frame hasn't arrived yet
                }
                // Untouched relay, not decoded here - see MSG_TYPE_AUDIO's
                // comment and stream_audio_handler().
                pushPBSlot(MSG_TYPE_AUDIO, &s_fetch_buf[pos], frame_total);
                pos += frame_total;
                continue;
            }
        }

        if (remain >= RESPONSE_PREFIX_LEN && memcmp(&s_fetch_buf[pos], RESPONSE_PREFIX, RESPONSE_PREFIX_LEN) == 0) {
            size_t search_from = pos + RESPONSE_PREFIX_LEN;
            size_t suffix_at    = 0;
            bool   found        = false;
            for (size_t i = search_from; i + RESPONSE_SUFFIX_LEN <= s_fetch_len; ++i) {
                if (memcmp(&s_fetch_buf[i], RESPONSE_SUFFIX, RESPONSE_SUFFIX_LEN) == 0) {
                    suffix_at = i;
                    found     = true;
                    break;
                }
            }
            if (!found) {
                break;  // message not complete yet
            }
            size_t msg_len = (suffix_at + RESPONSE_SUFFIX_LEN) - pos;
            if (cb) {
                cb((const char*)&s_fetch_buf[pos], msg_len);
            }
            pos += msg_len;
            continue;
        }

        // Not a recognized frame start - drop one byte and try to resync
        // rather than get stuck (matches AI.fetch()'s own discard-and-
        // continue behavior for stray bytes).
        pos += 1;
    }

    if (pos > 0) {
        memmove(s_fetch_buf, s_fetch_buf + pos, s_fetch_len - pos);
        s_fetch_len -= pos;
    }
}

void loopRemoteProxy() {
    pollWe2Uart1Handshake();
    fetchFramedMessages(proxyCallback);
}

typedef struct {
    httpd_req_t* req;
    size_t       len;
} jpg_chunking_t;

#define PART_BOUNDARY "123456789000000000000987654321"
static const char* _STREAM_CONTENT_TYPE = "multipart/x-mixed-replace;boundary=" PART_BOUNDARY;
static const char* _STREAM_BOUNDARY     = "\r\n--" PART_BOUNDARY "\r\n";
static const char* _STREAM_PART = "Content-Type: image/jpeg\r\nContent-Length: %u\r\nX-Timestamp: %d.%06d\r\n\r\n";

httpd_handle_t web_httpd    = NULL;
httpd_handle_t stream_httpd = NULL;

typedef struct {
    size_t size;   //number of values used for filtering
    size_t index;  //current value index
    size_t count;  //value count
    int    sum;
    int*   values;  //array to be filled with values
} ra_filter_t;

static ra_filter_t ra_filter;

static ra_filter_t* ra_filter_init(ra_filter_t* filter, size_t sample_size) {
    memset(filter, 0, sizeof(ra_filter_t));

    filter->values = (int*)malloc(sample_size * sizeof(int));
    if (!filter->values) {
        return NULL;
    }
    memset(filter->values, 0, sample_size * sizeof(int));

    filter->size = sample_size;
    return filter;
}

#if ARDUHAL_LOG_LEVEL >= ARDUHAL_LOG_LEVEL_INFO
static int ra_filter_run(ra_filter_t* filter, int value) {
    if (!filter->values) {
        return value;
    }
    filter->sum -= filter->values[filter->index];
    filter->values[filter->index] = value;
    filter->sum += filter->values[filter->index];
    filter->index++;
    filter->index = filter->index % filter->size;
    if (filter->count < filter->size) {
        filter->count++;
    }
    return filter->sum / filter->count;
}
#endif

static esp_err_t results_handler(httpd_req_t* req) {
    esp_err_t     res     = ESP_OK;
    static size_t last_id = 0;
    static char*  hdr_buf[128];
    static char*  rst_buf = NULL;
    if (rst_buf == NULL) {
        rst_buf = (char*)malloc(RST_BUFFER_SIZE);
        if (rst_buf == NULL) {
            log_e("Failed to allocate results buffer...");
            httpd_resp_send_500(req);
            return ESP_ERR_NO_MEM;
        }
    }

    std::shared_ptr<PtrBuffer::Slot> slot = nullptr;

    TickType_t time_begin = xTaskGetTickCount();
    while ((xTaskGetTickCount() - time_begin) < RESULT_TIMEOUT_MS) {
        xSemaphoreTake(PB.mutex, portMAX_DELAY);
        auto slots = PB.slots;
        xSemaphoreGive(PB.mutex);

        for (auto it = slots.rbegin(); it != slots.rend(); ++it) {
            if (it->get()->id <= last_id) {
                break;
            }
            if (it->get()->type == (MSG_TYPE_EVENT | CMD_TYPE_SAMPLE) ||
                it->get()->type == (MSG_TYPE_EVENT | CMD_TYPE_INVOKE)) {
                slot    = *it;
                last_id = slot->id;
                break;
            }
        }

        if (!slot) {
            vTaskDelay(5 / portTICK_PERIOD_MS);
            continue;
        }

        break;
    }

    if (slot == nullptr) {
        log_w("Find newer results slot timeout...");
        httpd_resp_send_500(req);
        return ESP_OK;
    }

    const char* img_head = strnstr((const char*)slot->data, MSG_IMAGE_KEY MSG_QUOTE_STR, slot->size);
    if (img_head != NULL) {
        size_t offset = (img_head - (const char*)slot->data) + strlen(MSG_IMAGE_KEY MSG_QUOTE_STR);

        const char* img_tail = strnstr((const char*)slot->data + offset, MSG_QUOTE_STR, slot->size - offset);
        if (img_tail == NULL) {
            log_e("Broken json format...");
            httpd_resp_send_500(req);
            return ESP_OK;
        }

        /* 2026-07-11: fixed - remove_end used to stay pointed AT the image
         * value's closing quote itself instead of past it whenever a
         * leading ", " was found (the common case: "image" is always the
         * last field in practice, so "boxes": [...], "image": "..." always
         * takes this branch). The splice below kept that quote character,
         * producing invalid JSON like "boxes": []"}} - a stray '"' right
         * after the array close - which is exactly what broke every
         * consumer that actually parses this endpoint instead of just
         * displaying it raw (found via media_client.py's /result reader
         * failing with "Expecting ',' delimiter" on every single poll). */
        const char* remove_end   = img_tail + strlen(MSG_QUOTE_STR);
        const char* remove_start = img_head;

        const char* prefix_comma     = img_head - strlen(MSG_COMMA_STR);
        bool        has_prefix_comma = prefix_comma >= (const char*)slot->data &&
                                 strncmp(prefix_comma, MSG_COMMA_STR, strlen(MSG_COMMA_STR)) == 0;
        if (has_prefix_comma) {
            remove_start = prefix_comma;
        } else if (strncmp(remove_end, MSG_COMMA_STR, strlen(MSG_COMMA_STR)) == 0) {
            /* "image" was the first field instead - eat the trailing ", "
             * that introduces the next one, so what's left isn't left
             * starting with a stray leading comma. */
            remove_end += strlen(MSG_COMMA_STR);
        }

        if (slot->size - (remove_end - remove_start) >= RST_BUFFER_SIZE) {
            log_e("Results buffer is not enough...");
            httpd_resp_send_500(req);
            return ESP_OK;
        }
        memset(rst_buf, 0, RST_BUFFER_SIZE);
        size_t size   = remove_start - (const char*)slot->data;
        size_t copied = 0;
        strncpy(rst_buf, (const char*)slot->data, size);
        copied += size;
        size = ((const char*)slot->data + slot->size) - remove_end;
        strncpy(rst_buf + copied, remove_end, size);
    }

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");

    char ts[32] = {0};
    snprintf(ts, sizeof(ts), "%ld", slot->id);
    httpd_resp_set_hdr(req, "X-Id", (const char*)ts);

    memset(ts, 0, sizeof(ts));
    snprintf(ts, sizeof(ts), "%ld.%06ld", slot->timestamp.tv_sec, slot->timestamp.tv_usec);
    httpd_resp_set_hdr(req, "X-Timestamp", (const char*)ts);

    size_t  last_frame_id;
    timeval last_frame_timestamp;

    xSemaphoreTake(SI.mutex, portMAX_DELAY);
    last_frame_id        = SI.last_frame_id;
    last_frame_timestamp = SI.last_frame_timestamp;
    xSemaphoreGive(SI.mutex);

    memset(ts, 0, sizeof(ts));
    snprintf(ts, sizeof(ts), "%ld", last_frame_id);
    httpd_resp_set_hdr(req, "X-Last-Frame-Id", (const char*)ts);

    memset(ts, 0, sizeof(ts));
    snprintf(ts, sizeof(ts), "%ld.%06ld", last_frame_timestamp.tv_sec, last_frame_timestamp.tv_usec);
    httpd_resp_set_hdr(req, "X-Last-Frame-Timestamp", (const char*)ts);

    res = httpd_resp_send(req, (const char*)rst_buf, strlen(rst_buf));
    if (res != ESP_OK) {
        log_e("Send results failed...");
    }

    return res;
}

/* Forward declaration - defined after sendTaggedCommand(), which it wraps;
 * stream_frame_handler()/stream_audio_handler() below need it earlier in
 * the file than that definition lives. */
static void sendBreakBestEffort();

static esp_err_t stream_frame_handler(httpd_req_t* req) {
    esp_err_t res = ESP_OK;
    char*     part_buf[128];
    char*     jpeg_buf = NULL;
    size_t    last_id  = 0;

    res = httpd_resp_set_type(req, _STREAM_CONTENT_TYPE);
    if (res != ESP_OK) {
        return res;
    }

    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    httpd_resp_set_hdr(req, "X-Framerate", "60");

    jpeg_buf = (char*)malloc(JPG_BUFFER_SIZE);
    if (jpeg_buf == NULL) {
        log_e("Failed to allocate jpeg buffer...");
        return ESP_ERR_NO_MEM;
    }

    while (true) {
        std::shared_ptr<PtrBuffer::Slot> slot = nullptr;

        {
            xSemaphoreTake(PB.mutex, portMAX_DELAY);
            auto slots = PB.slots;
            xSemaphoreGive(PB.mutex);

            for (auto it = slots.rbegin(); it != slots.rend(); ++it) {
                if (it->get()->id <= last_id) {
                    break;
                }
                if (it->get()->type == (MSG_TYPE_EVENT | CMD_TYPE_SAMPLE) ||
                    it->get()->type == (MSG_TYPE_EVENT | CMD_TYPE_INVOKE)) {
                    slot    = *it;
                    last_id = slot->id;
                    break;
                }
            }

            if (!slot) {
                vTaskDelay(5 / portTICK_PERIOD_MS);
                continue;
            }
        }

        const char* slice = strnstr((const char*)slot->data, MSG_IMAGE_KEY MSG_QUOTE_STR, slot->size);
        if (slice == NULL) {
            log_w("No image data found...");
            vTaskDelay(5 / portTICK_PERIOD_MS);
            continue;
        }
        size_t      offset = (slice - (const char*)slot->data) + strlen(MSG_IMAGE_KEY MSG_QUOTE_STR);
        const char* data   = (const char*)slot->data + offset;
        const char* quote  = strnstr(data, MSG_QUOTE_STR, slot->size - offset);
        if (quote == NULL) {
            log_w("Invalid image data size...");
            vTaskDelay(5 / portTICK_PERIOD_MS);
            continue;
        }
        size_t len = quote - data;
        if (len == 0) {
            log_w("Empty image data...");
            vTaskDelay(5 / portTICK_PERIOD_MS);
            continue;
        }

        size_t jpeg_size = 0;
        memset(jpeg_buf, 0, JPG_BUFFER_SIZE);
        int decode_ret = mbedtls_base64_decode((unsigned char*)jpeg_buf, JPG_BUFFER_SIZE, &jpeg_size,
                                                (const unsigned char*)data, len);
        if (decode_ret != 0) {
            // Most likely cause on this board: the frame's decoded size
            // exceeds JPG_BUFFER_SIZE (8KB - sized for the 240x240 debug
            // preview case, see that macro's own comment) - e.g. if
            // AT+SENSOR is currently set to 480x480/640x480 instead.
            log_e("Failed to decode image data (ret=%d, b64_len=%u)", decode_ret, (unsigned)len);
            vTaskDelay(5 / portTICK_PERIOD_MS);
            continue;
        }

        xSemaphoreTake(SI.mutex, portMAX_DELAY);
        SI.last_frame_id        = slot->id;
        SI.last_frame_timestamp = slot->timestamp;
        xSemaphoreGive(SI.mutex);

        res = httpd_resp_send_chunk(req, _STREAM_BOUNDARY, strlen(_STREAM_BOUNDARY));
        if (res != ESP_OK) {
            goto SendError;
        }

        {
            memset(part_buf, 0, sizeof(part_buf));
            size_t hlen = snprintf((char*)part_buf,
                                   sizeof(part_buf),
                                   _STREAM_PART,
                                   jpeg_size,
                                   slot->timestamp.tv_sec,
                                   slot->timestamp.tv_usec);
            res         = httpd_resp_send_chunk(req, (const char*)part_buf, hlen);
        }
        if (res != ESP_OK) {
            goto SendError;
        }

        res = httpd_resp_send_chunk(req, jpeg_buf, jpeg_size);
        if (res != ESP_OK) {
            goto SendError;
        }

        continue;

    SendError:
        log_e("Send frame failed - client disconnected, stopping WE2 stream...");
        sendBreakBestEffort();
        break;
    }

    free(jpeg_buf);

    return res;
}

/* 2026-07-11: relays raw AT+ASAMPLE binary frames (see MSG_TYPE_AUDIO's and
 * fetchFramedMessages()'s comments) to whatever's connected here, untouched
 * - this sketch doesn't decode PCM/WAV itself, a downstream platform does.
 * Plain application/octet-stream chunked body (no multipart boundary like
 * stream_frame_handler's MJPEG - each WE2 binary frame already carries its
 * own self-describing header/length, a client just needs to re-run the same
 * framing this sketch itself parses in fetchFramedMessages()). Unlike
 * stream_frame_handler (which only cares about the latest JPEG and is happy
 * to skip stale ones), this sends every not-yet-sent AUDIO slot in order -
 * dropping PCM chunks would leave audible gaps for whatever decodes this
 * downstream, not just a stale frame. */
static esp_err_t stream_audio_handler(httpd_req_t* req) {
    esp_err_t res     = ESP_OK;
    size_t    last_id = 0;

    res = httpd_resp_set_type(req, "application/octet-stream");
    if (res != ESP_OK) {
        return res;
    }
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");

    while (true) {
        std::vector<std::shared_ptr<PtrBuffer::Slot>> pending;
        {
            xSemaphoreTake(PB.mutex, portMAX_DELAY);
            for (auto& s : PB.slots) {
                if (s->id > last_id && s->type == MSG_TYPE_AUDIO) {
                    pending.push_back(s);
                }
            }
            xSemaphoreGive(PB.mutex);
        }

        if (pending.empty()) {
            vTaskDelay(5 / portTICK_PERIOD_MS);
            continue;
        }

        bool send_failed = false;
        for (auto& slot : pending) {
            last_id = slot->id;
            res     = httpd_resp_send_chunk(req, (const char*)slot->data, slot->size);
            if (res != ESP_OK) {
                send_failed = true;
                break;
            }
        }
        if (send_failed) {
            log_e("Send audio frame failed - client disconnected, stopping WE2 stream...");
            sendBreakBestEffort();
            break;
        }
    }

    return res;
}

static esp_err_t stream_result_handler(httpd_req_t* req) {
    esp_err_t     res     = ESP_OK;
    static size_t last_id = 0;

    res |= httpd_resp_set_status(req, HTTPD_200);
    res |= httpd_resp_set_type(req, "application/json");
    res |= httpd_resp_set_hdr(req, "Connection", "keep-alive");
    res |= httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    if (res != ESP_OK) {
        log_e("Failed to set response headers...");
        return res;
    }

    while (res == ESP_OK) {
        std::shared_ptr<PtrBuffer::Slot> slot = nullptr;

        xSemaphoreTake(PB.mutex, portMAX_DELAY);
        auto slots = PB.slots;
        xSemaphoreGive(PB.mutex);

        for (auto it = slots.rbegin(); it != slots.rend(); ++it) {
            if (it->get()->id <= last_id) {
                break;
            }
            if (it->get()->type == (MSG_TYPE_EVENT | CMD_TYPE_SAMPLE) ||
                it->get()->type == (MSG_TYPE_EVENT | CMD_TYPE_INVOKE)) {
                slot    = *it;
                last_id = slot->id;
                break;
            }
        }

        if (!slot) {
            vTaskDelay(5 / portTICK_PERIOD_MS);
            continue;
        }

        switch (slot->type) {
        case MSG_TYPE_EVENT | CMD_TYPE_SAMPLE: {
            res |= httpd_resp_send_chunk(req, (const char*)slot->data, slot->size);
            res |= httpd_resp_send_chunk(req, MSG_TERMI_STR, strlen(MSG_TERMI_STR));
            break;
        }

        case MSG_TYPE_EVENT | CMD_TYPE_INVOKE: {
            res |= httpd_resp_send_chunk(req, (const char*)slot->data, slot->size);
            res |= httpd_resp_send_chunk(req, MSG_TERMI_STR, strlen(MSG_TERMI_STR));
            break;
        }

        default:;
        }

        if (res != ESP_OK) {
            log_e("Send results failed...");
            break;
        }
    }

    return res;
}

static esp_err_t parse_get(httpd_req_t* req, char** obuf) {
    char*  buf     = NULL;
    size_t buf_len = 0;

    buf_len = httpd_req_get_url_query_len(req) + 1;
    if (buf_len > 1) {
        buf = (char*)malloc(buf_len);
        if (!buf) {
            httpd_resp_send_500(req);
            return ESP_FAIL;
        }
        if (httpd_req_get_url_query_str(req, buf, buf_len) == ESP_OK) {
            *obuf = buf;
            return ESP_OK;
        }
        free(buf);
    }
    httpd_resp_send_404(req);
    return ESP_FAIL;
}

/* 2026-07-10: extracted from what used to be all of command_handler() below
 * (straight extraction, no behavior change for that caller) so the new
 * /camera/* and /audio/* endpoints can drive the WE2 the same way instead
 * of only being reachable via the browser hand-assembling raw AT strings
 * through command_handler's generic base64 passthrough. Sends
 * "AT+<tag>@<body>\r\n" and polls the shared PtrBuffer for the reply
 * carrying that tag - tagging matters here same as it always did for
 * command_handler: multiple requests can be in flight concurrently (e.g.
 * this sketch's own /camera/start issuing SENSOR then INVOKE back to back,
 * or two browser tabs), so untagged replies couldn't be told apart. Returns
 * nullptr on timeout. cmd_tag_buf (caller-owned, >=32 bytes) is filled in
 * with the generated tag either way, for the caller's own diagnostics. */
static std::shared_ptr<PtrBuffer::Slot> sendTaggedCommand(const char* body, size_t body_len, char* cmd_tag_buf,
                                                            size_t cmd_tag_buf_size,
                                                            uint32_t timeout_ms = RESULT_TIMEOUT_MS) {
    TickType_t ticks        = xTaskGetTickCount();
    // masked to 12 bits (3 hex digits) - see CMD_TAG_FMT_STR's comment for
    // why: %.3X is a minimum width, not a truncation, so the raw tick count
    // must be masked down first or it'd print all its digits and defeat the
    // whole point of shrinking this.
    size_t cmd_tag_size = snprintf(cmd_tag_buf, cmd_tag_buf_size, CMD_TAG_FMT_STR, (unsigned)(ticks & 0xFFFu));

    size_t last_id = PB.id;

    xSemaphoreTake(PB.mutex, portMAX_DELAY);
    strncpy(s_awaited_tag, cmd_tag_buf, sizeof(s_awaited_tag) - 1);
    s_awaited_tag[sizeof(s_awaited_tag) - 1] = '\0';
    xSemaphoreGive(PB.mutex);

    AI.write(CMD_PREFIX, strlen(CMD_PREFIX));
    AI.write(cmd_tag_buf, cmd_tag_size);
    AI.write(body, body_len);
    AI.write(CMD_SUFFIX, strlen(CMD_SUFFIX));

    TickType_t time_begin = xTaskGetTickCount();
    while ((xTaskGetTickCount() - time_begin) < (timeout_ms / portTICK_PERIOD_MS)) {
        vTaskDelay(5 / portTICK_PERIOD_MS);

        xSemaphoreTake(PB.mutex, portMAX_DELAY);
        auto slots = PB.slots;
        xSemaphoreGive(PB.mutex);

        // 2026-07-11: was `p->id - last_id <= 0` - both are size_t (unsigned),
        // so that subtraction never goes negative; it silently wraps to a huge
        // value instead, breaking the "already examined" filter for any slot
        // older than last_id. Worse, the old code also reassigned `last_id =
        // p->id` for every *non-matching* slot regardless of whether it was
        // actually newer - including stale replies from a *previous*
        // sendTaggedCommand() call still sitting in the (3-slot) ring, which
        // could drag last_id backward/sideways and mask the genuinely fresh,
        // correctly-tagged reply examined later in the very same scan.
        // Confirmed on hardware: a reply could be pushed and correctly
        // classified as MSG_TYPE_REPLY (visible in pushPBSlot()'s own log)
        // and this loop would still never find it, timing out every time.
        // A plain unsigned `<=` with no mutation is both correct and simpler
        // - PTR_BUFFER_SIZE is only 3, so re-scanning the whole ring every
        // 5ms costs nothing.
        auto it = std::find_if(slots.begin(), slots.end(), [&](std::shared_ptr<PtrBuffer::Slot> p) {
            // pushPBSlot() assigns p_slot->id = PB.id *before* incrementing
            // PB.id (see its own code) - so the very next slot pushed after
            // `last_id = PB.id` was captured above gets an id exactly equal
            // to last_id, not greater. `<=` here would filter out precisely
            // the reply this call is waiting for - confirmed on hardware:
            // pushPBSlot() logged a correctly-classified REPLY landing, and
            // this still timed out every single time until changed to `<`.
            if (p->id < last_id) {
                return false;
            }
            if (p->type & MSG_TYPE_REPLY || p->type & MSG_TYPE_LOGGI) {
                const char* tag = strnstr((const char*)p->data, cmd_tag_buf, p->size);
                if (tag != NULL) {
                    return true;
                }
            }
            return false;
        });
        if (it == slots.end()) {
            continue;
        }

        xSemaphoreTake(PB.mutex, portMAX_DELAY);
        s_awaited_tag[0] = '\0';
        xSemaphoreGive(PB.mutex);
        s_consecutive_at_timeouts = 0;
        return *it;
    }

    xSemaphoreTake(PB.mutex, portMAX_DELAY);
    s_awaited_tag[0] = '\0';
    xSemaphoreGive(PB.mutex);

    if (++s_consecutive_at_timeouts >= WE2_RESYNC_TIMEOUT_THRESHOLD) {
        log_w("sendTaggedCommand: %u consecutive AT reply timeouts - assuming WE2 "
              "rebooted independently and lost UART1 sync, re-arming probe handshake...",
              (unsigned)s_consecutive_at_timeouts);
        s_we2_uart1_synced        = false;
        s_consecutive_at_timeouts = 0;
        // Whatever's sitting in fetchFramedMessages()'s buffer is now stale
        // (mid-parse of a reply that's never coming) - drop it so a dangling
        // partial pattern can't delay recognizing real traffic once WE2
        // actually reconnects and pollWe2Uart1Handshake() hands the line back.
        s_fetch_len = 0;
    }
    return nullptr;
}

/* 2026-07-11: called from stream_frame_handler()/stream_audio_handler() the
 * moment httpd_resp_send_chunk() first fails - that only happens once the
 * client's TCP connection is actually gone, so this is the "client
 * disconnected unexpectedly, self-close the WE2 stream" behavior (per
 * explicit direction: no browser UI to click a stop button anymore, so
 * nothing else would ever tell WE2 to stop if a client just vanishes).
 * Best-effort and short timeout - the caller is already tearing down its own
 * connection either way, this shouldn't hold that up waiting for a reply
 * that doesn't matter to it. Same AT+BREAK-stops-both caveat as camera_stop_
 * handler()/audio_stop_handler(): if a *different* client still has the
 * other stream (camera vs audio) open, this stops that one too - there's no
 * separate per-stream stop in WE2's AT protocol. */
// 2026-07-11: defensive debounce - hardware testing showed something (root
// cause not yet confirmed) can drive stream_frame_handler()/stream_audio_
// handler() into calling this back-to-back at ~10-15ms intervals, and each
// call blocks its own task for up to 1s waiting on a reply, which was
// enough to make the whole board unresponsive (no serial output, HTTP
// dead, only the underlying WiFi/network stack still answering ICMP).
// Whatever the real trigger turns out to be, nothing legitimate needs
// AT+BREAK sent more than once every couple seconds, so cap it here rather
// than let a retry loop (wherever it's coming from) saturate the single
// httpd task. TickType_t wraps, but a same-sign 32-bit delta still comes
// out correct across the wrap (matches how out_transport.c's own poll
// counters handle this).
static void sendBreakBestEffort() {
    static const char body[] = "BREAK";
    static TickType_t  s_last_sent   = 0;
    static bool        s_sent_once   = false;
    const TickType_t   cooldown_tick = pdMS_TO_TICKS(2000);

    TickType_t now = xTaskGetTickCount();
    if (s_sent_once && (now - s_last_sent) < cooldown_tick) {
        log_w("sendBreakBestEffort: suppressed (last sent %lums ago)",
              (unsigned long)((now - s_last_sent) * portTICK_PERIOD_MS));
        return;
    }
    s_last_sent = now;
    s_sent_once = true;

    char cmd_tag_buf[32] = {0};
    sendTaggedCommand(body, strlen(body), cmd_tag_buf, sizeof(cmd_tag_buf), 1000);
}

static esp_err_t command_handler(httpd_req_t* req) {
    char* buf = NULL;

    if (parse_get(req, &buf) != ESP_OK) {
        log_e("Failed to parse get data...");
        return ESP_FAIL;
    }

    char* qry_buf = (char*)malloc(QRY_BUFFER_SIZE);
    if (qry_buf == NULL) {
        free(buf);
        log_e("Failed to allocate query buffer...");
        httpd_resp_send_500(req);
        return ESP_ERR_NO_MEM;
    }

    memset(qry_buf, 0, QRY_BUFFER_SIZE);
    if (httpd_query_key_value(buf, "base64", qry_buf, QRY_BUFFER_SIZE - 1) != ESP_OK) {
        free(buf);
        free(qry_buf);
        httpd_resp_send_404(req);
        return ESP_FAIL;
    }
    free(buf);

    char* cmd_buf = (char*)malloc(CMD_BUFFER_SIZE);
    if (cmd_buf == NULL) {
        free(qry_buf);
        log_e("Failed to allocate cmd buffer...");
        httpd_resp_send_500(req);
        return ESP_ERR_NO_MEM;
    }
    size_t cmd_size = 0;
    memset(cmd_buf, 0, CMD_BUFFER_SIZE);
    if (mbedtls_base64_decode(
          (unsigned char*)cmd_buf, CMD_BUFFER_SIZE, &cmd_size, (const unsigned char*)qry_buf, strlen(qry_buf)) != 0) {
        free(qry_buf);
        free(cmd_buf);
        log_e("Failed to decode cmd data...");
        httpd_resp_send_500(req);
        return ESP_FAIL;
    }
    free(qry_buf);

    char                              cmd_tag_buf[32] = {0};
    std::shared_ptr<PtrBuffer::Slot>  slot = sendTaggedCommand(cmd_buf, cmd_size, cmd_tag_buf, sizeof(cmd_tag_buf));
    free(cmd_buf);

    if (slot == nullptr) {
        log_w("Wait client reply slot timeout...");
        httpd_resp_send_500(req);
        return ESP_OK;
    }

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    httpd_resp_set_hdr(req, "X-Cmd-Tag", cmd_tag_buf);

    return httpd_resp_send(req, (const char*)slot->data, slot->size);
}

/* 2026-07-10: optional-query-string helper for the /camera/* and /audio/*
 * endpoints below - unlike parse_get() (used by command_handler, where a
 * missing "base64" param is a hard error), every param on these endpoints
 * has a sensible default, so no query string at all is a normal case, not
 * a 404. Returns "" (via out, untouched) when the key is absent. */
static void query_param(httpd_req_t* req, const char* key, char* out, size_t out_size, const char* dflt) {
    strncpy(out, dflt, out_size - 1);
    out[out_size - 1] = '\0';

    size_t qlen = httpd_req_get_url_query_len(req);
    if (qlen == 0) {
        return;
    }
    char* qbuf = (char*)malloc(qlen + 1);
    if (qbuf == NULL) {
        return;
    }
    if (httpd_req_get_url_query_str(req, qbuf, qlen + 1) == ESP_OK) {
        char val[16];
        if (httpd_query_key_value(qbuf, key, val, sizeof(val)) == ESP_OK) {
            strncpy(out, val, out_size - 1);
            out[out_size - 1] = '\0';
        }
    }
    free(qbuf);
}

/* 2026-07-10: dedicated backend camera control, mirroring the AT+SENSOR then
 * AT+INVOKE/AT+SAMPLE sequence used throughout this session's WE2-side
 * testing (see sscma_at_protocol_audit memory) - unlike the existing
 * web/index.html "Start Stream" button, which assembles the same AT
 * commands client-side through command_handler's raw passthrough, this lets
 * app_httpd.cpp itself drive the camera directly.
 * Query params (all optional):
 *   resolution=0|1|2   opt_id (0=240x240 1=480x480 2=640x480, default 2)
 *   mode=invoke|sample default invoke
 *   result_only=0|1    default 0 (0=stream images too, 1=metadata only)
 *   differed=0|1        default 0 - AT+INVOKE's DIFFERED arg
 * Replies with the AT+INVOKE/AT+SAMPLE Operation Response JSON (not
 * AT+SENSOR's - that one's checked for success but not returned, to keep
 * the response shape predictable regardless of whether resolution actually
 * changed). */
static esp_err_t camera_start_handler(httpd_req_t* req) {
    char resolution[16], mode[16], result_only[16], differed[16];
    query_param(req, "resolution", resolution, sizeof(resolution), "2");
    query_param(req, "mode", mode, sizeof(mode), "invoke");
    query_param(req, "result_only", result_only, sizeof(result_only), "0");
    query_param(req, "differed", differed, sizeof(differed), "0");

    int  res_id  = atoi(resolution);
    bool sample  = (strcmp(mode, "sample") == 0);
    if (res_id < 0 || res_id > 2) {
        res_id = 2;
    }

    char cmd_buf[64];
    char cmd_tag_buf[32] = {0};

    int n = snprintf(cmd_buf, sizeof(cmd_buf), "SENSOR=1,1,%d", res_id);
    auto sensor_slot = sendTaggedCommand(cmd_buf, n, cmd_tag_buf, sizeof(cmd_tag_buf));
    if (sensor_slot == nullptr) {
        log_w("camera_start: AT+SENSOR reply timeout...");
        httpd_resp_send_500(req);
        return ESP_OK;
    }

    if (sample) {
        n = snprintf(cmd_buf, sizeof(cmd_buf), "SAMPLE=-1");
    } else {
        n = snprintf(cmd_buf, sizeof(cmd_buf), "INVOKE=-1,%s,%s", differed, result_only);
    }
    auto stream_slot = sendTaggedCommand(cmd_buf, n, cmd_tag_buf, sizeof(cmd_tag_buf));
    if (stream_slot == nullptr) {
        log_w("camera_start: AT+INVOKE/AT+SAMPLE reply timeout...");
        httpd_resp_send_500(req);
        return ESP_OK;
    }

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    return httpd_resp_send(req, (const char*)stream_slot->data, stream_slot->size);
}

/* AT+BREAK - stops whichever camera stream (INVOKE or SAMPLE) is active.
 * Same universal stop the WE2 uses for audio too (see audio_stop_handler's
 * comment) - there's no separate "just the camera" stop on that side. */
static esp_err_t camera_stop_handler(httpd_req_t* req) {
    static const char body[] = "BREAK";
    char               cmd_tag_buf[32] = {0};
    auto               slot = sendTaggedCommand(body, strlen(body), cmd_tag_buf, sizeof(cmd_tag_buf));
    if (slot == nullptr) {
        log_w("camera_stop: AT+BREAK reply timeout...");
        httpd_resp_send_500(req);
        return ESP_OK;
    }
    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    return httpd_resp_send(req, (const char*)slot->data, slot->size);
}

/* AT+ASR=<rate> then AT+ASAMPLE=-1 - starts the DMIC/PDM audio stream at the
 * given rate (16000 or 32000 only - the WE2 firmware rejects anything else).
 * Query params: rate=16000|32000, default 16000. This app doesn't consume
 * the audio stream itself (no browser playback - control only, per
 * explicit direction), it just starts/stops/configures it on the WE2. */
static esp_err_t audio_start_handler(httpd_req_t* req) {
    char rate_str[16];
    query_param(req, "rate", rate_str, sizeof(rate_str), "16000");
    int rate = atoi(rate_str);
    if (rate != 16000 && rate != 32000) {
        rate = 16000;
    }

    char cmd_buf[32];
    char cmd_tag_buf[32] = {0};

    int n = snprintf(cmd_buf, sizeof(cmd_buf), "ASR=%d", rate);
    auto asr_slot = sendTaggedCommand(cmd_buf, n, cmd_tag_buf, sizeof(cmd_tag_buf));
    if (asr_slot == nullptr) {
        log_w("audio_start: AT+ASR reply timeout...");
        httpd_resp_send_500(req);
        return ESP_OK;
    }

    n = snprintf(cmd_buf, sizeof(cmd_buf), "ASAMPLE=-1");
    auto asample_slot = sendTaggedCommand(cmd_buf, n, cmd_tag_buf, sizeof(cmd_tag_buf));
    if (asample_slot == nullptr) {
        log_w("audio_start: AT+ASAMPLE reply timeout...");
        httpd_resp_send_500(req);
        return ESP_OK;
    }

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    return httpd_resp_send(req, (const char*)asample_slot->data, asample_slot->size);
}

/* 2026-07-10: WE2's AT+BREAK is a universal stop - camera AND audio
 * together, there's no dedicated audio-only stop in the WE2 AT protocol
 * (see sscma_at_protocol_audit memory: "there's no separate ABREAK, BREAK
 * is the universal stop, same as real SSCMA firmware"). This endpoint is
 * just AT+BREAK under a name that matches its /audio/start counterpart for
 * API symmetry - calling it also stops any running camera stream, same as
 * camera_stop_handler. Not a bug, a WE2-side protocol constraint. */
static esp_err_t audio_stop_handler(httpd_req_t* req) {
    static const char body[] = "BREAK";
    char               cmd_tag_buf[32] = {0};
    auto               slot = sendTaggedCommand(body, strlen(body), cmd_tag_buf, sizeof(cmd_tag_buf));
    if (slot == nullptr) {
        log_w("audio_stop: AT+BREAK reply timeout...");
        httpd_resp_send_500(req);
        return ESP_OK;
    }
    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    return httpd_resp_send(req, (const char*)slot->data, slot->size);
}

/* 2026-07-11: no more browser UI - this sketch is now a pure API/streaming
 * backend (control via /camera, /audio, /command; data via /stream/frame,
 * /stream/audio, /stream/result). web/index.html and web_index.h are still
 * in the repo but no longer built into the served UI; nothing registers "/"
 * anymore, so it 404s. */
void startCameraServer() {
    httpd_config_t config   = HTTPD_DEFAULT_CONFIG();
    config.max_uri_handlers = 16;
    config.stack_size       = 10240;

    httpd_uri_t command_uri = {.uri      = "/command",
                               .method   = HTTP_GET,
                               .handler  = command_handler,
                               .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                               ,
                               .is_websocket             = true,
                               .handle_ws_control_frames = false,
                               .supported_subprotocol    = NULL
#endif
    };

    httpd_uri_t result_uri = {.uri      = "/result",
                              .method   = HTTP_GET,
                              .handler  = results_handler,
                              .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                              ,
                              .is_websocket             = true,
                              .handle_ws_control_frames = false,
                              .supported_subprotocol    = NULL
#endif
    };

    // 2026-07-10: dedicated backend camera/audio control (see
    // camera_start_handler()/camera_stop_handler()/audio_start_handler()/
    // audio_stop_handler()'s comments) - alongside /command, not replacing
    // it; web/index.html's existing raw-AT controls keep working unchanged.
    httpd_uri_t camera_start_uri = {.uri      = "/camera/start",
                                    .method   = HTTP_GET,
                                    .handler  = camera_start_handler,
                                    .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                                    ,
                                    .is_websocket             = true,
                                    .handle_ws_control_frames = false,
                                    .supported_subprotocol    = NULL
#endif
    };

    httpd_uri_t camera_stop_uri = {.uri      = "/camera/stop",
                                   .method   = HTTP_GET,
                                   .handler  = camera_stop_handler,
                                   .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                                   ,
                                   .is_websocket             = true,
                                   .handle_ws_control_frames = false,
                                   .supported_subprotocol    = NULL
#endif
    };

    httpd_uri_t audio_start_uri = {.uri      = "/audio/start",
                                   .method   = HTTP_GET,
                                   .handler  = audio_start_handler,
                                   .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                                   ,
                                   .is_websocket             = true,
                                   .handle_ws_control_frames = false,
                                   .supported_subprotocol    = NULL
#endif
    };

    httpd_uri_t audio_stop_uri = {.uri      = "/audio/stop",
                                  .method   = HTTP_GET,
                                  .handler  = audio_stop_handler,
                                  .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                                  ,
                                  .is_websocket             = true,
                                  .handle_ws_control_frames = false,
                                  .supported_subprotocol    = NULL
#endif
    };

    httpd_uri_t stream_frame_uri = {.uri      = "/stream/frame",
                                    .method   = HTTP_GET,
                                    .handler  = stream_frame_handler,
                                    .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                                    ,
                                    .is_websocket             = true,
                                    .handle_ws_control_frames = false,
                                    .supported_subprotocol    = NULL
#endif
    };

    httpd_uri_t stream_result_uri = {.uri      = "/stream/result",
                                     .method   = HTTP_GET,
                                     .handler  = stream_result_handler,
                                     .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                                     ,
                                     .is_websocket             = true,
                                     .handle_ws_control_frames = false,
                                     .supported_subprotocol    = NULL
#endif
    };

    httpd_uri_t stream_audio_uri = {.uri      = "/stream/audio",
                                    .method   = HTTP_GET,
                                    .handler  = stream_audio_handler,
                                    .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                                    ,
                                    .is_websocket             = true,
                                    .handle_ws_control_frames = false,
                                    .supported_subprotocol    = NULL
#endif
    };

    ra_filter_init(&ra_filter, 20);

    esp_err_t ret = ESP_OK;

    log_i("Starting web server on port: '%d'", config.server_port);
    if ((ret = httpd_start(&web_httpd, &config)) == ESP_OK) {
        ret |= httpd_register_uri_handler(web_httpd, &result_uri);
        ret |= httpd_register_uri_handler(web_httpd, &command_uri);
        ret |= httpd_register_uri_handler(web_httpd, &camera_start_uri);
        ret |= httpd_register_uri_handler(web_httpd, &camera_stop_uri);
        ret |= httpd_register_uri_handler(web_httpd, &audio_start_uri);
        ret |= httpd_register_uri_handler(web_httpd, &audio_stop_uri);
    }

    if (ret != ESP_OK) {
        log_e("Failed to start web server, code '0x%x' ...", ret);
        while (true) {
            vTaskDelay(1000 / portTICK_PERIOD_MS);
        }
    }

    config.server_port = 8080;
    config.ctrl_port   = 8080;

    log_i("Starting stream server on port: '%d'", config.server_port);
    if ((ret = httpd_start(&stream_httpd, &config)) == ESP_OK) {
        ret |= httpd_register_uri_handler(stream_httpd, &stream_frame_uri);
        ret |= httpd_register_uri_handler(stream_httpd, &stream_result_uri);
        ret |= httpd_register_uri_handler(stream_httpd, &stream_audio_uri);
    }

    if (ret != ESP_OK) {
        log_e("Failed to start stream server, code '0x%x' ...", ret);
        while (true) {
            vTaskDelay(1000 / portTICK_PERIOD_MS);
        }
    }
}
