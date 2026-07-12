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
#include <esp_heap_caps.h>
#include <esp_http_server.h>
#include <freertos/semphr.h>
#include <mbedtls/base64.h>
#include <sdkconfig.h>

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <utility>
#include <vector>

#include <errno.h>
#include <sys/socket.h>

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
// 2026-07-12: JPG_BUFFER_SIZE (base64-decode scratch buffer) removed along
// with the whole /stream/frame image relay - dropped entirely per explicit
// direction, production only ever needs audio+bbox concurrently.
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
        size_t   id       = 0;
        uint16_t type     = 0;
        void*    data     = NULL;
        size_t   size     = 0;
        timeval  timestamp;
        // 2026-07-11: -1 means `data` came from a plain malloc() (REPLY/
        // LOGGI - small and infrequent) and the deleter should free() it
        // normally. >=0 means `data` points into s_audio_pool[pool_idx]
        // (see that pool's own comment; EVENT/bbox no longer uses PtrBuffer
        // at all - see pushBboxEvent()) and the deleter should release the
        // slot back to the pool instead - freeing pool memory would be
        // wrong (it's a static array, not a heap allocation).
        int pool_idx = -1;
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

/* 2026-07-12: bbox delivery no longer goes through PB.slots/the general
 * pool-based Slot mechanism at all - it used to (EVENT_POOL_SLOTS x
 * EVENT_POOL_SLOT_CAP, whole-JSON-message slots, full history in git if
 * needed), but that only ever held 1-2 *whole INVOKE messages* at a time,
 * so a WE2-side send burst (confirmed on hardware: WE2 delivers bbox
 * INVOKE events in bursts of ~30-40 back-to-back rather than evenly spaced
 * - see esp32_camera_web_server_bridge memory) collapsed down to whatever
 * the single lagging "newest only" eviction kept, and most of each
 * 512-byte slot went unused anyway (real single-box payloads measured
 * 77-132 bytes, but a single BOX TUPLE - "[x, y, w, h, score, target]" -
 * is only ~24-28 bytes on this camera/model: worst case is
 * "[640, 480, 640, 480, 255, 2]" = 28 bytes for a 640x480 sensor ceiling
 * and this model's 3-class target range).
 *
 * New design: a small ring buffer sized for individual BOX ELEMENTS (not
 * whole messages) - BBOX_RING_SLOTS entries x BBOX_ELEM_CAP bytes of raw
 * "[x, y, w, h, score, target]" text each, plus per-entry metadata (a
 * monotonic id for per-client "already sent" tracking, and a timestamp).
 * pushBboxEvent() parses each incoming INVOKE JSON's "boxes": [...] array
 * and pushes every element it contains as its own ring entry - multiple
 * elements from the SAME incoming UART message (one frame can report
 * several detected boxes) share one batch timestamp, captured once per
 * message, so a consumer can regroup them back into one reconstructed
 * INVOKE JSON line (see buildInvokeJson()) instead of losing which boxes
 * were reported together. When the ring is full, the next push overwrites
 * the oldest entry (plain circular buffer, no separate eviction pass
 * needed) - a lagging consumer that falls behind by more than
 * BBOX_RING_SLOTS elements silently loses the oldest ones, same
 * "graceful skip" contract this file uses everywhere else. */
#define BBOX_RING_SLOTS 20
#define BBOX_ELEM_CAP   30  // bytes; real max measured ~28B, see comment above

struct BboxEntry {
    char     text[BBOX_ELEM_CAP];  // raw "[x, y, w, h, score, target]" text, NOT NUL-terminated
    uint8_t  len;
    uint64_t id;         // 0 = never written; monotonic otherwise, one per element
    uint32_t batch_ms;   // shared by every element pushed from the same UART message
};

static BboxEntry         s_bbox_ring[BBOX_RING_SLOTS] = {};
static size_t             s_bbox_ring_next = 0;  // next write index (wraps) == oldest valid entry
static uint64_t           s_bbox_next_id   = 1;
static SemaphoreHandle_t  s_bbox_mutex;           // dedicated lock, separate from PB.mutex

/* Parses `"boxes": [[x, y, w, h, score, target], ...]` out of one WE2
 * INVOKE/SAMPLE EVENT JSON message and pushes each box tuple as its own
 * ring entry, all sharing one timestamp (this message's arrival time).
 * If there's no "boxes" field at all (e.g. a SAMPLE/image event, or a
 * malformed message), this is a no-op - there's nothing to store. Runs
 * only on loopTask (same single-writer assumption pushPBSlot() documents),
 * so no cross-writer protection is needed beyond the mutex readers also
 * take. */
static void pushBboxEvent(const char* resp, size_t len) {
    static const char BOXES_KEY[] = "\"boxes\": [";
    const char*        p          = strnstr(resp, BOXES_KEY, len);
    if (p == NULL) {
        return;
    }
    const char* end = resp + len;
    p += strlen(BOXES_KEY);

    uint32_t batch_ms = (uint32_t)millis();

    xSemaphoreTake(s_bbox_mutex, portMAX_DELAY);
    while (p < end && *p == '[') {
        const void* close_v = memchr(p, ']', end - p);
        if (close_v == NULL) {
            break;
        }
        const char* close    = (const char*)close_v;
        size_t      elem_len = (size_t)(close - p) + 1;
        if (elem_len > BBOX_ELEM_CAP) {
            log_w("pushBboxEvent: skipping %u-byte box element - exceeds "
                  "BBOX_ELEM_CAP (%u)...", (unsigned)elem_len, (unsigned)BBOX_ELEM_CAP);
        } else {
            BboxEntry& e = s_bbox_ring[s_bbox_ring_next];
            memcpy(e.text, p, elem_len);
            e.len             = (uint8_t)elem_len;
            e.id              = s_bbox_next_id++;
            e.batch_ms        = batch_ms;
            s_bbox_ring_next  = (s_bbox_ring_next + 1) % BBOX_RING_SLOTS;
        }
        p = close + 1;
        while (p < end && (*p == ',' || *p == ' ')) {
            ++p;
        }
    }
    xSemaphoreGive(s_bbox_mutex);
}

/* Copies (under s_bbox_mutex, so this is the ONLY thing done while holding
 * it - callers should build any output JSON afterward, outside the lock)
 * every ring entry newer than *last_id, in chronological order, into `out`
 * (caller-provided, must be at least BBOX_RING_SLOTS entries - the ring can
 * never hold more than that many at once so this always fits). Advances
 * *last_id to the newest id copied. If the caller fell behind by more than
 * BBOX_RING_SLOTS elements, the gap is simply not there anymore (oldest
 * entries were overwritten) - *last_id silently jumps forward to whatever
 * is now the oldest surviving entry, same contract pushBboxEvent()
 * documents. */
static size_t drainBboxSince(uint64_t* last_id, BboxEntry* out) {
    size_t count = 0;
    xSemaphoreTake(s_bbox_mutex, portMAX_DELAY);
    // s_bbox_ring_next is the next WRITE position, which is also the
    // OLDEST entry currently in the ring (it's the next one due to be
    // overwritten) - walking forward from there with wraparound visits
    // every entry oldest-to-newest.
    for (size_t i = 0; i < BBOX_RING_SLOTS; ++i) {
        BboxEntry& e = s_bbox_ring[(s_bbox_ring_next + i) % BBOX_RING_SLOTS];
        if (e.id == 0 || e.id <= *last_id) {
            continue;
        }
        out[count++] = e;
    }
    xSemaphoreGive(s_bbox_mutex);
    if (count > 0) {
        *last_id = out[count - 1].id;
    }
    return count;
}

/* Reconstructs one INVOKE-shaped JSON line (the same shape WE2's own
 * event_reply_named("INVOKE", ...) sends) from a run of `entries` that
 * share entries[0]'s batch timestamp - i.e. the boxes that were originally
 * reported together in one WE2 message. Returns how many of the leading
 * `count` entries were consumed (so the caller can advance past them and
 * call again for the next batch); writes the NUL-terminated JSON into
 * out_buf and its length (excluding the NUL) into *out_len, or *out_len=0
 * if out_buf was too small (still returns the correct consumed count so
 * the caller can skip this batch and keep going). `count_field` becomes
 * the reconstructed message's "count" - this is an ESP32-local sequence
 * now (the ring doesn't retain WE2's own per-message counter, only the
 * per-box timestamp/id), which is fine since nothing downstream depends on
 * it matching WE2's original numbering. */
static size_t buildInvokeJson(const BboxEntry* entries, size_t count, uint64_t count_field, char* out_buf,
                               size_t out_buf_cap, size_t* out_len) {
    *out_len = 0;
    if (count == 0 || out_buf_cap == 0) {
        return 0;
    }
    uint32_t batch_ms = entries[0].batch_ms;
    size_t   n         = 0;
    while (n < count && entries[n].batch_ms == batch_ms) {
        ++n;
    }

    int written = snprintf(out_buf, out_buf_cap,
                            "{\"type\": 1, \"name\": \"INVOKE\", \"code\": 0, \"data\": {\"count\": %llu, "
                            "\"boxes\": [",
                            (unsigned long long)count_field);
    if (written < 0 || (size_t)written >= out_buf_cap) {
        return n;
    }
    size_t pos = (size_t)written;
    for (size_t i = 0; i < n; ++i) {
        size_t need = entries[i].len + (i ? 2u : 0u);
        if (pos + need + 3 >= out_buf_cap) {  // +3 headroom for the trailing "]}}"
            break;
        }
        if (i) {
            out_buf[pos++] = ',';
            out_buf[pos++] = ' ';
        }
        memcpy(out_buf + pos, entries[i].text, entries[i].len);
        pos += entries[i].len;
    }
    out_buf[pos++] = ']';
    out_buf[pos++] = '}';
    out_buf[pos++] = '}';
    out_buf[pos]   = '\0';
    *out_len       = pos;
    return n;
}

/* 2026-07-12: AUDIO gets the exact same fixed-pool treatment EVENT got
 * earlier this session, for the exact same reason - per-message malloc()/
 * free() of large (~8KB) payloads is a proven heap fragmentation source on
 * this chip (see EVENT_POOL_SLOTS's own comment: that's literally why
 * EVENT moved off malloc() in the first place). AUDIO was deliberately
 * *left* on the malloc() path back then ("may get its own fixed pool
 * later, following the same pattern once sized from real hardware data" -
 * see pushPBSlot()'s old comment on this) because it hadn't visibly caused
 * a problem yet. It has now: splitting /stream/* across separate httpd
 * instances (see result_httpd's own comment) reduced this board's baseline
 * free heap enough that AUDIO's malloc/free churn alone (confirmed via an isolated,
 * zero-camera-traffic test) drove the same fragmentation collapse EVENT
 * used to cause - free heap fine in aggregate, largest contiguous block
 * collapsing to ~2.5KB and never recovering without a reboot. Sized at
 * 8192B - real frames observed on hardware are consistently 8018B (WE2's
 * PDM chunking is a fixed sample count per poll, not variable), with a
 * little headroom; AUDIO_FRAME_MAX_PAYLOAD's 16KB theoretical ceiling is
 * deliberately NOT what this is sized for - oversized frames just get
 * skipped (same graceful-degradation contract EVENT_POOL_SLOT_CAP already
 * has), same as an oversized EVENT slot already does. 2 slots matches the
 * AUDIO_RING_LIMIT cap in pushPBSlot() - no point sizing the pool bigger
 * than the ring is ever allowed to retain. */
// 2026-07-12 (rev 2): bumped 1 -> 2. With only 1 slot, confirmed on
// hardware that /stream/result and /stream/audio running CONCURRENTLY (the
// actual production requirement - see startCameraServer()'s comment)
// caused the audio consumer task to stall mid-send often enough that the
// single slot stayed permanently "held by a lagging consumer", starving
// every subsequent AUDIO push even after the client-side counter read 0.
// A single slot has zero slack for a consumer that's merely running a
// LITTLE behind - the moment one is mid-send, the *next* push has nowhere
// to go. 2 slots (~17.4KB total, vs the previous ~8.7KB) costs another
// ~8.7KB of static reservation on top of an already-tight ~22-27KB
// post-boot baseline - smaller than the 64KB EVENT_POOL_SLOTS=2 reservation
// that caused an outright ESP_ERR_HTTPD_TASK boot failure earlier this
// session, but still worth confirming boots cleanly before trusting this.
//
// 2026-07-12 (rev 3, REVERTED - CONFIRMED REAL): tried bumping 2 -> 4
// (paired with the EVENT_POOL_SLOTS 1 -> 4 bump above) - confirmed via a
// real power cycle AND a direct DTR/RTS reset toggle to be a genuine,
// reproducible boot crash loop, not a soft-reset artifact. See
// EVENT_POOL_SLOTS's own comment for the full evidence. AUDIO_RING_LIMIT in
// pushPBSlot() is defined as `= AUDIO_POOL_SLOTS`, so it scales
// automatically with whatever this is set to - no separate change needed
// there if this is revisited.
//
// 2026-07-12 (rev 4): bumped 2 -> 3, a smaller step than rev 3's 2->4 jump,
// per explicit direction to increase gradually and re-verify via a real
// reset each time rather than repeat the rev-3 crash. +8.5KB static
// reservation (17.4KB -> 26.1KB total).
//
// 2026-07-12 (rev 5, REVERTED): tried bumping 3 -> 4. Didn't crash (the
// rev-3 crash was separately root-caused to an unrelated SSCMA::ID()/
// name() NULL-strcpy bug, already fixed - see esp32_camera_web_server_bridge
// memory), but hardware-confirmed a severe THROUGHPUT regression instead:
// a 120s concurrent camera+audio run that got 582 events / 422 audio
// frames at slots=3 dropped to just 48 events / 31 audio frames at
// slots=4 (>90% loss), plus the client threads needed their full
// DURATION+30 join timeout instead of finishing at ~120s, and the ESP32's
// own USB-CDC debug log went completely silent for the whole run (no
// crash markers either - board was still HTTP-reachable immediately
// after). Total EVENT+AUDIO static reservation at 2/4 would have been
// ~4KB+34KB=38KB against a ~41KB free-heap baseline - most likely
// explanation is this left too little headroom for everything else
// (WiFi/lwIP buffers, task stacks), causing broad degradation rather than
// a clean crash. Reverted to 3, the best-measured configuration so far
// (0% EVENT loss, ~10% AUDIO loss under the same 120s concurrent test).
#define AUDIO_POOL_SLOTS    3
#define AUDIO_POOL_SLOT_CAP (1024 * 8 + 512)
static uint8_t            s_audio_pool[AUDIO_POOL_SLOTS][AUDIO_POOL_SLOT_CAP];
static std::atomic<bool>  s_audio_pool_used[AUDIO_POOL_SLOTS] = {false};

static uint8_t* audio_pool_acquire(int* out_idx) {
    for (int i = 0; i < AUDIO_POOL_SLOTS; ++i) {
        bool expected = false;
        if (s_audio_pool_used[i].compare_exchange_strong(expected, true)) {
            *out_idx = i;
            return s_audio_pool[i];
        }
    }
    *out_idx = -1;
    return nullptr;
}

static void audio_pool_release(int idx) {
    if (idx >= 0 && idx < AUDIO_POOL_SLOTS) {
        s_audio_pool_used[idx] = false;
    }
}

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

/* 2026-07-11: added alongside pushPBSlot()'s heap-aware eviction (see its
 * own comment) - tracks whether any HTTP client is actively pulling EVENT
 * (bbox, from stream_result_handler()) or AUDIO (from stream_audio_
 * handler()) slots right now. With nobody connected to a /stream/*
 * endpoint, retaining up to PTR_BUFFER_SIZE slots of real-world ~8KB
 * audio/bbox payloads is pure heap pressure for data nobody will ever read
 * - results_handler()'s one-shot GET /result only ever wants the single
 * newest slot anyway, so it's unaffected by more aggressive eviction while
 * these are 0.
 *
 * 2026-07-12: was `volatile int`, safe only because stream_result_handler()/
 * stream_audio_handler() used to share a single stream_httpd instance's one
 * worker task, serializing both writers against each other by construction.
 * Now that each runs on its own dedicated httpd instance (result_httpd/
 * audio_httpd - see startCameraServer()'s comment on why: the old
 * single-task setup could only ever serve ONE stream at a time, queuing the
 * other client's connection indefinitely, which doesn't work for one client
 * wanting both streams open together), that assumption no longer holds -
 * both writers can now genuinely run concurrently on different tasks.
 * `volatile` gives no atomicity guarantee for a read-modify-write like
 * `+= 1` (a torn increment under real concurrency could lose an update),
 * so this is std::atomic<int> now - same pattern already used for
 * s_event_pool_used[] earlier this session for the same underlying reason. */
static std::atomic<int> s_event_stream_clients{0};
static std::atomic<int> s_audio_stream_clients{0};

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

void initSharedBuffer() {
    PB.mutex     = xSemaphoreCreateMutex();
    s_bbox_mutex = xSemaphoreCreateMutex();
}

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
        // 2026-07-12: reverted to the real WE2 board's baud (default
        // SSCMA_UART_BAUD = 921600) - was temporarily forced to 115200 to
        // match Orange Pi's ttyS5 we2_sim.py simulator (its 24MHz UART
        // clock can't hit an accurate 921600). Any audio-frame-rate numbers
        // measured at 115200 (e.g. ~1 frame/s solo) were bandwidth-throttled
        // by that test setup, not representative of real hardware.
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
// 2026-07-12: EVENT/bbox no longer flows through here at all - proxyCallback()
// routes it straight to pushBboxEvent() instead (see that function's own
// comment). This is now REPLY/LOGGI/AUDIO only.
static void pushPBSlot(uint16_t type, const void* data, size_t len) {
    if (!len) {
        return;
    }

    timeval    timestamp;
    TickType_t ticks   = xTaskGetTickCount();
    timestamp.tv_sec   = ticks / configTICK_RATE_HZ;
    timestamp.tv_usec  = (ticks % configTICK_RATE_HZ) * 1e6 / configTICK_RATE_HZ;

    size_t discarded = 0;
    size_t limit     = PB.limit;
    xSemaphoreTake(PB.mutex, portMAX_DELAY);

    // 2026-07-11: with nobody actively pulling this type via a /stream/*
    // connection right now, don't bother retaining backlog for it -
    // eagerly evict every existing same-type slot (still respecting the
    // in-flight-command protection below) instead of waiting for the ring
    // to fill up to `limit`. Only applies to AUDIO now (EVENT/bbox has its
    // own ring, see pushBboxEvent()) - REPLY/LOGGI never had eager eviction
    // either, they default to has_consumer=true below.
    bool has_consumer = true;
    if (type & MSG_TYPE_AUDIO) {
        has_consumer = (s_audio_stream_clients > 0);
    }
    if (!has_consumer) {
        for (auto it = PB.slots.begin(); it != PB.slots.end();) {
            bool same_type = (*it)->type == type;
            bool protected_ = s_awaited_tag[0] != '\0' &&
                               strnstr((const char*)(*it)->data, s_awaited_tag, (*it)->size) != NULL;
            if (same_type && !protected_) {
                it = PB.slots.erase(it);
                discarded += 1;
            } else {
                ++it;
            }
        }
    }

    // 2026-07-12: AUDIO-specific cap, independent of the general ring's
    // `limit` (3, shared across all types). Root-caused on hardware: with
    // a connected consumer, has_consumer above is true, so the eager
    // eviction block just above is skipped entirely for AUDIO (by design,
    // to preserve backlog for a consumer to catch up on) - meaning AUDIO
    // slots could accumulate up to the *general* ring's full capacity of 3
    // before the generic fallback below ever evicts one. At up to 8018
    // bytes/slot that's ~24KB held alive simultaneously - fit fine against
    // this board's old ~44KB post-boot free-heap baseline, but no longer
    // fits comfortably now that splitting /stream/* across separate httpd
    // instances (see result_httpd's own comment) dropped that baseline.
    // Confirmed on hardware: an isolated audio-only stream (zero
    // camera/EVENT traffic, so this is squarely an AUDIO-only effect) ran
    // heap into a fragmentation spiral that never recovered even after the
    // client disconnected - classic embedded malloc/free fragmentation,
    // not a strict "never freed" leak, but practically indistinguishable
    // once it happens (ESP32's allocator doesn't defragment).
    //
    // 2026-07-12 (rev 2): AUDIO moved to its own fixed s_audio_pool
    // (AUDIO_POOL_SLOTS, see its own comment) instead of malloc(), which
    // is the real fix for the fragmentation itself - this ring cap now
    // just needs to match the pool's own physical slot count (1) so the
    // ring never pretends to hold more audio backlog than the pool can
    // actually back; a mismatch here would just mean the 2nd "retained"
    // ring slot fails at audio_pool_acquire() anyway, silently dropping
    // that frame instead of this eviction doing it explicitly.
    if (type & MSG_TYPE_AUDIO) {
        const size_t AUDIO_RING_LIMIT = AUDIO_POOL_SLOTS;
        size_t       audio_count      = 0;
        for (auto& s : PB.slots) {
            if (s->type == MSG_TYPE_AUDIO) {
                audio_count += 1;
            }
        }
        while (audio_count >= AUDIO_RING_LIMIT) {
            auto victim = std::find_if(PB.slots.begin(), PB.slots.end(),
                                        [](const std::shared_ptr<PtrBuffer::Slot>& s) {
                                            return s->type == MSG_TYPE_AUDIO;
                                        });
            if (victim == PB.slots.end()) {
                break;
            }
            PB.slots.erase(victim);
            discarded += 1;
            audio_count -= 1;
        }
    }

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

    // 2026-07-12: eviction above is DONE with PB.mutex now (moved the
    // release up from where it used to be, past the acquire+memcpy+malloc
    // below) - those don't need PB.slots or cross-writer protection
    // (pushPBSlot() only ever runs on the single loopTask, and
    // event_pool_acquire()/audio_pool_acquire() are already safe on their
    // own via std::atomic<bool>::compare_exchange_strong), so holding
    // PB.mutex across them was pure critical-section bloat: readers
    // (stream_result_handler()/stream_audio_handler(), both HIGHER
    // FreeRTOS priority than loopTask - see HTTPD_DEFAULT_CONFIG()'s
    // task_priority vs loopTask's) were being blocked from taking PB.mutex
    // for as long as an up-to-8018-byte memcpy() and a malloc() call took,
    // for no reason - neither touches PB.slots. Splitting into two short
    // critical sections (evict, then separately insert) lets readers back
    // in between the two. Safe to release-then-reacquire here specifically
    // because eviction (which frees up pool slots by dropping old
    // shared_ptrs) and the acquire below both run on this same single
    // thread with nothing else able to mutate PB.slots or the pools
    // in between (confirmed: grep shows PB.slots is only ever
    // inserted/erased from this function).
    xSemaphoreGive(PB.mutex);

    char* copy    = NULL;
    int   pool_i  = -1;
    if (type & MSG_TYPE_AUDIO) {
        // 2026-07-12: AUDIO now uses its own fixed s_audio_pool for the
        // exact same reason EVENT does (see AUDIO_POOL_SLOTS's own
        // comment) - confirmed on hardware that the malloc()/free() churn
        // this branch used to do for every ~8KB frame was a real
        // fragmentation source, not just a theoretical risk.
        if (len > AUDIO_POOL_SLOT_CAP) {
            log_w("pushPBSlot: skipping %u-byte AUDIO slot - exceeds "
                  "AUDIO_POOL_SLOT_CAP (%u)...", (unsigned)len, (unsigned)AUDIO_POOL_SLOT_CAP);
            return;
        }
        copy = (char*)audio_pool_acquire(&pool_i);
        if (copy == NULL) {
            log_w("pushPBSlot: skipping %u-byte AUDIO slot - both pool "
                  "slots busy (still held by a lagging consumer)...", (unsigned)len);
            return;
        }
    } else {
        // 2026-07-11: kept on the malloc() path for REPLY/LOGGI, which are
        // small and infrequent (AT command replies, not continuous
        // streaming data) - the fragmentation risk that justified fixed
        // pools for EVENT/AUDIO doesn't apply here. Checking the largest
        // available block up front turns a doomed malloc() into one clean,
        // distinguishable "skipping - heap too low" log line instead of an
        // attempt we already know will fail (which itself has nonzero cost
        // and can worsen fragmentation).
        size_t free_heap    = heap_caps_get_free_size(MALLOC_CAP_8BIT);
        size_t largest_free = heap_caps_get_largest_free_block(MALLOC_CAP_8BIT);
        size_t need         = len + sizeof(PtrBuffer::Slot);
        if (largest_free < need) {
            log_w("pushPBSlot: skipping %u-byte type=0x%04x slot - heap too low "
                  "(largest_free=%u free_heap=%u need=%u)...",
                  (unsigned)len, type, (unsigned)largest_free, (unsigned)free_heap, (unsigned)need);
            return;
        }
        copy = (char*)malloc(len);
        if (copy == NULL) {
            // Shouldn't happen given the largest_free check above (nothing
            // else on this single-threaded main task runs between that
            // check and this malloc()), but kept as a hard backstop in
            // case some future caller moves this off the main task.
            log_e("pushPBSlot: failed to allocate copy despite heap check "
                  "(largest_free was %u, len=%u)...", (unsigned)largest_free, (unsigned)len);
            return;
        }
    }
    // memcpy() (up to 8018 bytes for AUDIO) done above, outside PB.mutex -
    // see this function's earlier comment on why. Same for this malloc().
    memcpy(copy, data, len);

    PtrBuffer::Slot* p_slot = (PtrBuffer::Slot*)malloc(sizeof(PtrBuffer::Slot));
    if (p_slot == NULL) {
        if (type & MSG_TYPE_AUDIO) {
            audio_pool_release(pool_i);
        } else {
            free(copy);
        }
        log_e("pushPBSlot: failed to allocate slot...");
        return;
    }

    p_slot->type      = type;
    p_slot->data      = copy;
    p_slot->size      = len;
    p_slot->timestamp = timestamp;
    p_slot->pool_idx  = pool_i;

    // Second, short critical section: just the actual insert. p_slot->id
    // has to be assigned under this same lock (not earlier) so PB.id stays
    // a true insertion-order sequence even though preparing the slot above
    // happened outside the lock.
    xSemaphoreTake(PB.mutex, portMAX_DELAY);
    p_slot->id = PB.id;
    PB.slots.emplace_back(std::shared_ptr<PtrBuffer::Slot>(p_slot, [](PtrBuffer::Slot* p) {
        if (p == NULL) {
            return;
        }
        if (p->data != NULL) {
            // pool_idx >= 0 means `data` points into the fixed AUDIO pool,
            // not the heap - release the slot back to it instead of
            // free()ing static array memory (see PtrBuffer::Slot::pool_idx's
            // comment). EVENT no longer flows through PB.slots at all (see
            // pushBboxEvent()), so pool_idx >= 0 can only mean AUDIO here.
            if (p->pool_idx >= 0 && (p->type & MSG_TYPE_AUDIO)) {
                audio_pool_release(p->pool_idx);
            } else {
                free(p->data);
            }
            p->data = NULL;
        }
        free(p);
    }));
    xSemaphoreGive(PB.mutex);
    PB.id += 1;

    // 2026-07-12: this used to log every single push unconditionally (every
    // ~200-250ms while camera/audio is running) - invaluable while hunting
    // the heap-fragmentation and EVENT-pool-starvation bugs this session,
    // but pure noise now that both are fixed. Throttled to once every 2s,
    // except a low-heap reading (the exact condition worth catching early)
    // always logs immediately regardless of throttle.
    size_t free_now    = heap_caps_get_free_size(MALLOC_CAP_8BIT);
    size_t largest_now = heap_caps_get_largest_free_block(MALLOC_CAP_8BIT);
    static TickType_t s_last_heap_log   = 0;
    const TickType_t  heap_log_interval = pdMS_TO_TICKS(2000);
    bool              heap_low          = largest_now < 256;  // REPLY/LOGGI are small; EVENT/AUDIO use fixed pools now
    TickType_t        now_ticks         = xTaskGetTickCount();
    if (discarded > 0 || heap_low || (now_ticks - s_last_heap_log) >= heap_log_interval) {
        s_last_heap_log = now_ticks;
        log_i("Received %u bytes (type=0x%04x), heap: free=%u largest=%u, "
              "event_clients=%d audio_clients=%d, discarded=%u%s",
              len, type, (unsigned)free_now, (unsigned)largest_now, s_event_stream_clients.load(),
              s_audio_stream_clients.load(), discarded, heap_low ? " [LOW]" : "");
    }
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

    if (type & MSG_TYPE_EVENT) {
        pushBboxEvent(resp, len);
        return;
    }

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

// DBG (2026-07-12): diagnosing a suspected permanent-stall bug - if
// s_fetch_len ever gets stuck near FETCH_BUF_SIZE without pos advancing,
// the `s_fetch_len < FETCH_BUF_SIZE` guard on the read loop below stops
// draining atSerial forever. Logs only on state transitions (not every
// call) to avoid flooding.
static size_t s_dbg_last_logged_len   = 0;
static int    s_dbg_stall_call_count  = 0;

static void fetchFramedMessages(ResponseCallback cb) {
    while (s_fetch_len < FETCH_BUF_SIZE && atSerial.available()) {
        int c = atSerial.read();
        if (c < 0) {
            break;
        }
        s_fetch_buf[s_fetch_len++] = (uint8_t)c;
    }

    if (s_fetch_len >= (FETCH_BUF_SIZE - 4096) && s_fetch_len != s_dbg_last_logged_len) {
        log_w("DBG fetchFramedMessages: s_fetch_len high-watermark=%u/%u (first 16 bytes: "
              "%02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x %02x)",
              (unsigned)s_fetch_len, (unsigned)FETCH_BUF_SIZE,
              s_fetch_buf[0], s_fetch_buf[1], s_fetch_buf[2], s_fetch_buf[3],
              s_fetch_buf[4], s_fetch_buf[5], s_fetch_buf[6], s_fetch_buf[7],
              s_fetch_buf[8], s_fetch_buf[9], s_fetch_buf[10], s_fetch_buf[11],
              s_fetch_buf[12], s_fetch_buf[13], s_fetch_buf[14], s_fetch_buf[15]);
        s_dbg_last_logged_len = s_fetch_len;
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
        s_dbg_stall_call_count = 0;
    } else if (s_fetch_len >= (FETCH_BUF_SIZE - 4096)) {
        // near/at cap and made zero parse progress this call - confirms the
        // suspected permanent-stall scenario if this keeps firing.
        s_dbg_stall_call_count++;
        if (s_dbg_stall_call_count == 1 || (s_dbg_stall_call_count % 50) == 0) {
            log_e("DBG fetchFramedMessages: STALLED - s_fetch_len=%u, zero "
                  "progress for %d consecutive calls", (unsigned)s_fetch_len,
                  s_dbg_stall_call_count);
        }
    }
}

void loopRemoteProxy() {
    pollWe2Uart1Handshake();
    fetchFramedMessages(proxyCallback);
}

httpd_handle_t web_httpd = NULL;

/* 2026-07-12: used to be one `stream_httpd` hosting both /stream/result and
 * /stream/audio (and, briefly, a third /stream/frame image relay - dropped
 * entirely per explicit direction) -
 * but esp_http_server gives each httpd_start() instance exactly one worker
 * task, which serializes every handler registered on it. Confirmed on
 * hardware: a single client wanting both streams at once saw the second
 * connection's handler never even get scheduled - accepted at the TCP
 * level, then queued indefinitely - until the first one's handler function
 * returned. Split onto two separate instances, each its own worker task on
 * its own port, so bbox and audio streaming genuinely run in parallel
 * instead of taking turns. */
httpd_handle_t result_httpd = NULL;  // /stream/result (JSON events, bbox only)
httpd_handle_t audio_httpd  = NULL;  // /stream/audio  (raw framed PCM)

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
    esp_err_t        res     = ESP_OK;
    static uint64_t  last_id = 0;
    static char*     rst_buf = NULL;
    if (rst_buf == NULL) {
        rst_buf = (char*)malloc(RST_BUFFER_SIZE);
        if (rst_buf == NULL) {
            log_e("Failed to allocate results buffer...");
            httpd_resp_send_500(req);
            return ESP_ERR_NO_MEM;
        }
    }

    BboxEntry entries[BBOX_RING_SLOTS];
    size_t    count = 0;

    TickType_t time_begin = xTaskGetTickCount();
    while ((xTaskGetTickCount() - time_begin) < RESULT_TIMEOUT_MS) {
        count = drainBboxSince(&last_id, entries);
        if (count > 0) {
            break;
        }
        vTaskDelay(5 / portTICK_PERIOD_MS);
    }

    if (count == 0) {
        log_w("Find newer results slot timeout...");
        httpd_resp_send_500(req);
        return ESP_OK;
    }

    // Only the trailing batch (the most recently reported set of boxes) -
    // matches this one-shot endpoint's original "single newest slot"
    // semantics, now expressed as "single newest batch" since bbox no
    // longer arrives as whole cached messages - see pushBboxEvent().
    uint32_t newest_batch_ms = entries[count - 1].batch_ms;
    size_t   start           = count;
    while (start > 0 && entries[start - 1].batch_ms == newest_batch_ms) {
        --start;
    }

    size_t out_len = 0;
    buildInvokeJson(&entries[start], count - start, last_id, rst_buf, RST_BUFFER_SIZE, &out_len);
    if (out_len == 0) {
        log_e("Results buffer is not enough or broken json format...");
        httpd_resp_send_500(req);
        return ESP_OK;
    }

    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");

    char ts[32] = {0};
    snprintf(ts, sizeof(ts), "%llu", (unsigned long long)last_id);
    httpd_resp_set_hdr(req, "X-Id", (const char*)ts);

    memset(ts, 0, sizeof(ts));
    snprintf(ts, sizeof(ts), "%u", (unsigned)newest_batch_ms);
    httpd_resp_set_hdr(req, "X-Timestamp-Ms", (const char*)ts);

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

    res = httpd_resp_send(req, (const char*)rst_buf, out_len);
    if (res != ESP_OK) {
        log_e("Send results failed...");
    }

    return res;
}

/* Forward declaration - defined after sendTaggedCommand(), which it wraps;
 * stream_result_handler()/stream_audio_handler() below need it earlier in
 * the file than that definition lives. */
static void sendBreakBestEffort();

/* 2026-07-12: WE2's AT+BREAK is a universal stop - it kills the camera
 * INVOKE/SAMPLE loop AND the audio ASAMPLE loop together, there's no
 * modality-specific stop (see audio_stop_handler's own comment on this).
 * Every /stream/* handler below calls this exactly once, at its single
 * common exit point (replacing what used to be a bare `s_..._clients -= 1`)
 * - on the assumption that once nobody's watching *anything*, WE2 should
 * stop wasting power/NPU cycles, but a lone modality's client leaving
 * mustn't kill an unrelated, still-connected client's stream.
 *
 * 2026-07-12 (rev 2): originally this decremented the caller's counter
 * separately (at the true end of the function) from a *pre*-decrement
 * "<=1" check called earlier, right before each `break` - correct only
 * because stream_result_handler()/stream_audio_handler() used to share one
 * stream_httpd worker task, so at most one of them was ever actually
 * running this logic at a time. Now that each runs on its own dedicated
 * httpd instance (see startCameraServer()'s comment on why), both can
 * genuinely disconnect at the same instant on different tasks, and a
 * "check before I've decremented" pattern has a real TOCTOU race (two
 * threads can each see the other's not-yet-decremented count and both skip
 * sending BREAK). Fixed by decrementing THIS caller's own counter first
 * (atomically), then checking BOTH counters - whichever of the two
 * decrements happens last in real time is guaranteed to observe both
 * already at zero and send BREAK exactly once; the other observes the
 * not-yet-departed sibling and correctly stays quiet. */
static void disconnectStreamClient(std::atomic<int>* my_counter) {
    my_counter->fetch_sub(1);
    if (s_event_stream_clients.load() <= 0 && s_audio_stream_clients.load() <= 0) {
        sendBreakBestEffort();
    } else {
        log_i("disconnectStreamClient: another stream client still active "
              "(event=%d audio=%d) - not stopping WE2...",
              s_event_stream_clients.load(), s_audio_stream_clients.load());
    }
}

/* 2026-07-12: all three /stream/* handlers below sit in a tight idle loop
 * (vTaskDelay(5ms) + continue) while waiting for the *next* PtrBuffer slot
 * of the right type, and only ever discover a client disconnected when
 * they *actually try to send* something and that send fails. If nothing
 * new arrives from WE2 for a while (camera/audio not actively streaming,
 * or just a quiet moment) and the client goes away in the meantime (killed
 * abruptly - confirmed on hardware with `curl ... | head -c N`, which
 * closes its end without a clean shutdown the moment N bytes are read),
 * the handler never notices: it just keeps waiting forever, permanently
 * occupying the one worker task each server instance has (esp_http_server
 * serializes handlers on a single task per instance - see this file's
 * other comments on that) and locking out every subsequent /stream/*
 * request. Confirmed on hardware repeatedly this session, including via
 * this exact `head -c` pattern.
 *
 * Fix: during any idle wait, peek the raw socket for a clean close/error
 * without consuming or blocking - MSG_PEEK leaves any real (unexpected,
 * these are one-way GET streams) incoming bytes alone, MSG_DONTWAIT means
 * this never stalls the loop. recv() returning 0 is the standard "peer
 * shut down its write side" signal; a negative return that isn't
 * EAGAIN/EWOULDBLOCK (nothing available right now, not an error) is some
 * other real socket error. Deliberately doesn't inject synthetic
 * heartbeat *data* into the actual response body (JSON-lines/MJPEG/raw
 * audio consumers downstream would have to know to ignore it) - this
 * checks the transport directly instead. */
static bool client_gone(httpd_req_t* req) {
    int fd = httpd_req_to_sockfd(req);
    if (fd < 0) {
        log_w("DBG client_gone: httpd_req_to_sockfd returned %d - treating as gone", fd);
        return true;
    }
    uint8_t peek;
    errno   = 0;
    int     r = recv(fd, &peek, 1, MSG_PEEK | MSG_DONTWAIT);
    if (r == 0) {
        log_w("DBG client_gone: recv()==0 (peer closed cleanly), fd=%d", fd);
        return true; // peer closed
    }
    if (r < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        log_w("DBG client_gone: recv() returned %d, errno=%d (%s), fd=%d",
              r, errno, strerror(errno), fd);
        return true; // real socket error
    }
    return false;
}

/* 2026-07-12: relays raw AT+ASAMPLE binary frames (see MSG_TYPE_AUDIO's and
 * fetchFramedMessages()'s comments) to whatever's connected here, untouched
 * - this sketch doesn't decode PCM/WAV itself, a downstream platform does.
 * Plain application/octet-stream chunked body - each WE2 binary frame
 * already carries its own self-describing header/length, a client just
 * needs to re-run the same framing this sketch itself parses in
 * fetchFramedMessages(). Sends every not-yet-sent AUDIO slot in order -
 * dropping PCM chunks would leave audible gaps downstream. */
static esp_err_t stream_audio_handler(httpd_req_t* req) {
    esp_err_t res     = ESP_OK;
    size_t    last_id = 0;

    res = httpd_resp_set_type(req, "application/octet-stream");
    if (res != ESP_OK) {
        return res;
    }
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");

    // See s_audio_stream_clients's own comment - pushPBSlot() checks this
    // to decide whether AUDIO backlog is worth retaining at all.
    s_audio_stream_clients += 1;

    // 2026-07-12: see stream_result_handler's identical comment - hard
    // backstop against client_gone() not detecting a dead peer promptly.
    TickType_t idle_since               = xTaskGetTickCount();
    const TickType_t IDLE_TIMEOUT_TICKS = pdMS_TO_TICKS(10000);

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
            if (client_gone(req)) {
                log_w("stream_audio_handler: client gone while idle, stopping WE2 stream...");
                break;
            }
            if ((xTaskGetTickCount() - idle_since) >= IDLE_TIMEOUT_TICKS) {
                log_w("stream_audio_handler: idle timeout (10s with nothing new), "
                      "stopping WE2 stream as a backstop...");
                break;
            }
            vTaskDelay(5 / portTICK_PERIOD_MS);
            continue;
        }
        idle_since = xTaskGetTickCount();

        // 2026-07-12: under continuous traffic this loop never goes idle, so
        // the client_gone() check above never runs - a dead peer left this
        // task blocked inside httpd_resp_send_chunk() for 58+ seconds on
        // hardware (confirmed: event_clients/audio_clients stayed pinned at
        // 1 with zero progress) because the socket's own send-side didn't
        // error out promptly. Re-check (cheap, non-blocking MSG_PEEK) right
        // before every send attempt too, not just when idle.
        if (client_gone(req)) {
            log_w("stream_audio_handler: client gone (detected before active send), stopping WE2 stream...");
            break;
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
            break;
        }
    }

    disconnectStreamClient(&s_audio_stream_clients);

    return res;
}

static esp_err_t stream_result_handler(httpd_req_t* req) {
    esp_err_t        res     = ESP_OK;
    static uint64_t  last_id = 0;

    res |= httpd_resp_set_status(req, HTTPD_200);
    res |= httpd_resp_set_type(req, "application/json");
    res |= httpd_resp_set_hdr(req, "Connection", "keep-alive");
    res |= httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    if (res != ESP_OK) {
        log_e("Failed to set response headers...");
        return res;
    }

    // See s_event_stream_clients's own comment - pushPBSlot() checks this
    // to decide whether EVENT backlog is worth retaining at all.
    s_event_stream_clients += 1;

    // 2026-07-12: hard backstop against client_gone()'s MSG_PEEK detection
    // not firing promptly. Confirmed on hardware via targeted tracing: a
    // client that disconnected (curl -m 8, client-side timeout) left this
    // handler's idle loop calling client_gone() every 5ms for over FIFTY
    // SECONDS before it finally recognized the peer was gone - during that
    // whole window the single worker task stayed occupied, so every other
    // connection attempt to this port just queued at the TCP level
    // (accepted, per max_open_sockets) without ever actually being
    // serviced. Root cause of the slow detection itself wasn't pinned down
    // (a curl -m timeout's close() should send a normal FIN, which
    // MSG_PEEK's recv()==0 should catch quickly) - this is a pragmatic
    // ceiling regardless of why: EVENT traffic arrives every ~200-250ms
    // when the camera's running, so genuinely nothing new for 10 straight
    // seconds already means either nobody's watching or something's stuck
    // - safe to just close and let a fresh connection start clean either
    // way.
    TickType_t idle_since               = xTaskGetTickCount();
    const TickType_t IDLE_TIMEOUT_TICKS = pdMS_TO_TICKS(10000);

    while (res == ESP_OK) {
        BboxEntry entries[BBOX_RING_SLOTS];
        size_t    count = drainBboxSince(&last_id, entries);

        if (count == 0) {
            if (client_gone(req)) {
                log_w("stream_result_handler: client gone while idle, stopping WE2 stream...");
                break;
            }
            if ((xTaskGetTickCount() - idle_since) >= IDLE_TIMEOUT_TICKS) {
                log_w("stream_result_handler: idle timeout (10s with nothing new), "
                      "stopping WE2 stream as a backstop...");
                break;
            }
            vTaskDelay(5 / portTICK_PERIOD_MS);
            continue;
        }
        idle_since = xTaskGetTickCount();

        // 2026-07-12: see stream_audio_handler's identical comment - under
        // continuous EVENT traffic this loop never goes idle, so the
        // client_gone() check above never runs. Re-check right before every
        // active send too.
        if (client_gone(req)) {
            log_w("stream_result_handler: client gone (detected before active send), stopping WE2 stream...");
            break;
        }

        // 2026-07-12: rebuilds the entries copied out of the ring (under
        // s_bbox_mutex, inside drainBboxSince()) into one INVOKE JSON line
        // PER BATCH (boxes that arrived together in the same WE2 message) -
        // see buildInvokeJson()'s own comment. A single poll can carry
        // several batches at once (WE2 delivers bbox in bursts, see
        // BBOX_RING_SLOTS' own comment) - this loop sends each as its own
        // chunk instead of collapsing them into one. Static buffer: this
        // handler's own dedicated httpd instance still only has one worker
        // task, so exactly one invocation of this function ever runs at a
        // time - same single-writer assumption results_handler()'s rst_buf
        // already relies on.
        static char s_bbox_buf[RST_BUFFER_SIZE];
        size_t      pos = 0;
        while (pos < count && res == ESP_OK) {
            size_t out_len  = 0;
            size_t consumed = buildInvokeJson(&entries[pos], count - pos, entries[pos].id, s_bbox_buf,
                                               sizeof(s_bbox_buf), &out_len);
            if (consumed == 0) {
                break;  // shouldn't happen (count - pos > 0 here), safety backstop
            }
            pos += consumed;
            if (out_len == 0) {
                log_e("stream_result_handler: failed to rebuild INVOKE json "
                      "(buffer too small) - skipping this batch...");
                continue;
            }
            res |= httpd_resp_send_chunk(req, s_bbox_buf, out_len);
            res |= httpd_resp_send_chunk(req, MSG_TERMI_STR, strlen(MSG_TERMI_STR));
        }

        if (res != ESP_OK) {
            log_e("Send results failed...");
            break;
        }
    }

    disconnectStreamClient(&s_event_stream_clients);

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

/* 2026-07-11: called from stream_result_handler()/stream_audio_handler() the
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
// cause not yet confirmed) can drive stream_result_handler()/stream_audio_
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
 *   differed=0|1        default 0 - AT+INVOKE's DIFFERED arg
 * Replies with the AT+INVOKE/AT+SAMPLE Operation Response JSON (not
 * AT+SENSOR's - that one's checked for success but not returned, to keep
 * the response shape predictable regardless of whether resolution actually
 * changed).
 *
 * 2026-07-12: result_only is no longer a client-settable query param -
 * always forced to "1" (bbox/metadata only, no image). The /stream/frame
 * image relay this existed to feed was dropped entirely per explicit
 * direction (production only needs audio+bbox concurrently, image was
 * debug-only). Letting a client still request result_only=0 would mean
 * WE2 keeps sending full ~18KB images over UART for pushPBSlot() to
 * receive and then immediately discard in stream_result_handler() - pure
 * waste of UART bandwidth and (until EVENT_POOL_SLOT_CAP was shrunk to
 * match, see its own comment) precious pool memory too. */
static esp_err_t camera_start_handler(httpd_req_t* req) {
    char resolution[16], mode[16], differed[16];
    query_param(req, "resolution", resolution, sizeof(resolution), "2");
    query_param(req, "mode", mode, sizeof(mode), "invoke");
    query_param(req, "differed", differed, sizeof(differed), "0");
    static const char* result_only = "1";

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

    // 2026-07-12: two separate instances (see result_httpd/audio_httpd's
    // own comment for why) instead of one shared stream_httpd - each needs
    // its own smaller stack_size, not the control-plane web_httpd's 10240
    // (that one parses AT command JSON replies; these are plain relay
    // loops with modest locals).
    //
    // max_open_sockets also lowered from HTTPD_DEFAULT_CONFIG()'s default
    // of 7 (per instance!) down to 2 - each only ever needs to serve one
    // client at a time by design (single-client deployment, one connection
    // per stream type).
    config.max_uri_handlers  = 1;
    config.max_open_sockets  = 2;

    config.server_port = 8081;
    config.ctrl_port   = 8081;
    // 2026-07-12: bumped 4096 -> 6144. stream_result_handler() now has a
    // real stack-local, `BboxEntry entries[BBOX_RING_SLOTS]` - 20 x
    // sizeof(BboxEntry) (48B, alignment-padded - see BboxEntry's own
    // comment) = 960B, about a quarter of the old 4096B budget on its own -
    // the "no large locals" assumption this stack size was originally
    // sized under (see stream_audio_handler's identical comment, still
    // true for that one) no longer holds here.
    config.stack_size  = 6144;
    log_i("Starting result stream server on port: '%d'", config.server_port);
    if ((ret = httpd_start(&result_httpd, &config)) == ESP_OK) {
        ret |= httpd_register_uri_handler(result_httpd, &stream_result_uri);
    }
    if (ret != ESP_OK) {
        log_e("Failed to start result stream server, code '0x%x' ...", ret);
        while (true) {
            vTaskDelay(1000 / portTICK_PERIOD_MS);
        }
    }

    config.server_port = 8082;
    config.ctrl_port   = 8082;
    config.stack_size  = 4096;  // stream_audio_handler: plain binary relay, no large locals
    log_i("Starting audio stream server on port: '%d'", config.server_port);
    if ((ret = httpd_start(&audio_httpd, &config)) == ESP_OK) {
        ret |= httpd_register_uri_handler(audio_httpd, &stream_audio_uri);
    }
    if (ret != ESP_OK) {
        log_e("Failed to start audio stream server, code '0x%x' ...", ret);
        while (true) {
            vTaskDelay(1000 / portTICK_PERIOD_MS);
        }
    }
}
