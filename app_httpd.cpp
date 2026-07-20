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
#include <Preferences.h>
#include <Seeed_Arduino_SSCMA.h>
#include <Wire.h>
#include <WiFi.h>
#include <esp_wifi.h>

#include "ST7735_XIAO.h"
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
#include <sys/uio.h>

#if defined(ARDUINO_ARCH_ESP32) && defined(CONFIG_ARDUHAL_ESP_LOG)
    #include <HardwareSerial.h>
    #include <esp32-hal-log.h>
#endif

// 2026-07-17: was 3000 - see CMD_TAG_FMT_STR's comment for the hardware
// evidence (genuine replies arriving a few hundred ms late, repeatedly)
// that motivated this bump. 8000 comfortably covers that margin.
#define RESULT_TIMEOUT_MS 8000
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
// proxyCallback()/ggedCommand() (s_awaited_tag) instead of trying
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
// uniqueness (every tag started with it), so it's dropped entirely.
//
// 2026-07-17: widened 3 hex digits -> 4 (12 bits -> 16 bits, wraps every
// ~4s -> ~65s) alongside bumping RESULT_TIMEOUT_MS 3000 -> 8000 - the old
// 3-digit tag's ~4s wrap was no longer "comfortably longer" than the new
// 8s timeout (it'd wrap mid-wait, risking exactly the late-reply/fresh-
// request tag collision this comment used to warn about). Hardware
// evidence motivating the RESULT_TIMEOUT_MS bump: `sendTaggedCommand()`'s
// own diagnostic logging (see its consecutive-timeout branch) showed
// genuine UART1 traffic arriving a few hundred ms before each 3s timeout
// fired, repeatedly, for the same in-flight command - consistent with the
// real reply arriving just after the old deadline rather than never at
// all. Costs one more tag byte (5 total vs 4), so short queries now need
// 17 bytes or under to stay inside the 16-byte DW_UART RX FIFO in one
// burst - longer bodies were already at risk of this either way.
#define CMD_TAG_FMT_STR "%.4X@"
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

// 2026-07-18: TEMP DIAGNOSTIC - `free`/`largest` alone can't distinguish
// "genuine leak" (total free shrinking) from "fragmentation" (total free
// roughly stable but largest contiguous block collapsing while allocated
// block *count* climbs). heap_caps_get_info() gives the full multi_heap_info_t
// breakdown (allocated_blocks, free_blocks, total_allocated_bytes,
// minimum_free_bytes) in one call - logging all of it at the same points
// the existing free/largest DBG lines already fire, so the two can be
// correlated against the same timeline. Remove once the fragmentation
// source is found.
static void dbg_log_heap_info(const char* tag) {
    multi_heap_info_t info;
    heap_caps_get_info(&info, MALLOC_CAP_8BIT);
    log_w("DBG heap_info[%s]: free=%u largest=%u min_ever_free=%u "
          "alloc_blocks=%u free_blocks=%u total_alloc_bytes=%u",
          tag, (unsigned)info.total_free_bytes, (unsigned)info.largest_free_block,
          (unsigned)info.minimum_free_bytes, (unsigned)info.allocated_blocks,
          (unsigned)info.free_blocks, (unsigned)info.total_allocated_bytes);
}

// Forward declaration - full definition lives further down this file,
// after slot_pool_release()/audio_pool_release() (which SlotRef's
// destructor needs). std::deque tolerates an incomplete element type at
// the point of member declaration (only needs to be complete by the time
// any deque member function is actually instantiated, which happens much
// later in this file, well after SlotRef's real definition).
class SlotRef;

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
        // 2026-07-18: intrusive refcount for SlotRef (see its own comment,
        // defined further down this file after slot_pool_release()) -
        // replaces std::shared_ptr<Slot>, which allocated a separate
        // control block on the heap on every single construction (every
        // audio frame + every REPLY/LOGGI, the same high-frequency-small-
        // alloc pattern that caused fragmentation before Slot itself and
        // AUDIO's payload were moved onto fixed pools). Living inside the
        // already-pooled Slot means refcounting this needs zero additional
        // heap allocations.
        std::atomic<int> refcount{0};
    };

    SemaphoreHandle_t          mutex;
    std::deque<SlotRef>        slots;
    volatile size_t            id    = 1;
    const size_t               limit = PTR_BUFFER_SIZE;
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

/* 2026-07-11: tracks whether any HTTP client is actively pulling EVENT
 * (bbox) or AUDIO data right now. With nobody connected, retaining data
 * nobody will ever read is pure waste - results_handler()'s one-shot
 * GET /result only ever wants the single newest slot anyway, so it's
 * unaffected by more aggressive eviction while this is 0.
 *
 * 2026-07-18: was two separate counters (s_event_stream_clients/
 * s_audio_stream_clients), one per streaming httpd instance/worker task,
 * from the brief period this file had `/stream/result` and `/stream/audio`
 * as two independent long-lived connections - merged into one
 * `/stream/data` endpoint (stream_data_handler()) carrying both AUDIO and
 * EVENT/bbox traffic over a single connection/worker/accept(). One
 * connection, one counter. Declared up here (rather than near where the
 * rest of this file's globals cluster, further down) specifically so
 * pushBboxEvent() below - which runs on loopTask, upstream of any
 * WiFi/httpd-side buffering - can gate on it too (see that function's own
 * 2026-07-18 comment on why: an un-gated bbox ring kept accumulating a
 * detection backlog while nobody was connected, which a fresh connect then
 * had to burst-flush all at once - a real, measured contributor to the
 * connect-time heap spike this session spent a long time chasing, per
 * user's diagnosis: nothing should accumulate for a connection nobody has
 * open yet). */
static std::atomic<int> s_data_stream_clients{0};

/* Parses `"boxes": [[x, y, w, h, score, target], ...]` out of one WE2
 * INVOKE/SAMPLE EVENT JSON message and pushes each box tuple as its own
 * ring entry, all sharing one timestamp (this message's arrival time).
 * If there's no "boxes" field at all (e.g. a SAMPLE/image event, or a
 * malformed message), this is a no-op - there's nothing to store. Runs
 * only on loopTask (same single-writer assumption pushPBSlot() documents),
 * so no cross-writer protection is needed beyond the mutex readers also
 * take. */
static void pushBboxEvent(const char* resp, size_t len) {
    // TEMP DIAGNOSTIC (2026-07-17): investigating a "No route to host" WiFi
    // drop specifically correlated with camera activity, ~35-55s into a
    // run - checking whether a genuine heap leak/growth (as opposed to a
    // power-supply brownout) is the real cause. pushBboxEvent() is the one
    // thing that runs once per camera detection and previously had zero
    // heap visibility (unlike pushPBSlot(), which already logs heap every
    // 2s for REPLY/AUDIO traffic). Remove once this is resolved either way.
    {
        static uint32_t s_dbg_last_heap_log_ms = 0;
        uint32_t        now_ms                 = millis();
        if (now_ms - s_dbg_last_heap_log_ms >= 2000) {
            s_dbg_last_heap_log_ms = now_ms;
            dbg_log_heap_info("pushBboxEvent");
        }
    }
    static const char BOXES_KEY[] = "\"boxes\": [";
    const char*        p          = strnstr(resp, BOXES_KEY, len);
    if (p == NULL) {
        return;
    }

    // 2026-07-18: see s_data_stream_clients' own comment above - with
    // nobody connected to /stream/data right now, don't bother storing
    // detections nobody will ever read. Mirrors pushPBSlot()'s AUDIO
    // has_consumer gating (same rationale, same fix pattern) - a fresh
    // connect should start clean instead of immediately having to
    // burst-flush whatever accumulated in the ring while it was gone.
    if (s_data_stream_clients <= 0) {
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

/* 2026-07-18: fixed pool for the `PtrBuffer::Slot` metadata struct itself -
 * the one remaining `malloc()`/`free()` in pushPBSlot()'s hot path (see
 * this function's own comment: it used to be flagged as a candidate for
 * the combined-camera+audio-only heap collapse this session was chasing).
 * Every single push - EVERY audio frame (~every 125-250ms while streaming,
 * even though the frame's PAYLOAD already lives in the fixed s_audio_pool
 * above) plus every REPLY/LOGGI - mallocs and frees one of these. Measured
 * directly on hardware this session: `heap_caps_get_largest_free_block()`
 * trended steadily DOWNWARD across repeated combined camera+audio
 * connect/disconnect cycles even though `heap_caps_get_free_size()` (total
 * free bytes) recovered fully after each cycle - the signature of
 * fragmentation, not a leak of total bytes. Isolated audio-only/camera-only
 * runs (see esp32_camera_web_server_bridge memory, 2026-07-18 bisection)
 * never showed this because each alone has only ONE traffic pattern's worth
 * of alloc sizes cycling through the heap; combined load interleaves this
 * struct's fixed-size churn with REPLY/LOGGI's *variable*-size mallocs
 * (`pushPBSlot()`'s `malloc(len)` below) at a much higher combined rate,
 * which is exactly the kind of mixed-size churn that defeats a
 * general-purpose allocator's coalescing. Moving this struct itself onto a
 * fixed pool (same pattern already proven for AUDIO_POOL/EVENT_POOL above)
 * removes it from the equation entirely - it can no longer contribute
 * fragmentation regardless of push rate or interleaving pattern.
 * Sized generously beyond PB.limit (PTR_BUFFER_SIZE=3): a slot erased from
 * PB.slots can stay alive a little longer via an outstanding shared_ptr
 * copy held by sendTaggedCommand()'s or a stream handler's own local
 * snapshot, so more than `limit` instances can technically be live for a
 * brief moment. */
#define SLOT_POOL_SIZE 8
static PtrBuffer::Slot   s_slot_pool[SLOT_POOL_SIZE];
static std::atomic<bool> s_slot_pool_used[SLOT_POOL_SIZE] = {false};

static PtrBuffer::Slot* slot_pool_acquire() {
    for (int i = 0; i < SLOT_POOL_SIZE; ++i) {
        bool expected = false;
        if (s_slot_pool_used[i].compare_exchange_strong(expected, true)) {
            return &s_slot_pool[i];
        }
    }
    return nullptr;
}

static void slot_pool_release(PtrBuffer::Slot* p) {
    int idx = (int)(p - s_slot_pool);
    if (idx >= 0 && idx < SLOT_POOL_SIZE) {
        s_slot_pool_used[idx] = false;
    }
}

/* 2026-07-18: intrusive-refcount replacement for std::shared_ptr<Slot> -
 * see PtrBuffer::Slot::refcount's own comment for why (std::shared_ptr's
 * pointer+deleter constructor always heap-allocates a separate control
 * block, once per construction - a high-frequency small allocation this
 * file had already root-caused and fixed for the Slot struct itself and
 * AUDIO's payload buffer, but missed for the shared_ptr wrapping them).
 * Storing the refcount inside the already-pooled Slot means this needs
 * zero heap allocations of its own. Deliberately mimics only the subset
 * of shared_ptr's interface this file actually uses (op->, op*, op bool,
 * .get(), .reset(), copy/move, == nullptr) so every call site only needed
 * its TYPE changed, not its logic. */
class SlotRef {
public:
    SlotRef() : p_(nullptr) {}
    explicit SlotRef(PtrBuffer::Slot* p) : p_(p) {
        if (p_) {
            p_->refcount.fetch_add(1, std::memory_order_relaxed);
        }
    }
    SlotRef(const SlotRef& other) : p_(other.p_) {
        if (p_) {
            p_->refcount.fetch_add(1, std::memory_order_relaxed);
        }
    }
    SlotRef(SlotRef&& other) noexcept : p_(other.p_) {
        other.p_ = nullptr;
    }
    SlotRef& operator=(const SlotRef& other) {
        if (this != &other) {
            release();
            p_ = other.p_;
            if (p_) {
                p_->refcount.fetch_add(1, std::memory_order_relaxed);
            }
        }
        return *this;
    }
    SlotRef& operator=(SlotRef&& other) noexcept {
        if (this != &other) {
            release();
            p_       = other.p_;
            other.p_ = nullptr;
        }
        return *this;
    }
    ~SlotRef() { release(); }

    void             reset() { release(); p_ = nullptr; }
    PtrBuffer::Slot* get() const { return p_; }
    PtrBuffer::Slot* operator->() const { return p_; }
    PtrBuffer::Slot& operator*() const { return *p_; }
    explicit operator bool() const { return p_ != nullptr; }
    bool operator==(std::nullptr_t) const { return p_ == nullptr; }
    bool operator!=(std::nullptr_t) const { return p_ != nullptr; }

private:
    // Releases this reference; once the last one goes, frees the payload
    // (pool release for AUDIO, free() for REPLY/LOGGI - same split
    // pushPBSlot() already established) and returns the Slot struct itself
    // to s_slot_pool. Does NOT touch p_ - callers clear it themselves
    // (reset()/assignment operators) since a couple of call sites need the
    // old value gone before they can safely overwrite it.
    void release() {
        if (p_ && p_->refcount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            if (p_->data != NULL) {
                if (p_->pool_idx >= 0 && (p_->type & MSG_TYPE_AUDIO)) {
                    audio_pool_release(p_->pool_idx);
                } else {
                    free(p_->data);
                }
                p_->data = NULL;
            }
            slot_pool_release(p_);
        }
    }

    PtrBuffer::Slot* p_;
};

/* 2026-07-11: guards PB's oldest-first eviction (see proxyCallback()) from
 * discarding the one REPLY/LOGGI slot an in-flight sendTaggedCommand() is
 * actually waiting for. Confirmed on hardware: while INVOKE/ASAMPLE keep
 * streaming EVENT frames into the same small ring (PTR_BUFFER_SIZE has to
 * stay small - see its own comment - continuous ~8KB JPEG frames make a
 * bigger ring blow the chip's ~86KB heap), a REPLY sitting behind even a
 * couple of those EVENT slots got evicted before sendTaggedCommand()'s 5ms
 * polling loop ever saw it, so AT+BREAK/AT+ASR kept timing out even though
 * WE2 had genuinely already replied.
 *
 * 2026-07-18: the "single string because single-threaded" reasoning this
 * comment used to give is WRONG and was already contradicted by
 * s_event_stream_clients/s_audio_stream_clients's own comment just below -
 * once result_httpd/audio_httpd became separate instances (each with its
 * own worker task), stream_result_handler() and stream_audio_handler() can
 * each independently call disconnectStreamClient() -> sendBreakBestEffort()
 * -> sendTaggedCommand() on their own task, genuinely concurrently with each
 * other AND with web_httpd's task (camera/audio start/stop, /command). Root-
 * caused as the actual explanation for "combined camera+audio load only"
 * heap/connection collapse (camera-only and audio-only bisection each ran
 * 15 clean cycles - see esp32_camera_web_server_bridge memory - because
 * with only one stream ever active, only one task ever reaches this code at
 * a time; only the combined case can have two tasks racing here). Two
 * concurrent sendTaggedCommand() calls clobber this shared buffer AND
 * interleave raw bytes on the single I2C Wire bus with no locking at all -
 * s_cmd_mutex (declared right below) now serializes the whole function, so
 * this really is single-writer again, just enforced instead of assumed. */
static char s_awaited_tag[32] = {0};

/* 2026-07-18: see s_awaited_tag's comment above - serializes tag bookkeeping
 * + the reply-wait poll of sendTaggedCommand() across whichever task calls
 * it, since independent httpd worker tasks (web_httpd; data_httpd via
 * disconnectStreamClient()) can both legitimately call it. Plain
 * (non-recursive) mutex - sendBreakBestEffort() takes and releases its OWN
 * separate short critical section for its debounce check before calling
 * sendTaggedCommand(), never while already holding this one, so there's no
 * self-deadlock risk.
 *
 * 2026-07-21: NO LONGER covers the raw I2C write itself (see
 * s_i2c_bus_mutex below, which now guards that) - it used to, and that was
 * the whole problem: this mutex is held for the ENTIRE AT-command round
 * trip including the reply-wait poll, up to RESULT_TIMEOUT_MS (8000ms) on a
 * timeout. The servo code (stepServoAngle()/servoRampPoll(), called from
 * loopTask - the same, highest-priority task that drains WE2's UART, see
 * loopTask's own priority-bump comment in camera_web_server.ino) used to
 * share this exact mutex purely to arbitrate the physical I2C bus, which
 * meant a servo step could block loopTask for up to 8 seconds behind an
 * in-flight AT command that has nothing to do with I2C at that point (it's
 * just waiting on a UART reply). Splitting the bus-arbitration concern out
 * into its own short-held mutex lets a servo step proceed immediately even
 * while a sendTaggedCommand() call is deep in its reply-wait poll, since
 * that poll doesn't touch the I2C bus at all - see stepServoAngle()'s and
 * s_servo_mutex's own comments. */
static SemaphoreHandle_t s_cmd_mutex;

/* 2026-07-21: guards the physical I2C bus (Wire, D0/D1) itself - the ONE
 * thing sendTaggedCommand()'s raw write and every PCA9685 servo write
 * (pca9685WriteReg()/pca9685SetPwm()) genuinely share and must not
 * interleave on. Held only for the duration of a single Wire
 * transmission (microseconds-to-low-single-digit-ms), never across a
 * poll, a delay(), or an NVS write - see s_cmd_mutex's own comment for why
 * that distinction matters. */
static SemaphoreHandle_t s_i2c_bus_mutex;

/* 2026-07-21: guards the read-modify-write of s_servo_angle + the PWM write
 * + the persist-to-NVS decision as one atomic unit, so two concurrent step
 * requests (e.g. /motor/left and servoRampPoll() both landing at once)
 * can't race on the read and silently drop a step - see stepServoAngle()'s
 * own comment. Deliberately separate from s_cmd_mutex: the servo has never
 * needed to be mutually exclusive with an AT command's multi-second
 * reply-wait, only with the brief moment another servo write or an
 * AT-command's I2C write (itself now under s_i2c_bus_mutex, taken
 * internally by pca9685WriteReg()/pca9685SetPwm()) is on the wire. */
static SemaphoreHandle_t s_servo_mutex;

/* 2026-07-18: minimal RAII wrapper so sendTaggedCommand()'s several early
 * return points (timeout, success) can't accidentally leak s_cmd_mutex held -
 * matches this file's existing preference for RAII (std::shared_ptr,
 * std::atomic) over manual accounting. */
struct MutexGuard {
    SemaphoreHandle_t m;
    explicit MutexGuard(SemaphoreHandle_t m_) : m(m_) { xSemaphoreTake(m, portMAX_DELAY); }
    ~MutexGuard() { xSemaphoreGive(m); }
};

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
 * probe handshake automatically instead of requiring a manual ESP32 reboot.
 *
 * 2026-07-17: N-consecutive-timeouts alone turned out to fire falsely under
 * sustained camera+audio streaming - confirmed via dual WE2+ESP32 console
 * capture: WE2 was still very much alive and still sending real UART1
 * traffic (bbox JSON, audio frames), but a single AT reply (e.g. BREAK from
 * sendBreakBestEffort()) got queued behind that continuous ~8018B-frame
 * traffic on the same wire often enough to miss RESULT_TIMEOUT_MS (3s) two
 * times in a row purely from congestion, with WE2 never actually rebooting.
 * That falsely tripped this resync path mid-stream, which drops
 * s_fetch_len (below) and forces pollWe2Uart1Handshake() to wait for a probe
 * WE2's own out_transport.c will in fact never send again post-switch (it's
 * a one-way switch - see esp32_camera_web_server_bridge memory) - the false
 * trip itself was the real cause of the ~13-16s stream_audio_handler/
 * stream_result_handler reconnect cycle this was meant to explain. See
 * s_last_uart1_rx_ms below - the resync trigger now also requires genuine
 * UART1 silence, not just one tagged reply going missing. */
#define WE2_RESYNC_TIMEOUT_THRESHOLD (2)
/* Only actually treat consecutive tagged-reply timeouts as evidence of a
 * real WE2 reboot if UART1 has ALSO been completely silent for this long -
 * real traffic (any byte at all, tracked in fetchFramedMessages()) is proof
 * WE2 is still alive and still on UART1, no matter how backed up the one
 * specific reply sendTaggedCommand() is waiting for might be. Comfortably
 * longer than RESULT_TIMEOUT_MS x WE2_RESYNC_TIMEOUT_THRESHOLD so a
 * congestion-only false positive (traffic still flowing, just not that one
 * reply) never trips this, while an actual reboot (UART1 goes fully dead)
 * still gets caught quickly.
 *
 * 2026-07-17: bumped 5000 -> 20000 alongside RESULT_TIMEOUT_MS's 3000->8000
 * bump - needs to stay above RESULT_TIMEOUT_MS x WE2_RESYNC_TIMEOUT_THRESHOLD
 * (now 8000 x 2 = 16000), and the old 5000 no longer cleared that bar on its
 * own (it was already less than one single new-length timeout). */
#define WE2_RESYNC_SILENCE_MS (20000)
/* Absolute safety valve, independent of the silence check above - hardware-
 * confirmed 2026-07-17: a sustained-audio-load scenario where UART1 traffic
 * (real audio/bbox bytes) kept trickling in but the AT+BREAK reply
 * specifically never arrived, indefinitely (s_consecutive_at_timeouts
 * climbed past 15 with zero recovery), left the board needing a manual
 * ESP32 hard reset to unstick - the silence-gated check alone has no upper
 * bound on how long it'll keep trusting "still alive" over "actually do
 * something about it". This ceiling forces a resync attempt regardless of
 * recent traffic once things have clearly gone wrong for long enough that
 * continuing to wait isn't a reasonable bet anymore, trading a (rare)
 * unnecessary resync for never being stuck forever. */
#define WE2_RESYNC_HARD_CEILING (8)
static uint32_t s_last_uart1_rx_ms = 0;

/* 2026-07-13: HTTP Basic Auth, required on every endpoint (control commands
 * AND the /stream/* and /result endpoints) - per explicit direction, change
 * these two to whatever credentials are actually wanted. Any client (a
 * browser, curl, or media_client.py) must send an `Authorization: Basic
 * <base64(user:pass)>` header matching s_expected_auth_hdr (computed once at
 * boot in initHttpAuth()) or gets a 401. Plain strcmp, not constant-time -
 * fine for this board's threat model (local network, not a public-internet
 * service), not appropriate to reuse as-is somewhere timing attacks matter.
 *
 * Re-added 2026-07-13 after a first attempt was reverted on suspicion of
 * causing a boot crash - on investigation the board never actually crashed
 * from this code: DHCP handed it a new IP after a reflash/power-cycle, so
 * requests against the old IP looked like total failure, compounded by the
 * WE2 UART1 handshake needing longer than usual to settle that particular
 * boot. Re-verify end to end against whatever IP the board actually has
 * before assuming a regression next time. */
#define HTTP_AUTH_USER ""
#define HTTP_AUTH_PASS ""

static char s_expected_auth_hdr[64] = {0};  // "Basic <base64(user:pass)>", filled in by initHttpAuth()

static void initHttpAuth() {
    char cred[64];
    int  cred_len = snprintf(cred, sizeof(cred), "%s:%s", HTTP_AUTH_USER, HTTP_AUTH_PASS);

    unsigned char b64[96]  = {0};
    size_t        b64_len  = 0;
    mbedtls_base64_encode(b64, sizeof(b64), &b64_len, (const unsigned char*)cred, cred_len);

    snprintf(s_expected_auth_hdr, sizeof(s_expected_auth_hdr), "Basic %.*s", (int)b64_len, (const char*)b64);
}

/* Called as the first line of every registered handler (see each handler's
 * own body). Sends the 401 + WWW-Authenticate response itself on failure -
 * callers just need to `return ESP_FAIL` right after a false-returning call,
 * they don't build any response of their own in that case. */
static bool checkAuth(httpd_req_t* req) {
    char hdr[96] = {0};
    if (httpd_req_get_hdr_value_str(req, "Authorization", hdr, sizeof(hdr)) != ESP_OK) {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_set_hdr(req, "WWW-Authenticate", "Basic realm=\"ESP32\"");
        httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return false;
    }
    if (strcmp(hdr, s_expected_auth_hdr) != 0) {
        httpd_resp_set_status(req, "401 Unauthorized");
        httpd_resp_set_hdr(req, "WWW-Authenticate", "Basic realm=\"ESP32\"");
        httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
        httpd_resp_send(req, "Unauthorized", HTTPD_RESP_USE_STRLEN);
        return false;
    }
    return true;
}

void initSharedBuffer() {
    PB.mutex        = xSemaphoreCreateMutex();
    s_bbox_mutex    = xSemaphoreCreateMutex();
    s_cmd_mutex     = xSemaphoreCreateMutex();
    s_i2c_bus_mutex = xSemaphoreCreateMutex();
    s_servo_mutex   = xSemaphoreCreateMutex();
    initHttpAuth();
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
        // 2026-07-17: reverted to the real WE2 board's baud (default
        // SSCMA_UART_BAUD = 921600) - the 115200 override was only ever for
        // testing against an Orange Pi UART stand-in (see
        // esp32_camera_web_server_bridge / orangepi_zero3_uart5_i2c3_probe
        // memory - that whole approach was abandoned this session in favor
        // of testing directly against real WE2 hardware).
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

// 2026-07-17: WE2's I2C slave address (matches Seeed_Arduino_SSCMA.h's own
// I2C_ADDRESS default, and the WE2-side i2c_cmd.c's I2C_CMD_SLV_ADDR -
// EPII_CM55M_APP_S/app/scenario_app/sscma_cam_mic/i2c_cmd.c).
#define WE2_I2C_CMD_ADDR (0x62)

// Split-channel bring-up: commands go out over I2C (this function), replies/
// results still arrive over UART exactly as before (atSerial, PROTO_UART
// case above, untouched) - WE2-side i2c_cmd.c feeds whatever it receives
// here into the exact same AT-command dispatcher UART commands go through,
// so no reply-side change was needed there either. Separate from
// startRemoteProxy()'s existing PROTO_I2C case, which routes *everything*
// through Wire via the SSCMA library's own AI object - that case is
// untouched/unused here, this is an additional path alongside PROTO_UART.
void initI2CCommandChannel() {
    // NOT COM_BUFFER_SIZE (32KB, sized for UART's much higher throughput) -
    // this only ever carries a single short AT command string per
    // transmission. 512 comfortably covers CMD_PREFIX+tag+body+CMD_SUFFIX
    // for any command this sketch builds, with margin (WE2-side i2c_cmd.c's
    // own receive buffer is 128 bytes). The 32KB request failed outright
    // once WiFi/HTTP had already claimed most of the heap: "[E][Wire.cpp]
    // allocateWireBuffer(): Can't allocate memory for I2C_0 txBuffer".
    Wire.setBufferSize(512);
    // 2026-07-17: moved off the default D4(SDA)/D5(SCL) - i2c_master_transmit
    // kept failing with ESP_ERR_INVALID_STATE (bus not idle-high at transmit
    // time) on D4/D5 even after checking pull-ups/GND against real WE2
    // hardware, so trying a different ESP32 pin pair per user's direction.
    // D0=GPIO2 is an ESP32-C3 strapping pin - fine here since this only
    // matters during the boot/reset window, not during normal operation
    // long after boot. WE2 side (i2c_cmd.c) is unchanged - still PA2/PA3 -
    // only the physical wire's ESP32 endpoint moves, from D4/D5 to D0/D1.
    Wire.begin(D0, D1);  // master mode, SDA=D0(GPIO2), SCL=D1(GPIO3)
}

// ---------------------------------------------------------------------------
// 2026-07-19: PCA9685 (I2C addr 0x40, default - all address pins tied low)
// driving an SG92R servo on PWM channel 0. Shares the same physical I2C
// bus/pins that initI2CCommandChannel() above already configured
// (Wire.begin(D0, D1)) - this does NOT call Wire.begin() again, and every
// raw Wire access below is taken under s_cmd_mutex (same mutex
// sendTaggedCommand() uses for WE2 traffic on this bus - see s_cmd_mutex's
// own comment), so a servo write can never interleave its bytes with an
// in-flight AT command's.
#define PCA9685_ADDR          (0x40)
#define PCA9685_REG_MODE1     (0x00)
#define PCA9685_REG_MODE2     (0x01)
#define PCA9685_REG_PRESCALE  (0xFE)
#define PCA9685_REG_LED0_ON_L (0x06)
#define PCA9685_MODE1_AI      (0x20)  // auto-increment register address
#define PCA9685_MODE1_SLEEP   (0x10)
#define PCA9685_MODE1_RESTART (0x80)
#define PCA9685_MODE2_OUTDRV  (0x04)  // totem-pole (not open-drain) output

#define SERVO_PWM_CHANNEL  (0)
#define SERVO_PWM_FREQ_HZ  (50)
// SG92R datasheet pulse-width range at 50Hz: ~500us (0 deg) - ~2400us
// (180 deg). If a specific unit's mechanical 0/180 endpoints turn out to
// grind against the gear stops, narrow this range rather than changing the
// angle clamp below.
#define SERVO_MIN_PULSE_US (500)
#define SERVO_MAX_PULSE_US (2400)
#define SERVO_MIN_ANGLE    (0)
#define SERVO_MAX_ANGLE    (180)
#define SERVO_BOOT_ANGLE   (90)
#define SERVO_STEP_DEG     (2)
// 2026-07-19: boot-time centering ramps through intermediate angles instead
// of jumping straight to SERVO_BOOT_ANGLE in one PWM write - the servo's
// true physical position after a power cycle is unknown (no feedback), so a
// single-shot command can be a large, fast, abrupt swing. Ramping from 0
// covers the full possible range regardless of where it actually started.
#define SERVO_RAMP_STEP_DEG       (2)
#define SERVO_RAMP_STEP_DELAY_MS  (60)

// Tracks the servo's last-commanded angle so motor_left/right_handler()
// only need to know the step direction, not an absolute target.
static std::atomic<int> s_servo_angle(SERVO_BOOT_ANGLE);

// 2026-07-20: target-angle tracking for motor_set_handler()/servoRampPoll()
// - see servoRampPoll()'s own comment for why this exists (one HTTP call
// sets this, a background poll steps toward it over time) instead of the
// caller having to spam many /motor/left or /motor/right calls to sweep a
// wide range. Originally motivated by real s_cmd_mutex contention measured
// against WE2 AT-command traffic under sustained sweeping (every ~0.5-0.65s
// for the whole test) - that root cause (the servo sharing s_cmd_mutex's
// multi-second AT-command reply-wait hold, not just brief I2C bus access)
// is now fixed directly (2026-07-21, see s_cmd_mutex's own comment), but
// this mechanism still stands on its own merits: fewer HTTP round trips to
// sweep a wide range either way.
static std::atomic<int> s_servo_target_angle(SERVO_BOOT_ANGLE);

// 2026-07-19: persists the last-commanded angle across power cycles
// (hardware-verified bug: without this, initServoMotor()'s boot ramp always
// started counting up from a hardcoded 0, so if the servo had last been
// left near 180, boot caused an immediate full 180->0 jump before the ramp
// even began - worse than the abrupt snap the ramp was added to avoid).
// NVS write wear is not a concern here - servo stepping is a manual,
// human-paced action (HTTP calls from button presses), nowhere near NVS's
// ~100k-erase-cycle lifetime.
#define SERVO_NVS_NAMESPACE "servo"
#define SERVO_NVS_KEY_ANGLE "angle"
static Preferences s_servo_prefs;

// 2026-07-19: set explicitly (matches sendTaggedCommand()'s own
// Wire.setTimeOut(200) on this same shared Wire instance) - without it, a
// missing/unresponsive PCA9685 (unplugged, no ACK, floating SDA/SCL) can
// leave Wire.endTransmission() blocking indefinitely, which - called from
// initServoMotor() during setup() - would hang the entire boot before WiFi/
// HTTP ever comes up, with zero serial output to explain why. Bounds every
// I2C op on this bus (WE2 command traffic included) to 200ms.
static bool s_servo_available = false;

// 2026-07-21: takes s_i2c_bus_mutex itself now (used to rely on callers
// holding s_cmd_mutex - see that mutex's own comment for why that no longer
// works now the servo path doesn't take s_cmd_mutex at all). Returns the
// Wire.endTransmission() status (0 = success) so callers can detect a
// missing/non-ACKing PCA9685 instead of assuming the write landed.
static uint8_t pca9685WriteReg(uint8_t reg, uint8_t val) {
    MutexGuard bus_lock(s_i2c_bus_mutex);
    Wire.beginTransmission(PCA9685_ADDR);
    Wire.write(reg);
    Wire.write(val);
    return Wire.endTransmission();
}

// Sets one PWM channel's ON/OFF tick counts (0-4095 across one 4096-tick
// PWM period). ON is always 0 here (pulse starts at the top of the period),
// only OFF varies - the datasheet's simplest single-pulse-per-period usage.
// Takes s_i2c_bus_mutex itself - see pca9685WriteReg()'s own comment.
//
// 2026-07-21: now returns the Wire.endTransmission() status (previously
// discarded) so a NACK/timeout here is at least detectable if a caller ever
// wants to check it - a live sweep is a repeated string of these calls with
// the servo physically under load, and this was previously invisible
// (silently eating up to Wire.setTimeOut(200)'s 200ms inside loopTask with
// zero log trace). Investigated as a candidate root cause for a client-side
// stream latency correlation this session; hardware-measured clean (no
// errors, all calls under threshold) - the actual culprit turned out to be
// stepServoAngle()'s display redraw instead, see its own comment.
static uint8_t pca9685SetPwm(uint8_t channel, uint16_t on, uint16_t off) {
    MutexGuard bus_lock(s_i2c_bus_mutex);
    uint8_t    base = PCA9685_REG_LED0_ON_L + 4 * channel;
    Wire.beginTransmission(PCA9685_ADDR);
    Wire.write(base);
    Wire.write(on & 0xFF);
    Wire.write(on >> 8);
    Wire.write(off & 0xFF);
    Wire.write(off >> 8);
    return Wire.endTransmission();
}

// Converts a 0-180 degree angle into the PCA9685 tick count (0-4095 across
// one 20ms/50Hz period) for the SG92R pulse-width range above. Pure
// function, no I2C/locking.
static uint16_t servoAngleToTicks(int angle) {
    uint32_t pulse_us = SERVO_MIN_PULSE_US + ((uint32_t)(SERVO_MAX_PULSE_US - SERVO_MIN_PULSE_US) * angle) /
                                                  SERVO_MAX_ANGLE;
    return (uint16_t)((pulse_us * 4096UL) / (1000000UL / SERVO_PWM_FREQ_HZ));
}

// Steps from from_angle to to_angle in SERVO_RAMP_STEP_DEG increments
// (either direction), pausing SERVO_RAMP_STEP_DELAY_MS between each - see
// SERVO_RAMP_STEP_DEG's own comment for why boot centering ramps instead of
// jumping straight to the target. No lock of its own - callers take
// s_servo_mutex first. Both angles are assumed already clamped to
// [SERVO_MIN_ANGLE, SERVO_MAX_ANGLE].
static void rampServoTo(int from_angle, int to_angle) {
    int step = (to_angle >= from_angle) ? SERVO_RAMP_STEP_DEG : -SERVO_RAMP_STEP_DEG;
    int a    = from_angle;
    while ((step > 0 && a < to_angle) || (step < 0 && a > to_angle)) {
        pca9685SetPwm(SERVO_PWM_CHANNEL, 0, servoAngleToTicks(a));
        delay(SERVO_RAMP_STEP_DELAY_MS);
        a += step;
    }
    pca9685SetPwm(SERVO_PWM_CHANNEL, 0, servoAngleToTicks(to_angle));
}

// Applies delta (+/-SERVO_STEP_DEG, from motor_left/right_handler, or a
// ramp step from servoRampPoll()) to the tracked angle, clamped to
// [SERVO_MIN_ANGLE, SERVO_MAX_ANGLE] so the SG92R can never be commanded
// past its physical 0-180 range. Read-modify-write of s_servo_angle happens
// under s_servo_mutex (not just the I2C write, which pca9685SetPwm() itself
// further guards with s_i2c_bus_mutex) so two concurrent step requests
// can't race on the read and silently drop a step.
//
// 2026-07-21: `persist` lets the caller defer both the NVS write AND the
// display redraw instead of doing them on every single call - see
// servoRampPoll()'s own comment for why a multi-step ramp only wants this
// true on the step that actually reaches the target, not every intermediate
// step. motor_left/right_handler (manual, human-paced single steps - each
// one a deliberate final resting position) always pass true. Returns the
// resulting angle.
//
// 2026-07-21: the display redraw specifically went through THREE attempts
// this session before landing back here - kept as history, all three traps
// are easy to fall back into if this is ever "simplified":
//   1. Direct, unconditional displayShowMotorAngle() call every step (the
//      original code): hardware-measured ~65-75ms per call (SPI text
//      redraw on the ST7735), an order of magnitude slower than the
//      PCA9685 I2C write - a 40-75 step sweep could burn 2.5-5+ seconds of
//      loopTask time on redraws alone, directly stalling WE2 UART drain
//      (loopTask's own priority-bump comment in camera_web_server.ino
//      explains why that task being blocked at all is bad).
//   2. Rate-limited to at most once per 250ms: measured on hardware to
//      still cut resp.read() spikes down but NOT eliminate them, and the
//      achieved gap between redraws came out to ~525ms, not the configured
//      250ms - under sustained 32kHz-audio+camera load, loopTask is
//      sensitive enough to ANY added per-iteration cost that a slower
//      loop() cycle lets UART RX backlog grow, which makes the NEXT
//      fetchFramedMessages() call take longer to drain, which delays the
//      loop even further - a self-reinforcing bufferbloat-style cascade
//      where a bounded, rate-limited cost turned into an unbounded-feeling
//      one.
//   3. Moved the SPI work off loopTask entirely onto a dedicated
//      xTaskCreate()'d consumer task fed by a queue: this DID fix the
//      loopTask-stall/latency-spike problem (hardware-confirmed), but
//      xTaskCreate()'s stack (4096B, permanently allocated for the life of
//      the process, not pooled/reused like everything else in this file)
//      pushed this board's already-razor-thin heap over the edge under
//      sustained 32kHz-audio+camera+motor load - hardware-confirmed via a
//      real crash/reboot (USB re-enumerated) with heap stats logged right
//      before it (free=6436 largest=1652 min_ever_free=348) showing severe
//      fragmentation and near-total exhaustion. Reverted. This board's heap
//      margin has bitten this exact way before (see AUDIO_POOL_SLOTS 3->4
//      in the bridge memory doc) - ANY new persistent allocation here
//      (task stack, static buffer, or otherwise) needs the same full
//      combined-load hardware soak test as that, not just a couple of
//      short sweeps, before it can be trusted.
// The persist-gate below (skip the redraw on every non-final ramp step,
// not just rate-limit it) is the one that's actually shipped: zero
// additional heap cost, and the tradeoff (display doesn't animate live
// during a sweep, only jumps to the final value once it settles) is a
// reasonable price for a fix that doesn't reopen the heap-exhaustion risk.
static int stepServoAngle(int delta, bool persist) {
    MutexGuard servo_lock(s_servo_mutex);
    int        angle = s_servo_angle.load() + delta;
    if (angle < SERVO_MIN_ANGLE) {
        angle = SERVO_MIN_ANGLE;
    } else if (angle > SERVO_MAX_ANGLE) {
        angle = SERVO_MAX_ANGLE;
    }
    // Skip the actual I2C write if init never found a PCA9685 (see
    // initServoMotor()) - still safe to call even so, now that
    // Wire.setTimeOut(200) bounds it, but there's no point blocking an httpd
    // worker for up to 200ms against hardware known not to be there.
    if (s_servo_available) {
        pca9685SetPwm(SERVO_PWM_CHANNEL, 0, servoAngleToTicks(angle));
    }
    s_servo_angle.store(angle);
    if (persist) {
        s_servo_prefs.putInt(SERVO_NVS_KEY_ANGLE, angle);
        // "--" (not a possibly-misleading number) if there's no PCA9685 to
        // actually be holding this position - matches s_servo_available's
        // own gating of the real PWM write just above. Only reached here on
        // the step that actually settles (manual step, or a ramp's final
        // step) - see this function's own comment for why intermediate ramp
        // steps skip this entirely instead of merely rate-limiting it or
        // moving it to another task.
        displayShowMotorAngle(s_servo_available ? angle : -1);
    }
    return angle;
}

// 2026-07-20: non-blocking counterpart to rampServoTo() - that function (used
// only at boot, see initServoMotor()) holds s_servo_mutex for its ENTIRE
// multi-step ramp, which is fine once at startup (nothing else can be
// calling in) but would be a much worse version of the exact contention
// problem this whole mechanism exists to avoid if reused for a live "move
// slowly to X" request. Call this from loop() instead: each call does AT
// MOST one SERVO_RAMP_STEP_DEG step, taking s_servo_mutex only for that one
// step's read-modify-write + I2C write (same brief-hold pattern as
// stepServoAngle(), which this largely mirrors), then returns - the mutex
// is never held across the pacing delay itself, so a long sweep now costs
// one HTTP request instead of dozens.
//
// 2026-07-21: this runs in loopTask - the same, highest-priority task that
// drains WE2's UART (see loopTask's own priority-bump comment in
// camera_web_server.ino) - so stepServoAngle() blocking here for any
// meaningful time would stall UART draining right along with it. That's
// exactly why the servo path was split onto its own s_servo_mutex +
// s_i2c_bus_mutex instead of continuing to share s_cmd_mutex with
// sendTaggedCommand(): the old shared mutex meant a ramp step could block
// this task for up to RESULT_TIMEOUT_MS (8000ms) behind an in-flight AT
// command's reply-wait poll, even though that poll never touches the I2C
// bus - see s_cmd_mutex's own comment.
void servoRampPoll() {
    static uint32_t s_last_step_ms = 0;
    int              target        = s_servo_target_angle.load();
    int              current       = s_servo_angle.load();
    if (target == current) {
        return;
    }
    uint32_t now_ms = millis();
    if (now_ms - s_last_step_ms < SERVO_RAMP_STEP_DELAY_MS) {
        return;
    }
    s_last_step_ms  = now_ms;
    int delta       = (target > current) ? SERVO_RAMP_STEP_DEG : -SERVO_RAMP_STEP_DEG;
    // Don't overshoot past target on the final step of a sweep.
    if (abs(target - current) < abs(delta)) {
        delta = target - current;
    }
    // 2026-07-21: only persist to NVS + redraw the display on the step that
    // actually reaches target, not every intermediate step - computed from
    // THIS call's own target/current snapshot, not re-read after the fact,
    // so it stays correct even if motor_set_handler() stores a brand new
    // target on the HTTP thread in between: the value this step writes is
    // simply "the angle the servo is physically at right now", which is
    // accurate regardless of what target changes to next - the ramp just
    // continues toward the new one on the following tick and persists again
    // whenever it (or a later one) is finally reached. Avoids doing either
    // on every single 60ms tick of a sweep (flash wear + latency for NVS;
    // for the display, see stepServoAngle()'s own comment for the full
    // history of why this all-or-nothing gate is what's shipped, not a
    // rate limit or a separate task) for a value that, for every step but
    // the last, is immediately superseded anyway.
    bool reaches_target = (current + delta == target);
    stepServoAngle(delta, reaches_target);
}

// Initializes the PCA9685 at 50Hz (standard analog-servo update rate) and
// drives the SG92R to its centered boot position. Must run after
// initI2CCommandChannel()'s Wire.begin(D0, D1) - reuses that same bus/pins,
// does not call Wire.begin() again. Tolerates a missing/unresponsive
// PCA9685 (logs a warning and leaves s_servo_available false) instead of
// blocking boot - see s_servo_available's own comment for why this matters.
void initServoMotor() {
    // Nothing else can be calling stepServoAngle()/servoRampPoll() yet at
    // this point in setup() (httpd isn't started until after this returns),
    // so this lock is just for consistency with every other PCA9685 access
    // path, not a real concurrency concern here.
    MutexGuard servo_lock(s_servo_mutex);

    s_servo_prefs.begin(SERVO_NVS_NAMESPACE, /*readOnly=*/false);
    // Ramp start point = wherever the servo was last actually commanded to
    // (persisted across the power cycle - see s_servo_prefs's own comment),
    // NOT a hardcoded angle - otherwise a servo last left near one extreme
    // would jump straight there before the ramp even starts. Falls back to
    // SERVO_BOOT_ANGLE itself (i.e. no ramp motion at all) on first-ever
    // boot when NVS has nothing stored yet.
    int last_angle = s_servo_prefs.getInt(SERVO_NVS_KEY_ANGLE, SERVO_BOOT_ANGLE);
    if (last_angle < SERVO_MIN_ANGLE) {
        last_angle = SERVO_MIN_ANGLE;
    } else if (last_angle > SERVO_MAX_ANGLE) {
        last_angle = SERVO_MAX_ANGLE;
    }

    Wire.setTimeOut(200);

    // 2026-07-19: retry a few times before giving up - hardware-verified
    // that a real, correctly-wired PCA9685 can still get one transient
    // ESP_ERR_INVALID_STATE (err 4) on the very first I2C write attempted
    // right after this point in setup() (heap freshly fragmented by WiFi
    // connect + the 32KB UART RX buffer from startRemoteProxy() just
    // before), with every retry seconds later succeeding fine - so this
    // isn't a real absence/wiring fault, just a momentary resource clash.
    uint8_t err = 0;
    for (int attempt = 0; attempt < 5; attempt++) {
        err = pca9685WriteReg(PCA9685_REG_MODE1, PCA9685_MODE1_SLEEP);  // must sleep before changing PRESCALE
        if (err == 0) {
            break;
        }
        delay(20);
    }
    if (err != 0) {
        log_w("initServoMotor: PCA9685 not responding after retries (I2C err %u) - servo control disabled", err);
        displayShowMotorAngle(-1);
        return;
    }
    delay(5);

    // prescale = round(25MHz internal osc / (4096 * update_rate)) - 1, per
    // the PCA9685 datasheet's own formula.
    uint8_t prescale = (uint8_t)((25000000UL / (4096UL * SERVO_PWM_FREQ_HZ)) - 1);
    pca9685WriteReg(PCA9685_REG_PRESCALE, prescale);

    pca9685WriteReg(PCA9685_REG_MODE1, PCA9685_MODE1_AI);
    delay(5);
    pca9685WriteReg(PCA9685_REG_MODE1, PCA9685_MODE1_AI | PCA9685_MODE1_RESTART);
    pca9685WriteReg(PCA9685_REG_MODE2, PCA9685_MODE2_OUTDRV);

    // Slow, stepped ramp from wherever it last was to the centered boot
    // position - see SERVO_RAMP_STEP_DEG's and last_angle's own comments
    // for why this isn't a single jump and isn't a hardcoded start point.
    rampServoTo(last_angle, SERVO_BOOT_ANGLE);
    s_servo_angle.store(SERVO_BOOT_ANGLE);
    s_servo_prefs.putInt(SERVO_NVS_KEY_ANGLE, SERVO_BOOT_ANGLE);
    s_servo_available = true;
    displayShowMotorAngle(SERVO_BOOT_ANGLE);
}

// ---------------------------------------------------------------------------
// 2026-07-19: ST7735 128x128 SPI status display (user-supplied, hardware-
// verified ST7735_XIAO.cpp/.h - see that file for the actual panel driver).
// Shows the board's WiFi IP and whether a /stream/data client is currently
// connected. CS/DC/RST are plain GPIO (per ST7735_XIAO's own design, not
// hardware-auto CS) so any pins work; SCK/MOSI use the board's default
// hardware SPI pins (D8/D10 on XIAO ESP32C3) since nothing else on this
// board uses hardware SPI (startRemoteProxy()'s PROTO_SPI case is dead code
// - PROTO_UART is what's actually selected).
//
// Pin note: DISPLAY_DC_PIN (D3) is the same pin AI.begin(&atSerial, D3)
// (see startRemoteProxy()'s PROTO_UART case) uses as WE2's reset line - not
// a real conflict since that use is a one-shot pulse-then-INPUT during
// startRemoteProxy() early in setup(), and initDisplay() is only called
// afterward (see camera_web_server.ino's setup()), by which point D3 is
// already idle/available for the display to drive as an output.
#define DISPLAY_CS_PIN  (D5)
#define DISPLAY_DC_PIN  (D3)
#define DISPLAY_RST_PIN (D2)

#define DISPLAY_COLOR_BLACK  (0x0000)
#define DISPLAY_COLOR_WHITE  (0xFFFF)
#define DISPLAY_COLOR_GREEN  (0x07E0)
#define DISPLAY_COLOR_YELLOW (0xFFE0)

static ST7735_XIAO   s_display(DISPLAY_CS_PIN, DISPLAY_DC_PIN, DISPLAY_RST_PIN);
static bool          s_display_ready = false;
// Guards SPI transactions against interleaving from concurrent callers -
// displayShowClientStatus() is called from httpd worker tasks (stream
// connect/disconnect), displayShowIP() from the main task at boot; the
// ST7735_XIAO driver itself has no locking of its own (matches this
// codebase's existing pattern of guarding shared-bus access at the call
// site - see s_cmd_mutex's own comment).
static SemaphoreHandle_t s_display_mutex = NULL;

void initDisplay() {
    s_display_mutex = xSemaphoreCreateMutex();
    s_display.begin();
    s_display.fillScreen(DISPLAY_COLOR_BLACK);
    s_display.drawText(2, 2, "WiFi IP:", DISPLAY_COLOR_WHITE, 1, DISPLAY_COLOR_BLACK, true);
    s_display.drawText(2, 40, "Client:", DISPLAY_COLOR_WHITE, 1, DISPLAY_COLOR_BLACK, true);
    s_display.drawText(2, 78, "Motor:", DISPLAY_COLOR_WHITE, 1, DISPLAY_COLOR_BLACK, true);
    s_display_ready = true;
    // Boot default, matches s_data_stream_clients' own initial value (0) -
    // updated for real once startCameraServer() is up and a client can
    // actually connect.
    s_display.drawText(2, 52, "WAITING   ", DISPLAY_COLOR_YELLOW, 2, DISPLAY_COLOR_BLACK, true);
    // Placeholder until initServoMotor() (called after initDisplay() in
    // camera_web_server.ino's setup()) determines the real angle - or
    // decides the PCA9685 isn't responding at all, see
    // displayShowMotorAngle()'s own negative-angle case.
    s_display.drawText(2, 90, "--       ", DISPLAY_COLOR_WHITE, 2, DISPLAY_COLOR_BLACK, true);
}

void displayShowIP(const char* ip) {
    if (!s_display_ready) {
        return;
    }
    MutexGuard lock(s_display_mutex);
    // Fixed-width blank-pad (up to 21 chars comfortably fits this 128px-wide
    // panel at text size 1) so a shorter new IP fully overwrites a longer
    // old one - same reasoning as displayShowClientStatus()'s own padding.
    char buf[22];
    snprintf(buf, sizeof(buf), "%-21s", ip);
    s_display.drawText(2, 14, buf, DISPLAY_COLOR_WHITE, 1, DISPLAY_COLOR_BLACK, true);
}

void displayShowClientStatus(bool connected) {
    if (!s_display_ready) {
        return;
    }
    MutexGuard lock(s_display_mutex);
    // Blank-padded to a fixed width ("CONNECTED " is the longest) so
    // switching between the two never leaves stray characters from the
    // previous, longer string un-overwritten.
    if (connected) {
        s_display.drawText(2, 52, "CONNECTED ", DISPLAY_COLOR_GREEN, 2, DISPLAY_COLOR_BLACK, true);
    } else {
        s_display.drawText(2, 52, "WAITING   ", DISPLAY_COLOR_YELLOW, 2, DISPLAY_COLOR_BLACK, true);
    }
}

// angle < 0 means "no PCA9685 responding" (see s_servo_available) - shown
// as "--" rather than a number that would otherwise look like a real,
// currently-held position.
void displayShowMotorAngle(int angle) {
    if (!s_display_ready) {
        return;
    }
    MutexGuard lock(s_display_mutex);
    char text[8];
    if (angle < 0) {
        snprintf(text, sizeof(text), "--");
    } else {
        snprintf(text, sizeof(text), "%d deg", angle);
    }
    // Blank-padded to a fixed width ("180 deg" is the longest possible
    // value) so a shorter new value never leaves stray characters behind.
    char buf[10];
    snprintf(buf, sizeof(buf), "%-9s", text);
    s_display.drawText(2, 90, buf, DISPLAY_COLOR_WHITE, 2, DISPLAY_COLOR_BLACK, true);
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
        has_consumer = (s_data_stream_clients > 0);
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
                                        [](const SlotRef& s) {
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

    // 2026-07-18: was malloc(sizeof(PtrBuffer::Slot)) - see slot_pool_acquire()'s
    // own comment for why this moved to a fixed pool.
    PtrBuffer::Slot* p_slot = slot_pool_acquire();
    if (p_slot == NULL) {
        if (type & MSG_TYPE_AUDIO) {
            audio_pool_release(pool_i);
        } else {
            free(copy);
        }
        log_e("pushPBSlot: failed to allocate slot (SLOT_POOL_SIZE=%u exhausted)...",
              (unsigned)SLOT_POOL_SIZE);
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
    // 2026-07-18: was a std::shared_ptr<PtrBuffer::Slot> built from a
    // pointer+custom-deleter constructor, which heap-allocates a separate
    // control block on every single call - see SlotRef's own comment
    // (defined earlier in this file, right after slot_pool_release()) for
    // why that was the last remaining high-frequency heap allocation in
    // this hot path and how SlotRef avoids it (intrusive refcount living
    // inside the already-pooled Slot itself). The cleanup logic that used
    // to live in the shared_ptr's deleter lambda now lives in
    // SlotRef::release().
    PB.slots.emplace_back(SlotRef(p_slot));
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
              "data_clients=%d, discarded=%u%s",
              len, type, (unsigned)free_now, (unsigned)largest_now, s_data_stream_clients.load(),
              discarded, heap_low ? " [LOW]" : "");
        // 2026-07-18: TEMP DIAGNOSTIC - alloc_blocks/free_blocks trend lets
        // us tell "genuine leak" (alloc_blocks climbing over the whole run)
        // apart from "fragmentation spike" (alloc_blocks flat, largest still
        // collapses) - see dbg_log_heap_info()'s own comment.
        dbg_log_heap_info("pushPBSlot");
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
        // 2026-07-17: was unconditional - ANY unexpected byte immediately
        // gave up on the real handshake and marked s_we2_uart1_synced=true,
        // on the assumption "WE2 must already be on UART1 sending real
        // protocol data" (true for the ESP32-reboots-while-WE2-already-
        // locked case this was written for). But if WE2 is genuinely still
        // in its own probe loop (confirmed on hardware this session, via
        // direct WE2-side console access: WE2 stuck repeating "probe write
        // on UART1, ret=1" for 90+ seconds with zero echo ever seen) and a
        // single stray/noise byte lands here first, this gave up
        // permanently for the rest of the boot - WE2 never gets its real
        // echo, keeps waiting forever, and every AT command times out even
        // though I2C delivery/WE2-side processing are both fine (WE2's
        // reply just goes out its still-USB-console output, nowhere this
        // ESP32 is listening). Only trust a byte as "already-synced"
        // evidence if it actually looks like the start of the real
        // SSCMA reply framing (RESPONSE_PREFIX = '\r', see
        // Seeed_Arduino_SSCMA.h) - any other stray byte is logged and
        // ignored, keeping this poller armed for a later genuine probe.
        if (c == '\r') {
            log_w("Unexpected byte 0x%02x (looks like SSCMA reply framing) while waiting for WE2 "
                  "UART1 handshake - treating as evidence WE2 is already live and handing off to "
                  "fetchFramedMessages()...", (unsigned)c);
            s_we2_uart1_synced = true;
            return;
        }
        log_w("Unexpected byte 0x%02x while waiting for WE2 UART1 handshake (not the probe byte, "
              "not reply-framing-shaped) - ignoring, still waiting for the real probe", (unsigned)c);
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
    // 2026-07-17 (rev 2): NOT "any byte read off atSerial" (rev 1, reverted -
    // hardware-confirmed broken). Only a byte that's part of a fully
    // recognized frame (a complete JSON reply/event, or a complete audio
    // frame) counts as proof WE2 is alive and genuinely on UART1 - see the
    // two `s_last_uart1_rx_ms = millis();` calls below, at the two `pos +=`
    // sites inside a recognized branch. Rev 1 counted ANY byte, including
    // ones immediately discarded by the "not a recognized frame start, drop
    // one byte" fallback a few lines down - and out_transport.c's periodic
    // UART1 probe byte (0x16, unrelated to any real message) is exactly
    // that: a stray byte fetchFramedMessages() reads, fails to recognize,
    // and drops, while pollWe2Uart1Handshake() itself never even looks at
    // atSerial because it early-returns whenever `s_we2_uart1_synced` is
    // already true. Confirmed on hardware: with `s_we2_uart1_synced` stuck
    // true from before a WE2-only reboot (the exact scenario
    // WE2_RESYNC_TIMEOUT_THRESHOLD exists to recover from), WE2's probe
    // bytes alone (5+s apart, `out_transport: probe write... ret=1` logged
    // 339 times with zero echoes over one 5-minute test) kept refreshing
    // rev 1's "still alive" belief just often enough that
    // WE2_RESYNC_SILENCE_MS's countdown never ran out, permanently blocking
    // the resync this scenario needs - every single AT+ASR attempt timed
    // out (verified via WE2's own console: it received and replied to all
    // 5 I2C-delivered attempts correctly, but every reply went to its still-
    // USB-attached console since the handshake had never switched output to
    // UART1). Real recognized traffic doesn't have this problem: it can
    // only exist after the handshake has already succeeded once.

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
                s_last_uart1_rx_ms = millis();
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
            s_last_uart1_rx_ms = millis();
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
 * entirely per explicit direction) - but esp_http_server gives each
 * httpd_start() instance exactly one worker task, which serializes every
 * handler registered on it. Confirmed on hardware: a single client wanting
 * both streams at once saw the second connection's handler never even get
 * scheduled - accepted at the TCP level, then queued indefinitely - until
 * the first one's handler function returned. Split onto two separate
 * instances, each its own worker task on its own port, so bbox and audio
 * streaming genuinely ran in parallel instead of taking turns.
 *
 * 2026-07-18: split back into ONE instance again (`data_httpd`, replacing
 * both `result_httpd`/`audio_httpd`) - root-caused that session that two
 * simultaneous NEW connections, one per port, spiked heap sharply enough at
 * accept-time to collapse the WiFi/httpd stack under repeated combined-load
 * cycling (see esp32_camera_web_server_bridge memory, 2026-07-18 section).
 * stream_data_handler() now interleaves both AUDIO and EVENT/bbox traffic
 * over one connection/worker/accept() instead - the "taking turns" problem
 * this split originally fixed doesn't recur, because it's no longer two
 * independent client connections queuing behind one worker; it's one
 * handler's own poll loop checking both data sources every iteration. */
httpd_handle_t data_httpd = NULL;  // /stream/data (AUDIO + EVENT/bbox, merged)

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
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
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
 * stream_data_handler() below needs it earlier in the file than that
 * definition lives. */
static void sendBreakBestEffort();

/* 2026-07-12: WE2's AT+BREAK is a universal stop - it kills the camera
 * INVOKE/SAMPLE loop AND the audio ASAMPLE loop together, there's no
 * modality-specific stop (see audio_stop_handler's own comment on this).
 * stream_data_handler() calls this exactly once, at its single exit point,
 * once its own client is gone - on the assumption that once nobody's
 * watching, WE2 should stop wasting power/NPU cycles.
 *
 * 2026-07-18: was two counters (`s_event_stream_clients`/
 * `s_audio_stream_clients`, one per then-separate `/stream/result`+
 * `/stream/audio` httpd instance) with a TOCTOU-safe decrement-then-check-
 * both dance, needed because two independent worker tasks could each
 * disconnect at the same instant. Now that both traffic types share ONE
 * `/stream/data` connection/handler/task (see s_data_stream_clients' own
 * comment - merged specifically to remove the simultaneous-dual-accept
 * heap spike that caused, not just to simplify this), there is only ever
 * one caller of this function at a time - a plain decrement-then-check is
 * sufficient again. */

// 2026-07-19: SECOND attempt at httpd_sess_trigger_close(), this time
// genuinely ASYNC (fired from esp_timer's own dedicated task, not from
// inside stream_data_handler() itself) - TRIED AND REVERTED AGAIN. Hardware
// result: measurably WORSE than the drop-streak-timeout fix alone (20-cycle
// reconnect test collapsed by cycle 2, vs cycle 16 without this). Root
// cause of the regression, confirmed via dual before/after console
// timestamps: the timer was armed with only a 10ms delay, intended just to
// satisfy "runs on a different task" - but 10ms is far shorter than
// stream_data_handler()'s OWN teardown sequence (NULL-chunk terminator +
// disconnectStreamClient() - measured ~1000ms end to end on the same
// hardware run), so the async trigger fired WHILE the owning task was still
// actively using the same socket, not after. Both this codebase's own first
// (synchronous) attempt at this API AND this second (nominally async, but
// still racing) attempt regressed things, on two separate hardware tests -
// treat httpd_sess_trigger_close() as not viable in this codebase without
// a much more careful hand-off (e.g. a real cross-task signal that only
// fires after stream_data_handler() has FULLY returned, not a short fixed
// delay) - not attempted further this session. The drop-streak-timeout
// force-disconnect (see DROP_STREAK_LIMIT_MS above) is real, measured
// progress on its own (crash pushed from cycle 7 to cycle 16) and is kept;
// this file no longer calls httpd_sess_trigger_close() anywhere.

// 2026-07-19: THIRD attempt at reclaiming a stuck TCP send buffer, after
// both httpd_sess_trigger_close() attempts regressed things (see this
// file's own comment just above) - TRIED AND REVERTED TOO. This one skipped
// the esp_http_server session-close API entirely and worked one layer down:
// setsockopt(SO_LINGER, {1,0}) + close() the raw fd directly, synchronously,
// from the same task that owns it, specifically to avoid the cross-task
// race the first two attempts had. Hardware result: same class of
// regression as the other two - 20-cycle reconnect test collapsed by cycle
// 2 again. Diagnostic detail worth keeping: setsockopt(SO_LINGER) itself
// FAILED (errno=109) - this lwIP socket layer doesn't support SO_LINGER at
// all, so the following close() was just an ordinary graceful close, not
// the intended abortive RST - and even that plain close(), called on a
// fd the framework still considers open/owned, made things worse than not
// closing it at all. Three different mechanisms (framework API sync,
// framework API async, raw POSIX close) have now all regressed this exact
// test - the consistent conclusion is that esp_http_server in this
// ESP-IDF/Arduino version does not tolerate the app touching a session's
// socket from outside the handler's own normal return path, full stop.
// Not attempting a fourth variant of "close it ourselves" - the
// drop-streak-timeout disconnect (DROP_STREAK_LIMIT_MS above), which only
// ever returns a non-OK code and lets the framework do its own cleanup in
// its own time, is the version worth keeping.

static void disconnectStreamClient(const char* reason) {
    s_data_stream_clients.fetch_sub(1);
    // TEMP DIAGNOSTIC (2026-07-17): tracking down a real, confirmed heap
    // leak/fragmentation that only shows up across many repeated
    // stream-connect/disconnect cycles (not visible within one clean run -
    // see esp32_camera_web_server_bridge memory). Logs `reason` (one of the
    // log strings each break site already had) so a clean close vs. a
    // timeout/error-driven one can be told apart in the data. Remove once
    // resolved.
    dbg_log_heap_info(reason);
    log_w("DBG disconnectStreamClient (counter after decrement=%d, reason=%s)",
          (int)s_data_stream_clients.load(), reason);
    if (s_data_stream_clients.load() <= 0) {
        sendBreakBestEffort();
        displayShowClientStatus(false);
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

// 2026-07-20: writev() with retry-until-complete-or-real-failure, used by
// stream_data_handler()'s combined audio+bbox send (see that call site's own
// comment for why it hand-rolls chunk framing instead of using
// httpd_resp_send_chunk() per item). A single httpd_resp_send_chunk() call
// loops internally until done or SO_SNDTIMEO fires once, reporting a clean
// all-or-nothing result - sending several buffers back to back gives no
// such guarantee, so a short partial write has to be retried on the
// REMAINING unsent bytes here. Once ANY byte of a chunk has hit the wire,
// the hex chunk-length already written is a promise to the client that
// can't be walked back - so unlike the drop-before-sending path in the
// caller (which can still cleanly skip a whole chunk before ever calling
// this), a partial write here can only end in "eventually finish" or "give
// up and force a disconnect" (returning false), never "drop and continue" -
// the caller must treat false as a hard failure, not a droppable one,
// regardless of what errno says.
//
// 2026-07-20: this was originally a single writev() call per attempt (true
// scatter-gather, one syscall for the whole iovec array) - REVERTED after a
// real build on this toolchain failed at link time with "undefined
// reference to `writev'": the ESP32 Arduino core's lwIP sockets shim
// doesn't implement it, confirmed by the linker, not just a missing
// declaration. Falls back to sending each iovec entry with its own send()
// call instead - still zero-copy (no memcpy of audio/bbox data), still one
// hand-rolled chunk frame covering everything collected this pass instead
// of routing each item through httpd_resp_send_chunk() (which itself does
// up to 3 of its own internal sends per call - header line, payload,
// trailing CRLF) - just N send() calls instead of 1 writev() call, not the
// pure single-syscall version originally intended.
static bool writev_retrying(int fd, struct iovec* iov, int iovcnt, uint32_t deadline_ms, int* out_errno) {
    while (iovcnt > 0) {
        errno     = 0;
        ssize_t n = send(fd, iov[0].iov_base, iov[0].iov_len, 0);
        *out_errno = errno;
        if (n > 0) {
            size_t remaining = (size_t)n;
            if (remaining < iov[0].iov_len) {
                iov[0].iov_base = (char*)iov[0].iov_base + remaining;
                iov[0].iov_len -= remaining;
            } else {
                iov++;
                iovcnt--;
            }
            continue;
        }
        if (*out_errno == EAGAIN || *out_errno == EWOULDBLOCK || *out_errno == ETIMEDOUT) {
            if (millis() >= deadline_ms) {
                return false;
            }
            continue;  // each attempt already bounded by the socket's own SO_SNDTIMEO
        }
        return false;  // real error (ECONNRESET, EPIPE, ENOTCONN, ...)
    }
    return true;
}

/* 2026-07-18: merged former stream_audio_handler()+stream_result_handler()
 * into ONE connection/handler/worker task - see s_data_stream_clients' own
 * comment for why (root-caused: two simultaneous NEW connections, one per
 * port, spiked heap sharply enough at accept-time to collapse the WiFi/
 * httpd stack under repeated combined-load cycling). Both AUDIO (raw framed
 * PCM, see MSG_TYPE_AUDIO's comment) and EVENT/bbox (one INVOKE JSON line
 * per detection batch, `\r\n`-terminated) traffic now share this single
 * `application/octet-stream` chunked body. No new envelope invented - a
 * client demuxes exactly the way this sketch's own fetchFramedMessages()
 * already does on the UART1 side: check for the audio magic
 * (`0xFF 'S' 'M' 'B'`) first, otherwise it's a JSON line ending in `\r\n`
 * (buildInvokeJson()'s output always starts with `{`, which can never
 * collide with the audio magic's leading 0xFF byte). Sends every
 * not-yet-sent slot of both types per poll, audio first then bbox -
 * dropping either would leave gaps downstream. */
static esp_err_t stream_data_handler(httpd_req_t* req) {
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
    esp_err_t        res          = ESP_OK;
    size_t           audio_last_id = 0;
    static uint64_t  bbox_last_id  = 0;

    res = httpd_resp_set_type(req, "application/octet-stream");
    if (res != ESP_OK) {
        return res;
    }
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");

    // 2026-07-17: Nagle's algorithm (on by default) can hold a small chunk
    // send for up to ~40ms waiting to coalesce with more data or an ACK -
    // audio frames arrive ~125ms apart and each is sent as its own chunk, so
    // that's a meaningful bite out of the inter-frame budget on every single
    // send, compounding into AUDIO_POOL_SLOTS exhaustion ("both pool slots
    // busy" - see esp32_camera_web_server_bridge memory) under sustained
    // streaming. This is a one-way, low-latency-sensitive relay, not a use
    // case Nagle's coalescing-for-bandwidth-efficiency trade-off suits at
    // all - disable it on this stream's socket.
    {
        int fd = httpd_req_to_sockfd(req);
        if (fd >= 0) {
            int one = 1;
            if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) != 0) {
                log_w("stream_data_handler: setsockopt(TCP_NODELAY) failed, errno=%d", errno);
            }
            // 2026-07-18: root-caused on hardware (send_chunk heap-delta
            // diagnostic) - with no send timeout, a client that can't drain
            // fast enough leaves httpd_resp_send_chunk() blocked for SECONDS
            // (measured: ~6s with zero send attempts logged) before the
            // socket layer finally errors out on its own. During that whole
            // window this task can't drain audio_pending, so it keeps
            // pool-exhausting new WE2 audio (see "both pool slots busy") and
            // the slow client silently drags everything down with it.
            // Explicit direction: WE2's send rate is authoritative - a slow
            // client should have its OWN data dropped, not stall the whole
            // pipe. A short send timeout turns an indefinite block into a
            // bounded one, so the per-chunk drop logic below (see the
            // EAGAIN/EWOULDBLOCK/ETIMEDOUT branch) gets a chance to run
            // instead of the task just sitting there.
            struct timeval snd_timeout;
            snd_timeout.tv_sec  = 0;
            snd_timeout.tv_usec = 200 * 1000;  // 200ms - a few chunk intervals, not a full stall
            if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &snd_timeout, sizeof(snd_timeout)) != 0) {
                log_w("stream_data_handler: setsockopt(SO_SNDTIMEO) failed, errno=%d", errno);
            }

            // 2026-07-19: TRIED tight TCP keepalive (SO_KEEPALIVE +
            // TCP_KEEPIDLE=2/TCP_KEEPINTVL=1/TCP_KEEPCNT=3), motivated by
            // github.com/espressif/esp-idf issue #8668 ("LwIP socket:
            // unwanted behaviour and memory leak after 'pulling-the-plug'
            // event", Espressif-confirmed, "Won't Do") - REVERTED again
            // after a THIRD 20-cycle test result, this time with the test
            // tooling's own bugs fixed first (see below). Results across all
            // three runs of this exact unchanged config: cycle 1, cycle 18,
            // cycle 1 - the cycle-18 run looks like the outlier, not the
            // norm, once a third data point was collected. The cycle-18
            // run's apparent win is what originally motivated re-testing
            // with fixed tooling in the first place: the session scratchpad
            // test scripts (coverage_test.py/matrix11.py) AND this repo's
            // own client/media_client.py all had a pure-Python bit-loop
            // CRC16 (~60ms/8KB audio frame, benchmarked on the Orange Pi
            // host) that could make the TEST SCRIPT ITSELF intermittently
            // the "slow client" under host CPU load, independent of real
            // WiFi/firmware behavior - a very plausible source of this
            // session's wide run-to-run variance. Fixed to table-driven
            // CRC16 (~6x faster) in all three files - but even with that
            // fixed, this exact keepalive config still collapsed by cycle 1
            // on the very next test. Net: not ruled out as harmful, but no
            // longer trusted as a proven win either - reverted back to
            // DROP_STREAK_LIMIT_MS alone (this file's other real,
            // consistently-kept fix) pending a clean baseline-vs-keepalive
            // comparison using the now-fixed test tooling on both sides.
        }
    }

    // See s_data_stream_clients' own comment - pushPBSlot() checks this to
    // decide whether AUDIO backlog is worth retaining at all.
    s_data_stream_clients += 1;
    displayShowClientStatus(true);
    // TEMP DIAGNOSTIC (2026-07-17) - see disconnectStreamClient()'s comment.
    log_w("DBG stream_data_handler CONNECT heap: free=%u largest=%u",
          (unsigned)heap_caps_get_free_size(MALLOC_CAP_8BIT),
          (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT));

    // 2026-07-12: hard backstop against client_gone()'s MSG_PEEK detection
    // not firing promptly. Confirmed on hardware via targeted tracing: a
    // client that disconnected (curl -m 8, client-side timeout) left this
    // handler's idle loop calling client_gone() every 5ms for over FIFTY
    // SECONDS before it finally recognized the peer was gone - during that
    // whole window the single worker task stayed occupied, so every other
    // connection attempt to this port just queued at the TCP level
    // (accepted, per max_open_sockets) without ever actually being
    // serviced. Root cause of the slow detection itself wasn't pinned down
    // - this is a pragmatic ceiling regardless of why: real traffic arrives
    // every ~125-250ms when camera/audio is running, so genuinely nothing
    // new for 10 straight seconds already means either nobody's watching or
    // something's stuck - safe to just close and let a fresh connection
    // start clean either way.
    TickType_t idle_since               = xTaskGetTickCount();
    const TickType_t IDLE_TIMEOUT_TICKS = pdMS_TO_TICKS(10000);
    const char*       disconnect_reason  = "loop_exit_unexpected";  // see disconnectStreamClient()'s DBG comment
    // 2026-07-19: see the idle-timeout check below (where this is read) -
    // tracks s_last_uart1_rx_ms so a genuinely-alive-but-quiet WE2 (real
    // traffic arriving, just nothing that lands in audio_pending/bbox_count)
    // isn't treated the same as a dead one.
    uint32_t last_seen_uart1_rx_ms = s_last_uart1_rx_ms;

    // 2026-07-18: pending audio/bbox queues now live OUTSIDE the outer
    // while loop (were re-collected fresh every iteration) so a big bbox
    // burst can be capped and spread across several outer-loop passes
    // instead of drained in one shot - see BBOX_BURST_CAP's own comment
    // below for why a per-pass cap turned out to be necessary even after
    // interleaving audio+bbox sends (2026-07-18 earlier fix): interleaving
    // alone provides ZERO protection for a camera-ONLY connection (no
    // audio ever available to interleave with), and a real hardware test
    // (11-combo matrix, camera-only resolution=0) reproduced the collapse
    // through exactly that gap - heap crashed to free=7092/largest=1652
    // within the first 30s of a pure camera-only run.
    std::vector<SlotRef> audio_pending;
    size_t                                        audio_idx = 0;
    BboxEntry                                     bbox_entries[BBOX_RING_SLOTS];
    size_t                                        bbox_count = 0;
    size_t                                        bbox_pos   = 0;
    // 2026-07-20: the very first chunk of the whole connection still goes
    // through httpd_resp_send_chunk() (see the combined-send block below) -
    // that's the call that makes esp_http_server send the status
    // line/headers, which the raw writev() path used for every later chunk
    // never does.
    bool first_chunk_sent = false;

    // 2026-07-19: ROOT CAUSE of the "heap freezes at a fixed value and
    // never recovers, even though drops are firing" signature - hardware-
    // confirmed via a 20-cycle rapid reconnect test (crashed by cycle 7,
    // heap frozen at free=8568/largest=1652 for 30+ seconds straight while
    // "dropping batch"/"dropping frame" kept firing every ~1s). The
    // EAGAIN/EWOULDBLOCK/ETIMEDOUT drop-and-continue path (see the two drop
    // sites below) correctly stops THIS task from blocking further, but it
    // does NOT reclaim memory already queued in the OS/lwIP TCP send buffer
    // from EARLIER successful-looking httpd_resp_send_chunk() calls (res==
    // ESP_OK just means the OS accepted the bytes into its own send queue,
    // not that the peer has actually read/ACKed them yet) - that backlog
    // only gets freed when the peer drains it or the socket is torn down.
    // A client that's merely "a little slow" recovers within a pass or two
    // (this session's many successful runs prove that) - a client that's
    // genuinely stuck/gone just keeps producing EAGAIN forever, and
    // dropping one item at a time was never going to reclaim that stuck
    // backlog. Track how long a streak of consecutive drops (either type)
    // has been going; once it's been continuous for DROP_STREAK_LIMIT_MS,
    // stop treating it as "a bit slow" and treat it as a real disconnect
    // instead - closing the socket is what actually lets the OS reclaim
    // the stuck send-buffer memory.
    uint32_t drop_streak_start_ms = 0;
#define DROP_STREAK_LIMIT_MS (3000u)
    // 2026-07-19: TRIED skipping the NULL-chunk terminator send on the
    // drop-streak-exceeded path (reasoning: a client that's been producing
    // EAGAIN/timeout for a solid DROP_STREAK_LIMIT_MS is already confirmed
    // dead, so sending it one more chunk just costs another SO_SNDTIMEO
    // wait for nothing) - REVERTED. Confirmed on hardware the skip itself
    // worked as intended (teardown went from ~1000ms to near-instant, per
    // adjacent send_failed/disconnectStreamClient log timestamps), but the
    // 20-cycle reconnect test still came out same-or-worse (cycle 4) than
    // the plain drop-streak-timeout baseline (cycle 16) - heap still didn't
    // recover after the faster disconnect either. Net: a logically sound,
    // measurably-faster-teardown change with no measured stability benefit
    // this session - not worth the added code path. Kept simple: always
    // send the terminator, exactly as before DROP_STREAK_LIMIT_MS existed.

    // Caps how many bbox batches get sent per outer-loop pass, regardless
    // of how many are queued or whether any audio is available to
    // interleave with. WE2 delivers bbox in bursts up to BBOX_RING_SLOTS
    // (20) at once, especially right after a camera restart while auto-
    // exposure/gain is still settling - draining all 20 in one uninterrupted
    // run of httpd_resp_send_chunk() calls (the old behavior, and still the
    // behavior of the pure-interleave fix when there's no audio to pair
    // each bbox send with) is what spikes heap/network pressure sharply
    // enough to collapse the connection. Small on purpose: forces the
    // outer loop back to its top (re-check client_gone(), let the
    // scheduler/network stack breathe) every few sends instead of
    // blasting a whole burst through unchecked.
#define BBOX_BURST_CAP (3)

    // 2026-07-20: replaces the old single reused s_bbox_buf[RST_BUFFER_SIZE
    // =4096] - the combined send below needs up to BBOX_BURST_CAP JSON
    // batches to coexist (as separate zero-copy iovecs) at once instead of
    // one at a time, so each slot's own cap is trimmed to
    // buildInvokeJson()'s real ceiling (BBOX_RING_SLOTS(20) x
    // (BBOX_ELEM_CAP+2) + JSON wrapper, comfortably under 1024) rather than
    // reusing RST_BUFFER_SIZE's generous headroom. Net static cost actually
    // DROPS vs the buffer it replaces: BBOX_BURST_CAP*1024 = 3072 < 4096.
#define BBOX_BATCH_CAP (1024)
    static char s_bbox_batch_buf[BBOX_BURST_CAP][BBOX_BATCH_CAP];

    while (res == ESP_OK) {
        if (audio_idx >= audio_pending.size() && bbox_pos >= bbox_count) {
            audio_pending.clear();
            audio_idx = 0;
            {
                xSemaphoreTake(PB.mutex, portMAX_DELAY);
                for (auto& s : PB.slots) {
                    if (s->id > audio_last_id && s->type == MSG_TYPE_AUDIO) {
                        audio_pending.push_back(s);
                    }
                }
                xSemaphoreGive(PB.mutex);
            }
            bbox_count = drainBboxSince(&bbox_last_id, bbox_entries);
            bbox_pos   = 0;

            if (audio_pending.empty() && bbox_count == 0) {
                if (client_gone(req)) {
                    log_w("stream_data_handler: client gone while idle, stopping WE2 stream...");
                    disconnect_reason = "client_gone_idle";
                    break;
                }
                // 2026-07-19: idle_since used to only reset when there was
                // actual audio/bbox DATA to send - but cvapp.cpp's
                // cam_handle_frame() now always sends a lightweight INVOKE
                // heartbeat even with an empty "boxes": [] (see its own
                // 2026-07-19 comment), specifically so a healthy-but-quiet
                // WE2 (sparse scene, no detections for a while) isn't
                // indistinguishable from a genuinely dead one. Hardware-
                // confirmed this session: a sparse scene alone produced 16+s
                // with zero NEW detections, well past the old 10s threshold,
                // and this handler force-disconnected a perfectly healthy
                // stream as a result. s_last_uart1_rx_ms already tracks the
                // timestamp of the last COMPLETE recognized UART1 message
                // regardless of its content (see fetchFramedMessages()) - an
                // empty-boxes heartbeat updates it exactly like any other
                // message, even though it produces zero ring entries
                // (drainBboxSince() has nothing to return). Treating fresh
                // UART1 traffic as "not idle", not just fresh ring entries,
                // is what actually lets that heartbeat do its job.
                if (s_last_uart1_rx_ms != last_seen_uart1_rx_ms) {
                    last_seen_uart1_rx_ms = s_last_uart1_rx_ms;
                    idle_since             = xTaskGetTickCount();
                } else if ((xTaskGetTickCount() - idle_since) >= IDLE_TIMEOUT_TICKS) {
                    log_w("stream_data_handler: idle timeout (10s with nothing new), "
                          "stopping WE2 stream as a backstop...");
                    disconnect_reason = "idle_timeout";
                    break;
                }
                // 2026-07-17: was 5ms (200Hz) in the old audio-only handler -
                // see the old stream_result_handler's identical reasoning
                // (removed with the merge, kept here): quarters this loop's
                // CPU/scheduling footprint on this board's single core with
                // no perceptible latency cost.
                vTaskDelay(20 / portTICK_PERIOD_MS);
                continue;
            }
            idle_since = xTaskGetTickCount();
        }

        // 2026-07-12: under continuous traffic this loop never goes idle,
        // so the client_gone() check above never runs - a dead peer left
        // this task blocked inside httpd_resp_send_chunk() for 58+ seconds
        // on hardware because the socket's own send-side didn't error out
        // promptly. Re-check (cheap, non-blocking MSG_PEEK) right before
        // every send pass too, not just when idle.
        if (client_gone(req)) {
            log_w("stream_data_handler: client gone (detected before active send), stopping WE2 stream...");
            disconnect_reason = "client_gone_active";
            break;
        }

        // 2026-07-20: collect this pass's whole audio_pending remainder plus
        // up to BBOX_BURST_CAP bbox batches, then send them as ONE combined
        // HTTP chunk via a single writev() syscall instead of one
        // httpd_resp_send_chunk() call per item - see writev_retrying()'s
        // own comment for the retry/failure semantics, and this function's
        // header comment for why hand-rolling chunk framing here is judged
        // safe despite esp_http_server shipping as a prebuilt .a on this
        // board (no source available locally to verify byte-for-byte).
        bool   send_failed = false;
        int    fd          = httpd_req_to_sockfd(req);

        // 2026-07-12 (unchanged rationale): rebuilds the entries copied out
        // of the ring (under s_bbox_mutex, inside drainBboxSince()) into one
        // INVOKE JSON line PER BATCH. Each batch now gets its OWN slot (see
        // BBOX_BATCH_CAP's own comment) instead of reusing one buffer, so
        // all of them can coexist as separate zero-copy iovecs below.
        size_t bbox_batch_len[BBOX_BURST_CAP];
        size_t bbox_batch_n = 0;
        while (bbox_pos < bbox_count && bbox_batch_n < BBOX_BURST_CAP) {
            size_t out_len  = 0;
            size_t consumed = buildInvokeJson(&bbox_entries[bbox_pos], bbox_count - bbox_pos,
                                               bbox_entries[bbox_pos].id,
                                               s_bbox_batch_buf[bbox_batch_n], BBOX_BATCH_CAP, &out_len);
            if (consumed == 0) {
                break;  // shouldn't happen (bbox_count - bbox_pos > 0 here), safety backstop
            }
            bbox_pos += consumed;
            if (out_len == 0) {
                continue;  // nothing to send for this batch, but still consumed
            }
            size_t termi_len = strlen(MSG_TERMI_STR);
            if (out_len + termi_len <= BBOX_BATCH_CAP) {
                memcpy(s_bbox_batch_buf[bbox_batch_n] + out_len, MSG_TERMI_STR, termi_len);
                bbox_batch_len[bbox_batch_n] = out_len + termi_len;
            } else {
                bbox_batch_len[bbox_batch_n] = out_len;  // shouldn't happen, see BBOX_BATCH_CAP
            }
            bbox_batch_n++;
        }

        if (audio_idx < audio_pending.size()) {
            // 2026-07-17 (unchanged rationale): advance audio_last_id before
            // any send attempt so a dropped/failed pass is never re-added
            // from PB.slots next time around.
            audio_last_id = audio_pending.back()->id;
        }

        size_t bbox_start = 0;  // batches at [0, bbox_start) already sent via the first-chunk peel below

        // 2026-07-20: the very first chunk of the whole connection must
        // still go through httpd_resp_send_chunk() - see first_chunk_sent's
        // own comment. Peel off exactly one item (audio preferred, matching
        // this handler's existing audio-first-then-bbox ordering) and send
        // it the old way; everything else in this pass, and every later
        // pass, goes through the combined writev() path below.
        if (!first_chunk_sent && (audio_idx < audio_pending.size() || bbox_batch_n > 0)) {
            bool        first_is_audio = (audio_idx < audio_pending.size());
            const char* first_buf      = first_is_audio ? (const char*)audio_pending[audio_idx]->data
                                                          : s_bbox_batch_buf[0];
            size_t      first_len      = first_is_audio ? audio_pending[audio_idx]->size : bbox_batch_len[0];
            errno                      = 0;
            res                        = httpd_resp_send_chunk(req, first_buf, first_len);
            int first_errno            = errno;
            if (res == ESP_OK) {
                first_chunk_sent      = true;
                drop_streak_start_ms  = 0;
                if (first_is_audio) {
                    audio_pending[audio_idx].reset();
                    audio_idx++;
                } else {
                    bbox_start = 1;
                }
            } else if (first_errno == EAGAIN || first_errno == EWOULDBLOCK || first_errno == ETIMEDOUT) {
                uint32_t now_ms = millis();
                if (drop_streak_start_ms == 0) {
                    drop_streak_start_ms = now_ms;
                }
                if (now_ms - drop_streak_start_ms >= DROP_STREAK_LIMIT_MS) {
                    log_e("stream_data_handler: drop streak exceeded %ums (first chunk) - "
                          "client presumed dead, forcing disconnect to reclaim TCP "
                          "send buffer - see DROP_STREAK_LIMIT_MS's own comment",
                          (unsigned)DROP_STREAK_LIMIT_MS);
                    send_failed = true;
                } else {
                    log_w("stream_data_handler: first-chunk send timed out (client too slow) - "
                          "dropping, errno=%d", first_errno);
                    if (first_is_audio) {
                        audio_pending[audio_idx].reset();
                        audio_idx++;
                    } else {
                        bbox_start = 1;
                    }
                }
                res = ESP_OK;
            } else {
                log_e("stream_data_handler: first-chunk send hard-failed (socket gone) - "
                      "errno=%d, res=%d", first_errno, (int)res);
                send_failed = true;
            }
        }

        if (!send_failed) {
            size_t audio_start = audio_idx;
            size_t total_len   = 0;
            for (size_t i = audio_start; i < audio_pending.size(); i++) {
                total_len += audio_pending[i]->size;
            }
            for (size_t i = bbox_start; i < bbox_batch_n; i++) {
                total_len += bbox_batch_len[i];
            }

            if (total_len > 0) {
                char chunk_hdr[16];
                int  chunk_hdr_len = snprintf(chunk_hdr, sizeof(chunk_hdr), "%x\r\n", (unsigned)total_len);

                struct iovec iov[1 + AUDIO_POOL_SLOTS + BBOX_BURST_CAP + 1];
                int          iovcnt          = 0;
                iov[iovcnt].iov_base         = chunk_hdr;
                iov[iovcnt].iov_len          = (size_t)chunk_hdr_len;
                iovcnt++;
                for (size_t i = audio_start; i < audio_pending.size(); i++) {
                    iov[iovcnt].iov_base = (void*)audio_pending[i]->data;
                    iov[iovcnt].iov_len  = audio_pending[i]->size;
                    iovcnt++;
                }
                for (size_t i = bbox_start; i < bbox_batch_n; i++) {
                    iov[iovcnt].iov_base = s_bbox_batch_buf[i];
                    iov[iovcnt].iov_len  = bbox_batch_len[i];
                    iovcnt++;
                }
                iov[iovcnt].iov_base = (void*)"\r\n";
                iov[iovcnt].iov_len  = 2;
                iovcnt++;

                bool already_exhausted = (drop_streak_start_ms != 0) &&
                                          (millis() - drop_streak_start_ms >= DROP_STREAK_LIMIT_MS);
                if (already_exhausted || fd < 0) {
                    log_e("stream_data_handler: drop streak exceeded %ums (combined send) - "
                          "client presumed dead, forcing disconnect to reclaim TCP "
                          "send buffer - see DROP_STREAK_LIMIT_MS's own comment",
                          (unsigned)DROP_STREAK_LIMIT_MS);
                    send_failed = true;
                } else {
                    int      wv_errno       = 0;
                    uint32_t send_start_ms  = millis();
                    bool     ok             = writev_retrying(fd, iov, iovcnt, millis() + DROP_STREAK_LIMIT_MS, &wv_errno);
                    uint32_t send_dur_ms    = millis() - send_start_ms;
                    // 2026-07-20: mirrors media_client.py's own ">30ms"
                    // TIMING log (client-side resp.read() etc.) so the two
                    // sides' logs can be lined up by timestamp - answers
                    // whether a slow client-observed resp.read() actually
                    // corresponds to THIS handler being slow to hand the
                    // bytes off, or whether the delay is happening further
                    // downstream (WiFi radio/link) after this call already
                    // returned quickly.
                    if (send_dur_ms > 30) {
                        log_w("DBG combined send TIMING: took %ums (audio=%u bbox=%u "
                              "total_bytes=%u ok=%d errno=%d)",
                              (unsigned)send_dur_ms, (unsigned)(audio_pending.size() - audio_start),
                              (unsigned)(bbox_batch_n - bbox_start), (unsigned)total_len, (int)ok, wv_errno);
                    }
                    if (ok) {
                        for (size_t i = audio_start; i < audio_pending.size(); i++) {
                            audio_pending[i].reset();
                        }
                        audio_idx             = audio_pending.size();
                        bbox_batch_n          = 0;
                        drop_streak_start_ms  = 0;
                    } else if (wv_errno == EAGAIN || wv_errno == EWOULDBLOCK || wv_errno == ETIMEDOUT) {
                        uint32_t now_ms = millis();
                        if (drop_streak_start_ms == 0) {
                            drop_streak_start_ms = now_ms;
                        }
                        log_w("stream_data_handler: combined send timed out (client too slow) - "
                              "dropping pass (audio=%u bbox=%u total_bytes=%u), errno=%d",
                              (unsigned)(audio_pending.size() - audio_start),
                              (unsigned)(bbox_batch_n - bbox_start), (unsigned)total_len, wv_errno);
                        for (size_t i = audio_start; i < audio_pending.size(); i++) {
                            audio_pending[i].reset();
                        }
                        audio_idx    = audio_pending.size();
                        bbox_batch_n = 0;
                    } else {
                        log_e("stream_data_handler: combined send hard-failed (socket gone) - "
                              "audio=%u bbox=%u total_bytes=%u errno=%d",
                              (unsigned)(audio_pending.size() - audio_start),
                              (unsigned)(bbox_batch_n - bbox_start), (unsigned)total_len, wv_errno);
                        send_failed = true;
                    }
                }
            }
        }

        if (send_failed) {
            log_e("stream_data_handler: send failed - client disconnected, stopping WE2 stream...");
            disconnect_reason = "send_failed";
            break;
        }
    }
#undef BBOX_BURST_CAP
#undef DROP_STREAK_LIMIT_MS

    // 2026-07-18: ROOT CAUSE FOUND this session (raw-socket byte-rate probe,
    // held one connection open across restart cycles - see
    // esp32_camera_web_server_bridge memory's "Experiment A" section) - this
    // handler's chunked-transfer-encoding response was NEVER explicitly
    // terminated (no trailing zero-length chunk) before returning. That's
    // harmless on the send_failed path (the client's TCP side is already
    // confirmed gone by then, nothing left to tell it), but on the
    // SERVER-initiated exit paths - idle_timeout, client_gone_idle/active -
    // the client can still be alive and actively waiting for either more
    // chunks or a proper end to the response. Confirmed on hardware: a
    // client held open across an idle_timeout self-close never saw ANY
    // socket-level close signal (recv() just blocked forever, no FIN) even
    // though the server had already decremented s_data_stream_clients and
    // moved on - a real "zombie" connection from the client's perspective.
    // This is very likely the actual mechanism behind the "wifi+stream
    // switch not releasing cleanly" root cause hunted most of this session:
    // a client that (reasonably) treats a stalled connection as dead and
    // reconnects would pile a genuinely-new connection on top of this
    // zombie one, which the framework may still be holding/pooling for
    // reuse - repeated over many cycles, this is exactly the kind of
    // per-cycle resource accumulation this file's very first investigation
    // into this bug already suspected. Best-effort: if the client truly is
    // already gone (send_failed path), this just fails again harmlessly.
    httpd_resp_send_chunk(req, NULL, 0);

    // 2026-07-18: TRIED httpd_sess_trigger_close(req->handle,
    // httpd_req_to_sockfd(req)) here, called synchronously from inside this
    // same handler right before it returns anyway - REVERTED, made things
    // measurably worse on hardware (combined camera+audio reconnect-cycling
    // collapsed by cycle 3 instead of cycle 5-6). esp_http_server's own doc
    // comment on this API says it's "only required in special circumstances
    // wherein some application requires to close an httpd client session
    // ASYNCHRONOUSLY" - i.e. from a DIFFERENT task than the one owning the
    // session. Calling it synchronously from the owning handler's own task,
    // milliseconds before that same handler's normal return would trigger
    // the framework's own cleanup anyway, plausibly queues a redundant/
    // conflicting close request that races with (or duplicates) the normal
    // teardown path - not confirmed exactly how, but the hardware result
    // was unambiguous. Left the NULL-chunk terminator above (that one did
    // NOT regress anything) - if session-close needs revisiting, do it from
    // a genuinely separate task/context, matching what the API doc actually
    // asks for, not from within stream_data_handler() itself.

    disconnectStreamClient(disconnect_reason);

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
static SlotRef sendTaggedCommand(const char* body, size_t body_len, char* cmd_tag_buf,
                                                            size_t cmd_tag_buf_size,
                                                            uint32_t timeout_ms = RESULT_TIMEOUT_MS) {
    // 2026-07-18: see s_cmd_mutex's own comment - this function is
    // reachable concurrently from two different httpd worker tasks
    // (web_httpd, for /camera or /audio or /command; data_httpd via
    // disconnectStreamClient()/sendBreakBestEffort()). Held for the whole
    // function (tag bookkeeping, the raw I2C write, and the reply-wait poll)
    // so two callers can never interleave bytes on the shared Wire bus or
    // clobber each other's s_awaited_tag. (Originally three worker tasks
    // when /stream/result and /stream/audio were still two separate httpd
    // instances - since merged into one, see data_httpd's own comment.)
    MutexGuard cmd_lock(s_cmd_mutex);

    TickType_t ticks        = xTaskGetTickCount();
    // masked to 16 bits (4 hex digits) - see CMD_TAG_FMT_STR's comment for
    // why: %.4X is a minimum width, not a truncation, so the raw tick count
    // must be masked down first or it'd print all its digits and defeat the
    // whole point of shrinking this.
    size_t cmd_tag_size = snprintf(cmd_tag_buf, cmd_tag_buf_size, CMD_TAG_FMT_STR, (unsigned)(ticks & 0xFFFFu));

    size_t last_id = PB.id;

    xSemaphoreTake(PB.mutex, portMAX_DELAY);
    strncpy(s_awaited_tag, cmd_tag_buf, sizeof(s_awaited_tag) - 1);
    s_awaited_tag[sizeof(s_awaited_tag) - 1] = '\0';
    xSemaphoreGive(PB.mutex);

    // 2026-07-17: was 4x AI.write() over atSerial (UART) - now sent over I2C
    // instead (WE2's i2c_cmd.c feeds the same bytes into the same AT-command
    // dispatcher). Replies still arrive over UART unchanged - see
    // initI2CCommandChannel()'s comment. Same byte sequence as before, just
    // over Wire instead of AI's configured transport.
    //
    // TEMP diagnostic (system-wide freeze under investigation): bracket the
    // I2C call with explicit before/after prints and an explicit short
    // timeout - Wire's documented default timeout is 50ms, but the freeze
    // observed this session lasted 10-25+ seconds with zero further output
    // anywhere (not even USB-CDC console, though ping/WiFi kept responding),
    // so either the 50ms default isn't actually being honored by the
    // underlying i2c_master_transmit() call, or the hang is somewhere else
    // entirely - this will show definitively which.
    //
    // 2026-07-21: this block now takes s_i2c_bus_mutex of its own accord
    // (scoped, released the instant the transmission finishes) instead of
    // relying on s_cmd_mutex (still held for the whole outer function) to
    // arbitrate the bus - see s_i2c_bus_mutex's own comment. Two concurrent
    // sendTaggedCommand() callers still can't reach this point at the same
    // time regardless (s_cmd_mutex already serializes the whole function),
    // so the only thing this actually adds exclusion against is a servo I2C
    // write landing mid-transmission.
    uint8_t i2c_err;
    {
        MutexGuard bus_lock(s_i2c_bus_mutex);
        Serial.printf("sendTaggedCommand: before Wire I2C send, tag=%s\r\n", cmd_tag_buf);
        Wire.setTimeOut(200);
        Wire.beginTransmission(WE2_I2C_CMD_ADDR);
        Wire.write((const uint8_t*)CMD_PREFIX, strlen(CMD_PREFIX));
        Wire.write((const uint8_t*)cmd_tag_buf, cmd_tag_size);
        Wire.write((const uint8_t*)body, body_len);
        Wire.write((const uint8_t*)CMD_SUFFIX, strlen(CMD_SUFFIX));
        i2c_err = Wire.endTransmission();
        Serial.printf("sendTaggedCommand: after Wire I2C send, tag=%s, err=%u\r\n", cmd_tag_buf, (unsigned)i2c_err);
    }
    if (i2c_err != 0) {
        Serial.printf("sendTaggedCommand: I2C write failed, err=%u\r\n", (unsigned)i2c_err);
    }

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
        auto it = std::find_if(slots.begin(), slots.end(), [&](const SlotRef& p) {
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
        uint32_t silence_ms   = millis() - s_last_uart1_rx_ms;
        bool     really_silent = silence_ms >= WE2_RESYNC_SILENCE_MS;
        // 2026-07-17: hardware-confirmed gap in the silence-gated check just
        // below - if UART1 traffic (audio/bbox) keeps trickling in but the
        // SPECIFIC awaited reply never does (observed: WE2-side task
        // starvation under sustained audio load can apparently starve its
        // own command-reply path this badly), silence_ms never crosses
        // WE2_RESYNC_SILENCE_MS and this resync path never fires *at all* -
        // s_consecutive_at_timeouts climbed past 15 in one hardware test
        // with zero recovery until the ESP32 itself was hard-reset. The
        // silence check is right to distrust a LOW count (2 timeouts is
        // nowhere near enough evidence under normal congestion), but must
        // not be trusted to protect against an unbounded stuck state either
        // - this hard ceiling is the same "just get it moving again" escape
        // hatch the pre-2026-07-17 logic always had, now only reached after
        // real, sustained failure instead of on every transient hiccup.
        bool many_timeouts = s_consecutive_at_timeouts >= WE2_RESYNC_HARD_CEILING;
        if (really_silent || many_timeouts) {
            log_w("sendTaggedCommand: %u consecutive AT reply timeouts (silent for "
                  "%ums, %s) - re-arming probe handshake...",
                  (unsigned)s_consecutive_at_timeouts, (unsigned)silence_ms,
                  really_silent ? "UART1 genuinely silent" : "hard ceiling reached despite traffic");
            s_we2_uart1_synced        = false;
            s_consecutive_at_timeouts = 0;
            // Whatever's sitting in fetchFramedMessages()'s buffer is now stale
            // (mid-parse of a reply that's never coming) - drop it so a dangling
            // partial pattern can't delay recognizing real traffic once WE2
            // actually reconnects and pollWe2Uart1Handshake() hands the line back.
            s_fetch_len = 0;
        } else {
            // 2026-07-17: real UART1 traffic within WE2_RESYNC_SILENCE_MS means
            // WE2 is still alive and still on UART1 - this specific reply is
            // just queued behind other traffic (bbox/audio), not evidence of a
            // reboot. Don't disrupt the handshake state over it - just let the
            // caller's own timeout (it'll report failure to its HTTP client)
            // handle this one reply going missing, and keep counting towards
            // the threshold (both this one and the hard ceiling above) in case
            // UART1 genuinely does go silent, or the stuck state persists long
            // enough to justify forcing a resync anyway.
            log_w("sendTaggedCommand: %u consecutive AT reply timeouts but UART1 "
                  "traffic seen %ums ago (< %ums) - WE2 still alive, NOT "
                  "re-arming probe handshake yet (likely congestion, not a reboot)",
                  (unsigned)s_consecutive_at_timeouts, (unsigned)silence_ms,
                  (unsigned)WE2_RESYNC_SILENCE_MS);
        }
    }
    return SlotRef();  // SlotRef's pointer ctor is `explicit`, so a bare
                        // `return nullptr;` (fine for shared_ptr) won't
                        // implicitly convert here - construct the empty/
                        // null ref directly instead.
}

/* 2026-07-11: called from disconnectStreamClient() the moment
 * stream_data_handler()'s httpd_resp_send_chunk() first fails - that only
 * happens once the client's TCP connection is actually gone, so this is the
 * "client disconnected unexpectedly, self-close the WE2 stream" behavior
 * (per explicit direction: no browser UI to click a stop button anymore, so
 * nothing else would ever tell WE2 to stop if a client just vanishes).
 * Best-effort and short timeout - the caller is already tearing down its own
 * connection either way, this shouldn't hold that up waiting for a reply
 * that doesn't matter to it. */
// 2026-07-11: defensive debounce - hardware testing showed something (root
// cause not yet confirmed) could drive this into being called back-to-back
// at ~10-15ms intervals (from the era when /stream/result and /stream/audio
// were still two separate connections/tasks racing each other - see
// data_httpd's own comment on the 2026-07-18 merge), and each
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

    // 2026-07-18: this debounce's check-then-set on s_last_sent/s_sent_once
    // was itself a TOCTOU race once result_httpd's and audio_httpd's worker
    // tasks could both call this at nearly the same instant (both streams
    // timing out together under combined load) - two callers could each see
    // "not within cooldown" before either updated the statics, both fall
    // through, and both call sendTaggedCommand() back to back. That's now
    // safe re: I2C corruption (sendTaggedCommand() serializes itself via
    // s_cmd_mutex - see its own comment), but still means BREAK could be
    // sent twice in a row for no reason, and the second call blocks its
    // caller's task for a full timeout waiting on a reply that was never
    // going to arrive differently. Guard the check+update as one atomic
    // step. Deliberately released BEFORE calling sendTaggedCommand() (not
    // held across it) - s_cmd_mutex isn't a recursive mutex, and
    // sendTaggedCommand() takes that lock itself.
    xSemaphoreTake(s_cmd_mutex, portMAX_DELAY);
    TickType_t now = xTaskGetTickCount();
    if (s_sent_once && (now - s_last_sent) < cooldown_tick) {
        xSemaphoreGive(s_cmd_mutex);
        log_w("sendBreakBestEffort: suppressed (last sent %lums ago)",
              (unsigned long)((now - s_last_sent) * portTICK_PERIOD_MS));
        return;
    }
    s_last_sent = now;
    s_sent_once = true;
    xSemaphoreGive(s_cmd_mutex);

    char cmd_tag_buf[32] = {0};
    sendTaggedCommand(body, strlen(body), cmd_tag_buf, sizeof(cmd_tag_buf), 1000);
}

static esp_err_t command_handler(httpd_req_t* req) {
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
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
    SlotRef                           slot = sendTaggedCommand(cmd_buf, cmd_size, cmd_tag_buf, sizeof(cmd_tag_buf));
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
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
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

    // 2026-07-19: hardware-verified against real WE2 that AT+SENSOR's reply
    // arrives before WE2 is actually done applying it - app_request_cam_
    // resolution() (WE2-side sscma_cam_mic.c) is asynchronous, cam_task only
    // applies the new resolution "within the next couple of frames" (see its
    // own comment there), which means turning the sensor off, reconfiguring
    // MIPI/CIS registers, and turning it back on - a real multi-step
    // sequence, not instant. Sending AT+INVOKE immediately after SENSOR's
    // reply (as this did before) can land while WE2's AT command parser task
    // is still busy servicing that reinit, and the INVOKE reply gets lost -
    // confirmed via simultaneous WE2+ESP32 console capture. A short settle
    // delay here is cheaper and simpler than adding a new WE2-side "sensor
    // ready" event/notification.
    delay(300);

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
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
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

/* TEMP DIAGNOSTIC (2026-07-17): lets a test script poll ESP32-side heap over
 * HTTP/curl instead of the USB-CDC debug console - the console has proven
 * unreliable for long/multi-cycle tests this session (see
 * esp32_camera_web_server_bridge memory: it can go silent for reasons
 * unrelated to whether the board itself is healthy), while `curl` against
 * this same board has stayed reliable across every test so far. No AT
 * command involved - purely local ESP32 heap state, doesn't touch WE2 at
 * all, so this alone should never contend with anything on the I2C/UART
 * side. Remove once the heap investigation wraps up. */
static esp_err_t heap_handler(httpd_req_t* req) {
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
    char buf[160];
    // 2026-07-20: added rssi/phy mode - see this session's investigation
    // into resp.read() itself blocking ~1s on the client with a clean (zero
    // retry/discard) WiFi link on the CLIENT side (checked via `iw`/
    // /proc/net/wireless), which shifted suspicion to the ESP32's OWN radio
    // link quality as the still-unchecked side of that path. Arduino's WiFi
    // library has no direct "negotiated bitrate" accessor (unlike Linux's
    // `iw` on the client side) - esp_wifi_sta_get_ap_info() is the closest
    // available: it exposes which PHY mode(s) the connection is actually
    // using (11b/11g/11n), not a Mbps number, but a connection stuck on
    // 11b-only (proto_11b, legacy ~1-11Mbps) vs. genuinely using 11n would
    // be a meaningful, visible difference here.
    wifi_ap_record_t ap_info = {};
    esp_wifi_sta_get_ap_info(&ap_info);
    int rssi = WiFi.RSSI();
    int  len = snprintf(buf, sizeof(buf),
                         "{\"free\": %u, \"largest\": %u, \"rssi\": %d, "
                         "\"phy_11b\": %d, \"phy_11g\": %d, \"phy_11n\": %d, \"channel\": %d}",
                         (unsigned)heap_caps_get_free_size(MALLOC_CAP_8BIT),
                         (unsigned)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT),
                         rssi, (int)ap_info.phy_11b, (int)ap_info.phy_11g,
                         (int)ap_info.phy_11n, (int)ap_info.primary);
    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    return httpd_resp_send(req, buf, len);
}

/* AT+ASR=<rate> then AT+ASAMPLE=-1 - starts the DMIC/PDM audio stream at the
 * given rate (16000 or 32000 only - the WE2 firmware rejects anything else).
 * Query params: rate=16000|32000, default 16000. This app doesn't consume
 * the audio stream itself (no browser playback - control only, per
 * explicit direction), it just starts/stops/configures it on the WE2. */
static esp_err_t audio_start_handler(httpd_req_t* req) {
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
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
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
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

/* 2026-07-19: PCA9685/SG92R servo control (see initServoMotor()'s comment
 * above for the driver itself). Client never sends an angle or step count -
 * it only ever calls /motor/left or /motor/right some number of times over
 * WiFi, and each call is exactly one SERVO_STEP_DEG (2 degree) step,
 * clamped to [0,180] by stepServoAngle(). Direction convention (left =
 * decreasing angle, right = increasing) is this handler's own choice, not a
 * hardware constant - swap the sign here if it turns out backwards for a
 * given physical mount. */
static esp_err_t motor_left_handler(httpd_req_t* req) {
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
    int  angle = stepServoAngle(-SERVO_STEP_DEG, /*persist=*/true);
    char buf[64];
    int  len = snprintf(buf, sizeof(buf), "{\"angle\": %d, \"connected\": %s}", angle,
                         s_servo_available ? "true" : "false");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    return httpd_resp_send(req, buf, len);
}

static esp_err_t motor_right_handler(httpd_req_t* req) {
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
    int  angle = stepServoAngle(SERVO_STEP_DEG, /*persist=*/true);
    char buf[64];
    int  len = snprintf(buf, sizeof(buf), "{\"angle\": %d, \"connected\": %s}", angle,
                         s_servo_available ? "true" : "false");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    return httpd_resp_send(req, buf, len);
}

/* 2026-07-20: sets a target angle and returns immediately - servoRampPoll()
 * (called from loop()) moves the servo toward it one SERVO_RAMP_STEP_DEG
 * step at a time, same pacing as the boot-time ramp. Exists so a caller that
 * wants to sweep a wide range (e.g. a stability test) can do it with ONE
 * request instead of dozens of /motor/left or /motor/right calls - see
 * s_servo_target_angle's own comment for the history there. Just stores an
 * atomic int - no lock needed here, unlike stepServoAngle()'s read-modify-
 * write of the CURRENT angle. */
static esp_err_t motor_set_handler(httpd_req_t* req) {
    if (!checkAuth(req)) {
        return ESP_FAIL;
    }
    char angle_str[16];
    query_param(req, "angle", angle_str, sizeof(angle_str), "");
    if (angle_str[0] == '\0') {
        httpd_resp_send_404(req);
        return ESP_FAIL;
    }
    int target = atoi(angle_str);
    if (target < SERVO_MIN_ANGLE) {
        target = SERVO_MIN_ANGLE;
    } else if (target > SERVO_MAX_ANGLE) {
        target = SERVO_MAX_ANGLE;
    }
    s_servo_target_angle.store(target);

    char buf[96];
    int  len = snprintf(buf, sizeof(buf), "{\"target\": %d, \"angle\": %d, \"connected\": %s}",
                         target, s_servo_angle.load(), s_servo_available ? "true" : "false");
    httpd_resp_set_type(req, "application/json");
    httpd_resp_set_hdr(req, "Access-Control-Allow-Origin", "*");
    return httpd_resp_send(req, buf, len);
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
    // 2026-07-17: see the streaming instances' identical comment below - a
    // stalled/vanished client on this control-plane server (camera/audio
    // start-stop, /command) can leave a worker occupied for HTTPD_DEFAULT_
    // CONFIG()'s full 5s default before the socket layer itself gives up,
    // on top of whatever client_gone()-based app-level detection exists.
    // Every command here is a short request/reply, never a legitimate
    // multi-second transfer, so 3s is still generous.
    config.recv_wait_timeout = 3;
    config.send_wait_timeout = 3;

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

    httpd_uri_t heap_uri = {.uri      = "/heap",
                            .method   = HTTP_GET,
                            .handler  = heap_handler,
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

    httpd_uri_t motor_left_uri = {.uri      = "/motor/left",
                                  .method   = HTTP_GET,
                                  .handler  = motor_left_handler,
                                  .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                                  ,
                                  .is_websocket             = true,
                                  .handle_ws_control_frames = false,
                                  .supported_subprotocol    = NULL
#endif
    };

    httpd_uri_t motor_right_uri = {.uri      = "/motor/right",
                                   .method   = HTTP_GET,
                                   .handler  = motor_right_handler,
                                   .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                                   ,
                                   .is_websocket             = true,
                                   .handle_ws_control_frames = false,
                                   .supported_subprotocol    = NULL
#endif
    };

    httpd_uri_t motor_set_uri = {.uri      = "/motor/set",
                                 .method   = HTTP_GET,
                                 .handler  = motor_set_handler,
                                 .user_ctx = NULL
#ifdef CONFIG_HTTPD_WS_SUPPORT
                                 ,
                                 .is_websocket             = true,
                                 .handle_ws_control_frames = false,
                                 .supported_subprotocol    = NULL
#endif
    };

    httpd_uri_t stream_data_uri = {.uri      = "/stream/data",
                                   .method   = HTTP_GET,
                                   .handler  = stream_data_handler,
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
        ret |= httpd_register_uri_handler(web_httpd, &heap_uri);
        ret |= httpd_register_uri_handler(web_httpd, &audio_start_uri);
        ret |= httpd_register_uri_handler(web_httpd, &audio_stop_uri);
        ret |= httpd_register_uri_handler(web_httpd, &motor_left_uri);
        ret |= httpd_register_uri_handler(web_httpd, &motor_right_uri);
        ret |= httpd_register_uri_handler(web_httpd, &motor_set_uri);
    }

    if (ret != ESP_OK) {
        log_e("Failed to start web server, code '0x%x' ...", ret);
        while (true) {
            vTaskDelay(1000 / portTICK_PERIOD_MS);
        }
    }

    // 2026-07-18: ONE instance (`data_httpd`) instead of the former two
    // (result_httpd/audio_httpd) - see data_httpd's own comment for why
    // (simultaneous dual-accept heap spike root-caused this session).
    // Needs its own smaller stack_size, not the control-plane web_httpd's
    // 10240 (that one parses AT command JSON replies; this is a relay loop
    // with modest locals).
    //
    // 2026-07-17: was 2 (lowered from HTTPD_DEFAULT_CONFIG()'s default of 7
    // on the reasoning "only ever needs to serve one client at a time by
    // design"). Raised to 4: that reasoning breaks down exactly when
    // client_gone()'s dead-peer detection is slow to notice a client that
    // vanished without a clean FIN (confirmed on hardware this session - a
    // client that timed out client-side, e.g. a test script's own read
    // timeout without a clean close(), left a zombie occupying a slot for
    // an extended period) - with only 2 slots, a single zombie halves
    // capacity and a second one exhausts it entirely, permanently locking
    // out every subsequent real client until a full ESP32 reset. More
    // headroom directly mitigates this regardless of whether the deeper
    // dead-peer-detection timing issue itself ever gets root-caused.
    config.max_uri_handlers  = 1;
    config.max_open_sockets  = 4;
    // Force the underlying socket layer to give up on a stalled peer
    // faster than HTTPD_DEFAULT_CONFIG()'s 5s default - this is a
    // low-latency streaming endpoint (audio chunks every ~125ms when
    // active), no legitimate send/recv should ever need close to 5s.
    config.recv_wait_timeout = 3;
    config.send_wait_timeout = 3;

    config.server_port = 8081;
    config.ctrl_port   = 8081;
    // 2026-07-12: bumped 4096 -> 6144. stream_data_handler() has a real
    // stack-local, `BboxEntry bbox_entries[BBOX_RING_SLOTS]` - 20 x
    // sizeof(BboxEntry) (48B, alignment-padded - see BboxEntry's own
    // comment) = 960B - plus a small `std::vector` for AUDIO's pending
    // slots (stack-resident header only, ~24-32B; the actual slot data is
    // heap/pool-backed, not on this stack) - comfortable margin either way.
    config.stack_size  = 6144;
    log_i("Starting data stream server on port: '%d'", config.server_port);
    if ((ret = httpd_start(&data_httpd, &config)) == ESP_OK) {
        ret |= httpd_register_uri_handler(data_httpd, &stream_data_uri);
    }
    if (ret != ESP_OK) {
        log_e("Failed to start data stream server, code '0x%x' ...", ret);
        while (true) {
            vTaskDelay(1000 / portTICK_PERIOD_MS);
        }
    }
}
