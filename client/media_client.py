#!/usr/bin/env python3
"""Orange Pi side client for the headless camera_web_server ESP32C3 bridge.

The ESP32 sketch is a pure API/streaming backend (no browser UI, no image
relay - see app_httpd.cpp's 2026-07-12 comments, /stream/frame was dropped
entirely): this script is what actually drives it over WiFi from this
machine - start camera (AI inference / bbox detection) and/or audio, save
whatever comes back to local files, and stop cleanly.

Endpoints used (all against --host, default port 80 unless noted):
    GET /camera/start?resolution=0|1|2&mode=invoke&differed=0
                                    - resolution: 0=240x240, 1=480x480,
                                      2=640x480 (confirmed against this
                                      board/model - re-verify per app if the
                                      firmware ever changes model/sensor)
    GET /camera/stop
    GET /audio/start?rate=16000|32000
    GET /audio/stop
    GET :8081/stream/data          - MERGED stream (2026-07-18, replaces the
                                      former separate :8081/stream/result +
                                      :8082/stream/audio): AI inference
                                      (bbox) JSON lines AND raw framed PCM
                                      audio interleaved over one connection.
                                      This client demuxes them itself - see
                                      read_data_stream(). Root cause for the
                                      merge: opening two brand-new streaming
                                      connections at the same instant spiked
                                      the ESP32's heap sharply enough to
                                      collapse its WiFi/httpd stack under
                                      repeated combined camera+audio cycling
                                      (see esp32_camera_web_server_bridge
                                      memory, 2026-07-18 section) - one
                                      connection instead of two removes the
                                      trigger entirely rather than just
                                      timing around it.
    GET /result                    - one-shot: just the single newest bbox
                                      batch (not used by this client's main
                                      capture loop, but exposed via `once`
                                      for a quick manual check)

Usage:
    media_client.py start --host 192.168.1.112 --outdir ./capture \\
        [--camera] [--audio] [--resolution 0|1|2] [--rate 16000|32000] \\
        [--user <user>] [--password <password>]
    media_client.py stop [--outdir ./capture]
    media_client.py status [--outdir ./capture]
    media_client.py once --host 192.168.1.112   # one-shot /result check, no capture

start with neither --camera nor --audio enables both.

Every endpoint on the ESP32 requires HTTP Basic Auth (see app_httpd.cpp's
checkAuth()/initHttpAuth(), 2026-07-13) - --user/--password must match the
firmware's HTTP_AUTH_USER/HTTP_AUTH_PASS #defines, no default credentials
are baked into this script. Note: the board's IP can change across reboots
(DHCP) - if this client can't connect at all, verify the current IP before
assuming an auth or firmware problem.

Output layout (under --outdir):
    results.jsonl                 - one JSON line per AI inference (bbox)
                                     batch, demuxed from /stream/data
    audio.wav                     - single continuous file, all audio
                                     frames demuxed from /stream/data,
                                     concatenated in arrival order
    media_client.pid              - present while a capture is running
    media_client.log              - background process's own log

This client does not retry or attempt to recover dropped frames/bytes on a
mid-stream network hiccup - it reconnects and keeps going, logging what it
saw. It also does not retry a stalled /stream/data connection instantly:
the ESP32 itself intentionally closes that connection after ~10s with zero
new bbox/audio activity (a deliberate idle-timeout backstop on its side) -
this client treats that the same as any other drop and just reconnects.
"""
import argparse
import base64
import json
import os
import signal
import socket
import struct
import sys
import threading
import time
import urllib.request
import urllib.error
import wave

AUDIO_MAGIC = b"\xffSMB"
AUDIO_HEADER_LEN = 16
AUDIO_CRC_LEN = 2

# HTTP Basic Auth credentials - every endpoint on the ESP32 requires these
# (see app_httpd.cpp's checkAuth()). No default is baked in here on purpose
# (avoid committing a real credential to this file) - set from --user/
# --password (required) in cmd_start()/cmd_once() before any request is
# made; module-level so http_get()/the stream readers don't all need an
# extra parameter threaded through them.
AUTH_USER = None
AUTH_PASS = None


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def auth_header():
    cred = f"{AUTH_USER}:{AUTH_PASS}".encode()
    return "Basic " + base64.b64encode(cred).decode()


def http_get(url, timeout=5):
    req = urllib.request.Request(url, headers={"Authorization": auth_header()})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def pid_file(outdir):
    return os.path.join(outdir, "media_client.pid")


def log_file(outdir):
    return os.path.join(outdir, "media_client.log")


# 2026-07-19: table-driven (was a pure-Python bit-loop, ~8 inner iterations
# per byte) - at up to 8000 bytes/audio frame that loop measured ~60ms on
# this Orange Pi's CPU (benchmarked directly), easily enough to make THIS
# SCRIPT the "slow client" from the ESP32's perspective under any load -
# not real WiFi conditions. Same bug independently found and fixed in the
# session scratchpad's coverage_test.py/matrix11.py; a stale memory note
# claimed this file was already fixed for it - it wasn't, the actual code
# still had the bit-loop. Table-driven is ~6x faster, mathematically
# identical output.
_CRC16_TABLE = []
for _i in range(256):
    _crc = _i
    for _ in range(8):
        _crc = (_crc >> 1) ^ 0xA001 if (_crc & 1) else (_crc >> 1)
    _CRC16_TABLE.append(_crc)


def crc16_maxim(data):
    crc = 0x0000
    for b in data:
        crc = (crc >> 8) ^ _CRC16_TABLE[(crc ^ b) & 0xFF]
    return crc ^ 0xFFFF


def _graceful_close_response(resp):
    """Drain any buffered-but-unread bytes and shutdown(SHUT_RDWR) before the
    caller's `with` block runs resp's own close() - see read_data_stream()'s
    own call site comment for why this matters. Reaches into urllib's
    internals (resp.fp.raw._sock) to get the underlying socket for the
    shutdown() call - urllib itself exposes no public API for this, so this
    is best-effort: if a future Python version changes that internal shape,
    this just falls back to draining (still real, still worth doing) without
    the explicit shutdown() call."""
    try:
        sock = resp.fp.raw._sock
    except AttributeError:
        sock = None
    if sock is not None:
        try:
            sock.settimeout(0.3)
        except OSError:
            pass
    # Overall deadline, not a per-read one - the server never stops sending
    # on its own just because we're about to leave, so a per-read timeout
    # alone would never fire as long as new data keeps arriving within each
    # window (same bug/fix already found the hard way in this session's
    # scratchpad coverage_test.py).
    drain_deadline = time.time() + 1.0
    while time.time() < drain_deadline:
        try:
            chunk = resp.read(65536)
        except Exception:
            break
        if not chunk:
            break
    if sock is not None:
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Stream reader (single thread inside the background process)
# ---------------------------------------------------------------------------

def read_data_stream(base, results_path, wav_path, stop_event):
    """Pulls the MERGED stream off :8081/stream/data (see
    stream_data_handler() in app_httpd.cpp, 2026-07-18 - replaces what used
    to be two separate connections/threads, one per :8081/stream/result and
    :8082/stream/audio). Demuxes each message off the front of the buffer
    exactly the way the ESP32's own fetchFramedMessages() demuxes WE2's
    UART1 bytes: if it starts with the audio magic (0xFF 'S' 'M' 'B'), it's
    a framed PCM audio frame (header + payload + CRC16); otherwise it's a
    '\\n'-terminated bbox JSON line. These can never collide - buildInvokeJson()
    on the firmware side always starts a JSON line with '{' (0x7B), never
    0xFF. CRC mismatches on audio are logged but not fatal, just dropped.
    Reconnects on any read error, or when the server itself closes the
    connection after ~10s of no new bbox/audio activity (its own deliberate
    idle-timeout backstop, not an error)."""
    url = base.replace(":80", ":8081") if ":80" in base else base + ":8081"
    url += "/stream/data"

    wav_writer = None
    wav_rate = None
    frame_count = 0
    crc_bad = 0
    bbox_count = 0

    with open(results_path, "a") as out:
        while not stop_event.is_set():
            try:
                req = urllib.request.Request(url, headers={"Authorization": auth_header()})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    buf = b""
                    while not stop_event.is_set():
                        chunk = resp.read(4096)
                        if not chunk:
                            break
                        buf += chunk
                        while True:
                            if len(buf) < 4:
                                break  # not enough to tell audio-magic from a JSON line's '{' yet
                            if buf[:4] == AUDIO_MAGIC:
                                if len(buf) < AUDIO_HEADER_LEN:
                                    break
                                rate, plen = struct.unpack_from("<II", buf, 4)
                                channels, bits = buf[12], buf[13]
                                total = AUDIO_HEADER_LEN + plen + AUDIO_CRC_LEN
                                if len(buf) < total:
                                    break
                                payload = buf[AUDIO_HEADER_LEN:AUDIO_HEADER_LEN + plen]
                                crc_recv = struct.unpack_from("<H", buf, AUDIO_HEADER_LEN + plen)[0]
                                if crc16_maxim(payload) != crc_recv:
                                    crc_bad += 1
                                else:
                                    if wav_writer is None:
                                        wav_writer = wave.open(wav_path, "wb")
                                        wav_writer.setnchannels(channels or 1)
                                        wav_writer.setsampwidth((bits or 16) // 8)
                                        wav_writer.setframerate(rate)
                                        wav_rate = rate
                                    if rate != wav_rate:
                                        log(f"data reader: sample rate changed {wav_rate}->{rate} mid-stream, "
                                            f"keeping {wav_rate} in the WAV header (rest of the file will play "
                                            f"back at the wrong speed) - stop/restart the capture after AT+ASR "
                                            f"changes")
                                    wav_writer.writeframes(payload)
                                    frame_count += 1
                                buf = buf[total:]
                            else:
                                idx = buf.find(b"\n")
                                if idx < 0:
                                    if len(buf) > 65536:
                                        # not audio-magic and no line terminator found in 64KB -
                                        # something's desynced (shouldn't happen with well-formed
                                        # firmware output); drop the buffered garbage rather than
                                        # growing it unboundedly.
                                        log(f"data reader: {len(buf)} bytes buffered with no audio "
                                            f"magic or line terminator - dropping and resyncing")
                                        buf = b""
                                    break
                                line = buf[:idx].decode("utf-8", "replace").strip()
                                buf = buf[idx + 1:]
                                if line:
                                    try:
                                        entry = json.loads(line)
                                        entry["_client_ts_ms"] = int(time.time() * 1000)
                                        out.write(json.dumps(entry) + "\n")
                                        out.flush()
                                        bbox_count += 1
                                    except json.JSONDecodeError as e:
                                        log(f"data reader: malformed bbox line dropped ({e}): {line[:120]!r}")
                    # 2026-07-19: drain before the `with` block's own close()
                    # runs - see _graceful_close_response()'s own comment.
                    # Reached whenever the inner loop above exits for any
                    # reason (stop_event set, or the server itself closed
                    # the connection) - in the stop_event case specifically,
                    # the server is almost certainly still actively
                    # streaming (it has no idea we're about to leave), so
                    # there's very likely unread data sitting in the OS
                    # receive buffer right now; closing on top of that sends
                    # an abortive RST instead of a clean FIN.
                    _graceful_close_response(resp)
            except Exception as e:
                if not stop_event.is_set():
                    log(f"stream/data reader: {e} - reconnecting in 2s")
                    time.sleep(2)

    if wav_writer is not None:
        wav_writer.close()
    log(f"data reader stopped, {bbox_count} detection batches + {frame_count} audio frames saved, "
        f"{crc_bad} CRC mismatches dropped")


# ---------------------------------------------------------------------------
# Background capture process
# ---------------------------------------------------------------------------

def run_capture(host, outdir, want_camera, want_audio, resolution, rate):
    base = f"http://{host}"
    results_path = os.path.join(outdir, "results.jsonl")
    wav_path = os.path.join(outdir, "audio.wav")

    stop_event = threading.Event()

    def handle_sigterm(signum, frame):
        log("received stop signal, shutting down...")
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_sigterm)
    signal.signal(signal.SIGINT, handle_sigterm)

    def start_with_retries(url, attempts=5, delay=2, recover_url=None):
        """The ESP32C3's single-threaded httpd can be briefly unresponsive
        right after a fresh flash/reset (UART1 handshake with WE2 still
        settling - see esp32_camera_web_server_bridge memory). A single
        timeout here used to crash the whole background process (unhandled
        exception -> process exits, leaving nothing running) - retry a few
        times before actually giving up.

        2026-07-19: hardware-verified against real WE2 that blindly retrying
        a *start* command (AT+INVOKE/AT+ASR) is actively dangerous if WE2 is
        already mid-invoke from an earlier attempt whose reply never made it
        back (e.g. the previous attempt's own timeout) - sending a second
        AT+INVOKE while WE2 hasn't acknowledged the first one hangs the
        ESP32's entire HTTP stack solid (every endpoint, not just this one)
        and needs a hard power-cycle of BOTH boards to recover (WE2 keeps
        streaming through an ESP32-only reset, then floods/wedges the fresh
        boot's UART1 handshake). If recover_url is given, it's hit
        (best-effort, errors ignored - if the board's already this wedged,
        the real attempt below will surface it) before every RETRY
        (attempt 2+), to force a clean stopped state before re-sending a
        start command that might otherwise collide with an earlier
        attempt's still-in-flight one.

        2026-07-19: NOT before attempt 1 anymore (was) - hardware-verified
        that stop-then-start (AT+BREAK immediately followed by AT+SENSOR/
        AT+INVOKE, which power-cycles the camera sensor and reconfigures
        MIPI) is real stress on the sensor/MIPI hardware, and this
        project's own test scripts calling it on every single capture
        start - even when nothing was actually running yet - is suspected
        of contributing to WE2 spontaneously rebooting mid-session over the
        course of a long test session. Skipping the pointless stop-when-
        nothing-is-running case on the common-path first attempt cuts a
        large fraction of that cycling without losing the protection this
        exists for (which only matters from the second attempt onward, when
        a previous attempt might have left WE2 in a real half-started
        state)."""
        last_err = None
        for attempt in range(1, attempts + 1):
            if recover_url is not None and attempt > 1:
                try:
                    http_get(recover_url, timeout=5)
                except Exception:
                    pass
                time.sleep(0.5)  # let WE2/ESP32 settle after BREAK before re-invoking
            try:
                return http_get(url, timeout=8)
            except Exception as e:
                last_err = e
                log(f"start request to {url} failed (attempt {attempt}/{attempts}): {e}")
                if attempt < attempts:
                    time.sleep(delay)
        raise RuntimeError(f"giving up on {url} after {attempts} attempts: {last_err}")

    # Control-plane starts stay sequential (camera then audio) - the ESP32
    # side serializes AT-command sends across a single mutex regardless
    # (see app_httpd.cpp's s_cmd_mutex), so firing these concurrently from
    # two client threads would only queue up there, not actually save time.
    # recover_url=/camera/stop on both - AT+BREAK stops whichever of
    # camera/audio happens to be running (see its own comment further down),
    # so it's the right recovery action before either start command.
    if want_camera:
        log(f"starting camera / AI inference (resolution={resolution})")
        start_with_retries(base + f"/camera/start?resolution={resolution}&mode=invoke&differed=0",
                            recover_url=base + "/camera/stop")
    if want_audio:
        log(f"starting audio (rate={rate})")
        start_with_retries(base + f"/audio/start?rate={rate}", recover_url=base + "/camera/stop")

    # 2026-07-18: ONE reader thread/connection now regardless of want_camera/
    # want_audio - see read_data_stream()'s own comment. It simply won't see
    # bbox lines if camera was never started, or audio frames if audio
    # wasn't, so this is correct for any combination of the two.
    threads = [threading.Thread(target=read_data_stream, args=(base, results_path, wav_path, stop_event))]

    for t in threads:
        t.start()

    stop_event.wait()

    # AT+BREAK stops both camera and audio together (no separate per-stream
    # stop in WE2's AT protocol - see camera_stop_handler()'s comment in
    # app_httpd.cpp), so one call covers whichever we started.
    try:
        http_get(base + "/camera/stop", timeout=5)
    except Exception as e:
        log(f"stop request failed (WE2 stream may still be running): {e}")

    for t in threads:
        t.join(timeout=10)

    log("capture stopped")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def cmd_start(args):
    global AUTH_USER, AUTH_PASS
    AUTH_USER, AUTH_PASS = args.user, args.password

    outdir = os.path.abspath(args.outdir)
    os.makedirs(outdir, exist_ok=True)
    pf = pid_file(outdir)
    if os.path.exists(pf):
        with open(pf) as f:
            old_pid = int(f.read().strip())
        try:
            os.kill(old_pid, 0)
            print(f"already running (pid {old_pid}) - stop it first")
            return 1
        except ProcessLookupError:
            os.remove(pf)  # stale pidfile

    want_camera = args.camera or not (args.camera or args.audio)
    want_audio = args.audio or not (args.camera or args.audio)

    lf = log_file(outdir)
    pid = os.fork()
    if pid > 0:
        # parent: wait briefly for the child to write its pidfile, then exit
        for _ in range(20):
            if os.path.exists(pf):
                break
            time.sleep(0.1)
        print(f"capture started in background (outdir={outdir}, log={lf})")
        return 0

    # child: fully detach and become the capture process
    os.setsid()
    sys.stdout.flush()
    sys.stderr.flush()
    devnull = os.open(os.devnull, os.O_RDWR)
    os.dup2(devnull, 0)
    log_fd = os.open(lf, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
    os.dup2(log_fd, 1)
    os.dup2(log_fd, 2)

    with open(pf, "w") as f:
        f.write(str(os.getpid()))

    try:
        run_capture(args.host, outdir, want_camera, want_audio, args.resolution, args.rate)
    finally:
        if os.path.exists(pf):
            os.remove(pf)
    os._exit(0)


def cmd_stop(args):
    outdir = os.path.abspath(args.outdir)
    pf = pid_file(outdir)
    if not os.path.exists(pf):
        print("not running (no pidfile)")
        return 1
    with open(pf) as f:
        pid = int(f.read().strip())
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        print("process already gone, cleaning up stale pidfile")
        os.remove(pf)
        return 0
    print(f"sent stop signal to pid {pid}, waiting for graceful shutdown...")
    for _ in range(50):
        if not os.path.exists(pf):
            print("stopped")
            return 0
        time.sleep(0.2)
    print("still running after 10s - check the log")
    return 1


def cmd_status(args):
    outdir = os.path.abspath(args.outdir)
    pf = pid_file(outdir)
    if not os.path.exists(pf):
        print("not running")
        return 1
    with open(pf) as f:
        pid = int(f.read().strip())
    try:
        os.kill(pid, 0)
        print(f"running (pid {pid})")
        return 0
    except ProcessLookupError:
        print("stale pidfile (process is gone)")
        return 1


def cmd_once(args):
    """One-shot sanity check against GET /result - the single newest AI
    inference (bbox) batch, without starting a background capture."""
    global AUTH_USER, AUTH_PASS
    AUTH_USER, AUTH_PASS = args.user, args.password

    base = f"http://{args.host}"
    try:
        body = http_get(base + "/result", timeout=8)
    except Exception as e:
        print(f"request failed: {e}")
        return 1
    print(body.decode("utf-8", "replace"))
    return 0


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    p_start = sub.add_parser("start")
    p_start.add_argument("--host", default="192.168.1.112")
    p_start.add_argument("--outdir", default="/home/orangepi/ESP32_wireless/capture")
    p_start.add_argument("--camera", action="store_true", help="AI inference / bbox detection via /stream/result")
    p_start.add_argument("--audio", action="store_true", help="raw PCM audio via /stream/audio")
    p_start.add_argument("--resolution", type=int, default=2, choices=[0, 1, 2],
                          help="0=240x240, 1=480x480, 2=640x480")
    p_start.add_argument("--rate", type=int, default=16000, choices=[16000, 32000])
    p_start.add_argument("--user", required=True, help="HTTP Basic Auth username (must match the board's HTTP_AUTH_USER)")
    p_start.add_argument("--password", required=True, help="HTTP Basic Auth password (must match the board's HTTP_AUTH_PASS)")
    p_start.set_defaults(func=cmd_start)

    p_stop = sub.add_parser("stop")
    p_stop.add_argument("--outdir", default="/home/orangepi/ESP32_wireless/capture")
    p_stop.set_defaults(func=cmd_stop)

    p_status = sub.add_parser("status")
    p_status.add_argument("--outdir", default="/home/orangepi/ESP32_wireless/capture")
    p_status.set_defaults(func=cmd_status)

    p_once = sub.add_parser("once", help="one-shot GET /result, no capture started")
    p_once.add_argument("--host", default="192.168.1.112")
    p_once.add_argument("--user", required=True)
    p_once.add_argument("--password", required=True)
    p_once.set_defaults(func=cmd_once)

    args = p.parse_args()
    sys.exit(args.func(args) or 0)


if __name__ == "__main__":
    main()
