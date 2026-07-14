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
    GET :8081/stream/result        - AI inference (bbox) results, one JSON
                                      line per detection batch, streamed
                                      continuously while camera is running
    GET :8082/stream/audio         - raw framed PCM audio, streamed
                                      continuously while audio is running
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
                                     batch received from /stream/result
    audio.wav                     - single continuous file, all audio
                                     frames concatenated in arrival order
    media_client.pid              - present while a capture is running
    media_client.log              - background process's own log

This client does not retry or attempt to recover dropped frames/bytes on a
mid-stream network hiccup - it reconnects and keeps going, logging what it
saw. It also does not retry a stalled /stream/result connection instantly:
the ESP32 itself intentionally closes that connection after ~10s with zero
new detections (a deliberate idle-timeout backstop on its side) - this
client treats that the same as any other drop and just reconnects.
"""
import argparse
import base64
import json
import os
import signal
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


def crc16_maxim(data):
    crc = 0x0000
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if (crc & 1) else (crc >> 1)
    return crc ^ 0xFFFF


# ---------------------------------------------------------------------------
# Stream readers (each runs in its own thread inside the background process)
# ---------------------------------------------------------------------------

def read_bbox_stream(base, results_path, stop_event):
    """Pulls AI inference (bbox detection) results off :8081/stream/result -
    a persistent, continuously-streamed connection (see
    stream_result_handler() in app_httpd.cpp), one JSON line (\\r\\n
    terminated) per detection batch. Reconnects on any read error, or when
    the server itself closes the connection after ~10s of no new detections
    (its own deliberate idle-timeout backstop, not an error)."""
    url = base.replace(":80", ":8081") if ":80" in base else base + ":8081"
    url += "/stream/result"
    count = 0
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
                        idx = buf.find(b"\n")
                        while idx >= 0:
                            line = buf[:idx].decode("utf-8", "replace").strip()
                            buf = buf[idx + 1:]
                            if line:
                                try:
                                    entry = json.loads(line)
                                    entry["_client_ts_ms"] = int(time.time() * 1000)
                                    out.write(json.dumps(entry) + "\n")
                                    out.flush()
                                    count += 1
                                except json.JSONDecodeError as e:
                                    log(f"bbox reader: malformed line dropped ({e}): {line[:120]!r}")
                            idx = buf.find(b"\n")
            except Exception as e:
                if not stop_event.is_set():
                    log(f"stream/result reader: {e} - reconnecting")
                    time.sleep(1)
    log(f"bbox reader stopped, {count} detection batches saved")


def read_audio_stream(base, wav_path, stop_event):
    """Pulls raw binary frames off :8082/stream/audio (see MSG_TYPE_AUDIO /
    stream_audio_handler()), unwraps WE2's own framing (0xFF 'S' 'M' 'B' +
    header + PCM + CRC16 - crc mismatches are logged but not treated as
    fatal, just dropped), and appends the PCM into one continuous WAV file
    for the whole capture session."""
    url = base.replace(":80", ":8082") if ":80" in base else base + ":8082"
    url += "/stream/audio"

    wav_writer = None
    wav_rate = None
    frame_count = 0
    crc_bad = 0

    while not stop_event.is_set():
        try:
            req = urllib.request.Request(url, headers={"Authorization": auth_header()})
            with urllib.request.urlopen(req, timeout=10) as resp:
                buf = b""
                while not stop_event.is_set():
                    chunk = resp.read(4096)
                    if not chunk:
                        break
                    buf += chunk
                    while True:
                        idx = buf.find(AUDIO_MAGIC)
                        if idx < 0:
                            if len(buf) > 4:
                                buf = buf[-4:]
                            break
                        if idx > 0:
                            buf = buf[idx:]
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
                                log(f"audio reader: sample rate changed {wav_rate}->{rate} mid-stream, "
                                    f"keeping {wav_rate} in the WAV header (rest of the file will play back "
                                    f"at the wrong speed) - stop/restart the capture after AT+ASR changes")
                            wav_writer.writeframes(payload)
                            frame_count += 1
                        buf = buf[total:]
        except Exception as e:
            if not stop_event.is_set():
                log(f"stream/audio reader: {e} - reconnecting in 3s")
                time.sleep(3)

    if wav_writer is not None:
        wav_writer.close()
    log(f"audio reader stopped, {frame_count} frames written, {crc_bad} CRC mismatches dropped")


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

    def start_with_retries(url, attempts=5, delay=2):
        """The ESP32C3's single-threaded httpd can be briefly unresponsive
        right after a fresh flash/reset (UART1 handshake with WE2 still
        settling - see esp32_camera_web_server_bridge memory). A single
        timeout here used to crash the whole background process (unhandled
        exception -> process exits, leaving nothing running) - retry a few
        times before actually giving up."""
        last_err = None
        for attempt in range(1, attempts + 1):
            try:
                return http_get(url, timeout=8)
            except Exception as e:
                last_err = e
                log(f"start request to {url} failed (attempt {attempt}/{attempts}): {e}")
                if attempt < attempts:
                    time.sleep(delay)
        raise RuntimeError(f"giving up on {url} after {attempts} attempts: {last_err}")

    threads = []
    if want_camera:
        log(f"starting camera / AI inference (resolution={resolution})")
        start_with_retries(base + f"/camera/start?resolution={resolution}&mode=invoke&differed=0")
        threads.append(threading.Thread(target=read_bbox_stream, args=(base, results_path, stop_event)))
    if want_audio:
        log(f"starting audio (rate={rate})")
        start_with_retries(base + f"/audio/start?rate={rate}")
        threads.append(threading.Thread(target=read_audio_stream, args=(base, wav_path, stop_event)))

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
