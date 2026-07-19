#!/usr/bin/env python3
"""Combined camera+audio+servo stability test against the ESP32 bridge.

Runs media_client.py's proven camera+audio capture (max resolution, one
merged /stream/data connection - see media_client.py's own module docstring)
in the background for the whole test, while this script independently hammers
/motor/left and /motor/right in bursts: burst-count commands paced evenly
across burst-window seconds, then rest-seconds idle, repeating for the whole
--duration. The servo is swept back and forth between --low and --high
(tracking the angle each response reports, flipping direction at either
boundary) rather than always stepping one way, so it's under continuous
motion for the whole test instead of just running into a clamp and stopping.

At the end: stops the capture, measures the resulting audio.wav's total
duration against the actual wall-clock capture time to report an audio
coverage percentage (dropped/missed audio frames make the wav shorter than
wall-clock, so this ratio is a direct coverage measure - same technique
prior audio-stability work in this project used), plus bbox/detection batch
count and control-command success/timing stats.

Usage:
    stability_test.py --host 192.168.1.109 --user admin --password PASS \\
        [--duration 600] [--resolution 2] [--rate 16000] \\
        [--low 45] [--high 135] [--burst-count 20] [--burst-window 3] [--rest 30] \\
        [--outdir ./stability_capture]
"""
import argparse
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import wave


def auth_header(user, password):
    cred = f"{user}:{password}".encode()
    return "Basic " + base64.b64encode(cred).decode()


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def motor_step(base, direction, user, password, timeout=3):
    url = f"{base}/motor/{direction}"
    req = urllib.request.Request(url, headers={"Authorization": auth_header(user, password)})
    t0 = time.time()
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read())
            return {"ok": True, "angle": body.get("angle"), "connected": body.get("connected"),
                    "latency_ms": int((time.time() - t0) * 1000)}
    except Exception as e:
        return {"ok": False, "error": str(e), "latency_ms": int((time.time() - t0) * 1000)}


def run_control_loop(host, user, password, low, high, burst_count, burst_window, rest, duration, log_path):
    base = f"http://{host}"
    direction = "right"  # sweep up toward `high` first
    angle = None
    start = time.time()
    round_num = 0
    with open(log_path, "w") as logf:
        while time.time() - start < duration:
            round_num += 1
            round_start = time.time()
            ok_count = 0
            per_cmd_gap = burst_window / burst_count
            for i in range(burst_count):
                cmd_t0 = time.time()
                r = motor_step(base, direction, user, password)
                logf.write(json.dumps({"round": round_num, "cmd_idx": i, "direction": direction,
                                        "ts": time.time(), **r}) + "\n")
                logf.flush()
                if r["ok"]:
                    ok_count += 1
                    if r["angle"] is not None:
                        angle = r["angle"]
                        # 2026-07-19: check the boundary after EVERY command,
                        # not just once at the end of the 20-command burst -
                        # hardware-verified bug: a burst starting near one
                        # edge could otherwise run the full 20 steps (up to
                        # 40 degrees) before the old end-of-burst-only check
                        # ever looked, overshooting well past --low/--high
                        # (reproduced: swung to 170/10 with a 45-135 target).
                        if angle >= high:
                            direction = "left"
                        elif angle <= low:
                            direction = "right"
                sleep_left = per_cmd_gap - (time.time() - cmd_t0)
                if sleep_left > 0:
                    time.sleep(sleep_left)
            round_elapsed = time.time() - round_start
            over_budget = " OVER-BUDGET" if round_elapsed > burst_window + 0.5 else ""
            log(f"round {round_num}: dir={direction} ok={ok_count}/{burst_count} "
                f"angle={angle} took={round_elapsed:.2f}s{over_budget}")
            remaining = duration - (time.time() - start)
            if remaining <= 0:
                break
            time.sleep(min(rest, remaining))
    log(f"control loop done, {round_num} rounds")


def wait_for_capture_ready(outdir, timeout=120):
    """Blocks until media_client.py's background capture has actually
    started producing audio data - NOT just that its 'start' subcommand
    returned (that only confirms the background process forked, not that
    /camera/start and /audio/start actually succeeded against WE2, which
    itself retries internally for up to ~80s).

    2026-07-19: hardware-verified this distinction matters a lot - starting
    the servo control loop immediately, racing camera/audio's own startup
    retries, measurably reduces the odds camera+audio ever come up at all
    (both compete for the same shared s_cmd_mutex/I2C bus, and a burst of
    motor commands landing mid-retry starved every one of camera/start's 5
    attempts in one run). Letting camera+audio establish uncontended first,
    then layering servo traffic on top of an already-healthy stream, is far
    more reliable. Returns True once audio.wav exists with real data,
    False if the capture process died first or the timeout elapsed."""
    pf = os.path.join(outdir, "media_client.pid")
    wav_path = os.path.join(outdir, "audio.wav")
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not os.path.exists(pf):
            log("capture process exited before producing any audio - see media_client.log")
            return False
        if os.path.exists(wav_path) and os.path.getsize(wav_path) > 0:
            log("capture confirmed producing audio data - starting servo control loop")
            return True
        time.sleep(2)
    log(f"capture did not produce audio within {timeout}s - proceeding anyway")
    return False


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", required=True)
    ap.add_argument("--user", required=True)
    ap.add_argument("--password", required=True)
    ap.add_argument("--outdir", default="/home/orangepi/ESP32_wireless/stability_capture")
    ap.add_argument("--duration", type=int, default=600, help="total test duration, seconds")
    ap.add_argument("--resolution", type=int, default=2, help="0=240x240 1=480x480 2=640x480(max)")
    ap.add_argument("--rate", type=int, default=16000, choices=[16000, 32000])
    ap.add_argument("--low", type=int, default=45)
    ap.add_argument("--high", type=int, default=135)
    ap.add_argument("--burst-count", type=int, default=20)
    ap.add_argument("--burst-window", type=float, default=3.0)
    ap.add_argument("--rest", type=float, default=30.0)
    ap.add_argument("--media-client", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "media_client.py"))
    args = ap.parse_args()

    outdir = os.path.abspath(args.outdir)
    os.makedirs(outdir, exist_ok=True)
    control_log = os.path.join(outdir, "control_log.jsonl")

    log(f"starting camera+audio capture (resolution={args.resolution}, rate={args.rate}) -> {outdir}")
    subprocess.run([sys.executable, args.media_client, "start",
                     "--host", args.host, "--outdir", outdir,
                     "--camera", "--audio",
                     "--resolution", str(args.resolution), "--rate", str(args.rate),
                     "--user", args.user, "--password", args.password], check=True)

    pf = os.path.join(outdir, "media_client.pid")
    if not wait_for_capture_ready(outdir) and not os.path.exists(pf):
        log("capture process crashed during startup - aborting rather than running the "
            "control loop against a dead capture (check media_client.log)")
        sys.exit(1)

    capture_start_ts = time.time()
    try:
        log(f"starting control loop: sweep {args.low}-{args.high}deg, "
            f"{args.burst_count} cmds/{args.burst_window}s burst, {args.rest}s rest, "
            f"total {args.duration}s")
        run_control_loop(args.host, args.user, args.password, args.low, args.high,
                          args.burst_count, args.burst_window, args.rest, args.duration, control_log)
    finally:
        capture_end_ts = time.time()
        log("stopping capture...")
        subprocess.run([sys.executable, args.media_client, "stop", "--outdir", outdir])
        time.sleep(2)  # let the reader thread flush/close audio.wav after SIGTERM

    wall_elapsed = capture_end_ts - capture_start_ts

    wav_path = os.path.join(outdir, "audio.wav")
    audio_seconds = None
    coverage = None
    if os.path.exists(wav_path):
        try:
            with wave.open(wav_path, "rb") as w:
                audio_seconds = w.getnframes() / float(w.getframerate())
            coverage = audio_seconds / wall_elapsed * 100
        except Exception as e:
            log(f"could not read {wav_path}: {e}")

    results_path = os.path.join(outdir, "results.jsonl")
    bbox_count = 0
    if os.path.exists(results_path):
        with open(results_path) as f:
            bbox_count = sum(1 for _ in f)

    total_cmds = 0
    ok_cmds = 0
    latencies = []
    if os.path.exists(control_log):
        with open(control_log) as f:
            for line in f:
                e = json.loads(line)
                total_cmds += 1
                if e.get("ok"):
                    ok_cmds += 1
                    latencies.append(e["latency_ms"])

    print("\n" + "=" * 60)
    print("STABILITY TEST SUMMARY")
    print("=" * 60)
    print(f"wall-clock capture duration : {wall_elapsed:.1f}s")
    if audio_seconds is not None:
        print(f"audio captured               : {audio_seconds:.1f}s")
        print(f"AUDIO COVERAGE                : {coverage:.2f}%")
    else:
        print("audio captured / coverage     : N/A (no audio.wav produced)")
    print(f"bbox/detection batches        : {bbox_count}")
    print(f"control commands sent         : {total_cmds} (ok={ok_cmds}, fail={total_cmds - ok_cmds})")
    if latencies:
        latencies.sort()
        print(f"control latency ms (min/median/max): "
              f"{latencies[0]}/{latencies[len(latencies)//2]}/{latencies[-1]}")
    print(f"outdir                        : {outdir}")


if __name__ == "__main__":
    main()
