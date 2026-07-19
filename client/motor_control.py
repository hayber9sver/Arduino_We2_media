#!/usr/bin/env python3
"""Interactive keyboard control for the PCA9685/SG92R servo (see
app_httpd.cpp's motor_left_handler()/motor_right_handler()).

Each left/right key press sends exactly one HTTP GET to /motor/left or
/motor/right - the ESP32 does one fixed 2-degree step per call (clamped to
[0,180] on the firmware side), this script never sends an angle or a step
count. Holding a key down just repeats the OS's own key-repeat, which
naturally becomes repeated single-step calls - no special handling needed.

Controls:
    Left arrow / a     -> /motor/left  (-2 degrees)
    Right arrow / d     -> /motor/right (+2 degrees)
    q / Ctrl-C          -> quit

Usage:
    python3 motor_control.py --host 192.168.1.112 --user USER --password PASS
"""

import argparse
import base64
import json
import sys
import termios
import tty
import urllib.error
import urllib.request

AUTH_USER = None
AUTH_PASS = None


def auth_header():
    cred = f"{AUTH_USER}:{AUTH_PASS}".encode()
    return "Basic " + base64.b64encode(cred).decode()


def http_get(url, timeout=3):
    req = urllib.request.Request(url, headers={"Authorization": auth_header()})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def step(host, direction):
    url = f"http://{host}/motor/{direction}"
    try:
        body = http_get(url)
    except urllib.error.HTTPError as e:
        print(f"\r[{direction}] HTTP {e.code} {e.reason}" + " " * 20)
        return
    except urllib.error.URLError as e:
        print(f"\r[{direction}] connection error: {e.reason}" + " " * 20)
        return
    try:
        reply = json.loads(body)
        angle, connected = reply["angle"], reply.get("connected", True)
        note = "" if connected else "  (PCA9685 not responding)"
        print(f"\r[{direction}] angle = {angle:3d} deg{note}" + " " * 10, end="", flush=True)
    except (ValueError, KeyError):
        print(f"\r[{direction}] unexpected reply: {body!r}" + " " * 10)


def read_key(fd):
    """Reads one keypress, resolving arrow-key escape sequences
    (ESC [ C / ESC [ D) to a single 'left'/'right'/'quit'/None token."""
    ch = sys.stdin.read(1)
    if ch == "\x1b":
        rest = sys.stdin.read(2)
        if rest == "[D":
            return "left"
        if rest == "[C":
            return "right"
        return None
    if ch in ("a", "A"):
        return "left"
    if ch in ("d", "D"):
        return "right"
    if ch in ("q", "Q", "\x03"):  # \x03 = Ctrl-C
        return "quit"
    return None


def main():
    global AUTH_USER, AUTH_PASS

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", default="192.168.1.112", help="ESP32 IP address")
    p.add_argument("--user", required=True, help="HTTP Basic Auth username (must match the board's HTTP_AUTH_USER)")
    p.add_argument("--password", required=True,
                    help="HTTP Basic Auth password (must match the board's HTTP_AUTH_PASS)")
    args = p.parse_args()

    AUTH_USER, AUTH_PASS = args.user, args.password

    print(f"Controlling servo at {args.host} - Left/Right arrows (or a/d) to step, q to quit.")

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        while True:
            key = read_key(fd)
            if key == "quit":
                break
            if key in ("left", "right"):
                step(args.host, key)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        print()


if __name__ == "__main__":
    main()
