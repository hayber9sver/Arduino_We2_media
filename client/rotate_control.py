#!/usr/bin/env python3
"""Example client for the WE2 AI-input rotation feature (see app_httpd.cpp's
camera_rotate_handler(), which relays to WE2's AT+ROTATE).

Rotates only the AI model's input (0/90/180/270 deg) - the JPEG preview
stream from /camera/start is never rotated, so this is purely for models
whose object orientation depends on how the camera is physically mounted.

One-shot usage:
    python3 rotate_control.py --host 192.168.1.112 --user USER --password PASS --value 1

Interactive usage (omit --value): press 0/1/2/3 to switch live, q to quit.
    python3 rotate_control.py --host 192.168.1.112 --user USER --password PASS
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

DEG_LABELS = {0: "0deg", 1: "90deg", 2: "180deg", 3: "270deg"}


def auth_header():
    cred = f"{AUTH_USER}:{AUTH_PASS}".encode()
    return "Basic " + base64.b64encode(cred).decode()


def http_get(url, timeout=3):
    req = urllib.request.Request(url, headers={"Authorization": auth_header()})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def set_rotation(host, value):
    url = f"http://{host}/camera/rotate?value={value}"
    try:
        body = http_get(url)
    except urllib.error.HTTPError as e:
        print(f"\rHTTP {e.code} {e.reason}" + " " * 20)
        return
    except urllib.error.URLError as e:
        print(f"\rconnection error: {e.reason}" + " " * 20)
        return
    try:
        reply = json.loads(body)
        applied = reply.get("data")
        print(f"\rrotation = {applied} ({DEG_LABELS.get(applied, '?')})" + " " * 10,
              end="", flush=True)
    except ValueError:
        print(f"\runexpected reply: {body!r}" + " " * 10)


def read_key(fd):
    ch = sys.stdin.read(1)
    if ch in ("0", "1", "2", "3"):
        return int(ch)
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
    p.add_argument("--value", type=int, choices=[0, 1, 2, 3], default=None,
                    help="one-shot: set rotation and exit (0/1/2/3 = 0/90/180/270deg). "
                         "Omit for interactive mode.")
    args = p.parse_args()

    AUTH_USER, AUTH_PASS = args.user, args.password

    if args.value is not None:
        set_rotation(args.host, args.value)
        print()
        return

    print(f"Controlling AI-input rotation at {args.host} - press 0/1/2/3, q to quit.")

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        while True:
            key = read_key(fd)
            if key == "quit":
                break
            if isinstance(key, int):
                set_rotation(args.host, key)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
        print()


if __name__ == "__main__":
    main()
