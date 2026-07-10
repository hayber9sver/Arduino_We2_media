Project Description

This is an Arduino sketch running on a XIAO ESP32C3, acting as a wireless bridge layer for a WE2 device (Grove Vision AI V2 / SSCMA). The ESP32C3 talks to WE2 over UART1 (Grove header pins, PB7=TX/PB6=RX) and wraps WE2's AT command protocol into an HTTP API, so you can control the camera/audio, pull detection results, and read data streams directly over the network — no web UI, purely an API/streaming backend.

Architecture

- Control: plain HTTP GET with query params (/camera/start?resolution=X, /audio/start?rate=X, etc.) — one request, one response
- Continuous data: either an always-open connection that keeps streaming (/stream/frame, /stream/audio), or long-polling (/result, each call waits up to 3s for one new result)
- Capture tool: client/media_client.py — configures settings, starts/stops, saves output, runs as a background daemon; defaults to saving under /home/orangepi/ESP32_wireless/capture

API Endpoints

┌───────────────────────────────────────────────────┬────────────────────────────────────────────────────────────────────┐
│                     Endpoint                      │                              Purpose                               │
├───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┤
│ /camera/start?resolution=0|1|2&mode=invoke|sample │ Start the camera (240x240/480x480/640x480)                         │
├───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┤
│ /camera/stop                                      │ Stop (AT+BREAK — shared stop command for both camera and audio)    │
├───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┤
│ /audio/start?rate=16000|32000                     │ Start audio                                                        │
├───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┤
│ /audio/stop                                       │ Same shared BREAK                                                  │
├───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┤
│ /command?base64=...                               │ Pass through any raw AT command                                    │
├───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┤
│ /result                                           │ Long-poll for one bbox detection result (JSON, no image data)      │
├───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┤
│ /stream/frame (port 8080)                         │ Continuous MJPEG image stream                                      │
├───────────────────────────────────────────────────┼────────────────────────────────────────────────────────────────────┤
│ /stream/audio (port 8080)                         │ Continuous raw binary audio stream (WE2's own custom frame format) │
└───────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────┘

Disconnect protection: /stream/frame and /stream/audio detect client disconnects and automatically send AT+BREAK to stop the WE2-side stream, with a debounce guard to prevent repeated commands if this ever fires in a burst.

Capability Limits

1. UART1 baud rate capped at 921600bps — this is the documented hardware ceiling in WE2's driver, not adjustable further.
2. Very limited memory (ESP32C3 only has ~86KB free heap):
  - PTR_BUFFER_SIZE=3: the shared buffer only holds 3 slots — multiple concurrent streams evict each other's data
  - JPG_BUFFER_SIZE=8KB: only enough for the 240x240 debug preview; 480x480/640x480 will fail to decode
  - Single-threaded httpd: only one request is handled at a time; under heavy load, requests queue up and lag
3. 32kHz audio + bbox running together is unstable (root-caused: WE2's own 32kHz audio generation is confirmed clean in isolation — the problem is that it doesn't fit alongside bbox/INVOKE traffic on the shared 921600 UART1 link, occasionally crashing the board or dropping large chunks of audio). 16kHz audio + bbox is confirmed stable (~90 seconds combined test time, zero crashes, 100% bbox integrity / 99.5% audio integrity).
4. Running camera + audio + image streaming all at once overloads the board — running all three simultaneously exceeds capacity; only enable what you actually need (e.g., a human presence sensor only needs bbox + audio, not saved video frames).
5. Occasional crash-and-reboot after extended operation (root cause not fully identified, suspected heap fragmentation; recovers on its own after rebooting, but long-term 24/7 reliability hasn't been verified).
6. No longer supports the ESP32S3 board — the code has been trimmed to target C3 only, removing the BYTETracker tracking algorithm that was only ever enabled for S3.
