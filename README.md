# pgmproxy

Small tool to proxy between pgm, udp or stdin/stdout.

## Building

Requirements (on debian): `libpgm-dev`, `libglib2.0-dev`

Use `make`. If it doesn't work, tweak it until it does.

## Usage

See `pgmproxy -h`.

## Test case

On the receiver, run:

~~~~~~
./pgmproxy 2 'eth0;227.0.0.1' 9000 1 127.0.0.1 7777
nc -u -l -p 7777 | mpv -
~~~~~~

On the sender, run:

~~~~~~
./pgmproxy 1 127.0.0.1 6666 2 'eth0;227.0.0.1' 9000 &
ffmpeg -re -i sober.flac -f flac udp://localhost:6666
~~~~~~
