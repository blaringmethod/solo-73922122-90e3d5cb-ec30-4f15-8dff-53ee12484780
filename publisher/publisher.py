#!/usr/bin/env python3
"""Sample instrument publisher for the fermentation lab archiver.

Publishes QoS 1 JSON messages on the fixed lab topic. Run without arguments
for a guided demo sequence; every knob is also available on the CLI so it can
act as a generic instrument simulator.

Examples (inside the compose network)::

    python publisher.py                      # guided scenario A-E
    python publisher.py --seq 7 --temp 31.2 --ph 6.8
    python publisher.py --seq 3 --temp 99.9  # contradict stored seq 3
    python publisher.py --count 20 --interval 0.2
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

BROKER_HOST = os.getenv("MQTT_HOST", "broker")
BROKER_PORT = int(os.getenv("MQTT_PORT", "1883"))
TOPIC = os.getenv("MQTT_TOPIC", "lab/fermenter/measurement")
DEFAULT_INSTRUMENT = os.getenv("INSTRUMENT_ID", "FERM-01")
DEFAULT_BATCH = os.getenv("BATCH_NO", "BATCH-2026-011")


def make_payload(
    instrument_id: str,
    batch_no: str,
    seq: int,
    temp: float,
    ph: float,
) -> dict:
    return {
        "instrument_id": instrument_id,
        "batch_no": batch_no,
        "sample_seq": seq,
        "ts_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "temperature_c": temp,
        "ph": ph,
    }


def connect_client() -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id=f"instrument-sim-{os.getpid()}",
        protocol=mqtt.MQTTv311,
    )
    client.connect(BROKER_HOST, BROKER_PORT, keepalive=30)
    client.loop_start()
    return client


def publish(
    client: mqtt.Client,
    payload: dict,
    *,
    dup: bool = False,
    timeout: float = 60.0,
) -> int:
    """Publish at QoS 1 and wait for the full publish handshake (PUBCOMP)."""
    info = client.publish(TOPIC, json.dumps(payload), qos=1)
    # wait_for_publish blocks until PUBACK comes back from the broker -- which
    # happens after the archiver's own PUBACK reaches it only when the broker
    # forwards... note: the broker acks the publisher as soon as *it* accepts
    # the message, not after the subscriber acks. So this returning quickly
    # does NOT mean the archiver stored it; use the API/ stats to verify.
    info.wait_for_publish(timeout=timeout)
    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        raise RuntimeError(f"publish failed: rc={info.rc}")
    kind = "re-delivery" if dup else "publish"
    print(
        f"[OK] {kind} mid={info.mid} key=({payload['instrument_id']},"
        f"{payload['batch_no']},#{payload['sample_seq']}) "
        f"temp={payload['temperature_c']} ph={payload['ph']}"
    )
    return info.mid


def guided_scenario(client: mqtt.Client, instrument: str, batch: str) -> None:
    print("=== A. Fresh samples (sequences 1..5) ===")
    for seq, temp, ph in [
        (1, 30.1, 6.80),
        (2, 30.3, 6.82),
        (3, 30.5, 6.79),
        (4, 30.6, 6.77),
        (5, 30.8, 6.75),
    ]:
        publish(client, make_payload(instrument, batch, seq, temp, ph))
        time.sleep(0.15)

    print("\n=== B. Exact duplicates (same key, same values -> ignored) ===")
    publish(client, make_payload(instrument, batch, 1, 30.1, 6.80), dup=True)
    publish(client, make_payload(instrument, batch, 3, 30.5, 6.79), dup=True)
    time.sleep(0.3)

    print("\n=== C. Contradicting readings (same key, different values -> "
          "conflicts, originals kept) ===")
    publish(client, make_payload(instrument, batch, 3, 31.9, 6.79))  # temp
    publish(client, make_payload(instrument, batch, 3, 30.5, 7.30))  # ph
    publish(client, make_payload(instrument, batch, 3, 31.9, 6.79), dup=True)
    time.sleep(0.3)

    print("\n=== D. Burst (queue/back-pressure demo; pair with /demo/pause) ===")
    for seq in range(6, 26):
        publish(
            client,
            make_payload(instrument, batch, seq, 30.0 + seq * 0.05, 6.70),
            timeout=120.0,
        )

    print("\n=== E. Malformed JSON (archiver must ack and not crash) ===")
    info = client.publish(TOPIC, b"{not valid json", qos=1)
    info.wait_for_publish(timeout=30)
    print(f"[OK] malformed publish mid={info.mid}")

    print(
        "\nDone. Inspect:\n"
        f"  GET http://localhost:8000/samples?instrument_id={instrument}"
        f"&batch_no={batch}\n"
        f"  GET http://localhost:8000/conflicts?instrument_id={instrument}"
        f"&batch_no={batch}\n"
        "  GET http://localhost:8000/demo/stats"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fermenter instrument simulator")
    p.add_argument("--host", default=BROKER_HOST)
    p.add_argument("--port", type=int, default=BROKER_PORT)
    p.add_argument("--topic", default=TOPIC)
    p.add_argument("--instrument-id", default=DEFAULT_INSTRUMENT)
    p.add_argument("--batch-no", default=DEFAULT_BATCH)
    p.add_argument("--seq", type=int, default=None,
                   help="sample_seq for a single publish")
    p.add_argument("--temp", type=float, default=None)
    p.add_argument("--ph", type=float, default=None)
    p.add_argument("--count", type=int, default=1,
                   help="number of messages (sequences advance automatically)")
    p.add_argument("--interval", type=float, default=0.5)
    p.add_argument("--guided", action="store_true",
                   help="run the built-in A-E demonstration scenario")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    global BROKER_HOST, BROKER_PORT, TOPIC
    BROKER_HOST, BROKER_PORT, TOPIC = args.host, args.port, args.topic

    client = connect_client()
    try:
        if args.guided or args.seq is None:
            guided_scenario(client, args.instrument_id, args.batch_no)
            return 0
        if args.temp is None or args.ph is None:
            print("--seq requires --temp and --ph", file=sys.stderr)
            return 2
        for i in range(args.count):
            publish(
                client,
                make_payload(
                    args.instrument_id,
                    args.batch_no,
                    args.seq + i,
                    args.temp,
                    args.ph,
                ),
            )
            if i + 1 < args.count:
                time.sleep(args.interval)
    finally:
        time.sleep(0.2)
        client.loop_stop()
        client.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
