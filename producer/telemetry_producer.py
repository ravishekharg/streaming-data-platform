"""Device telemetry generator for the device-telemetry topic.

Sends realistic readings from N devices plus a controlled share of problem records, and writes an expectations
file so tests can assert exact results end to end:
  - bad records (malformed JSON, out-of-range value, unknown metric, bad device id, future timestamp) -> quarantine
  - duplicates (an already-sent valid event re-sent with the same event_id)                           -> deduped
  - late events (event time 1-3 hours in the past)                                                    -> accepted

  python producer/telemetry_producer.py --events 20000 --devices 200 --expect expectations.json
"""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

METRICS = {"temperature_c": (15.0, 45.0, "C"), "battery_pct": (5.0, 100.0, "%"),
           "signal_rssi_dbm": (-115.0, -60.0, "dBm")}
SITES = ["bangalore", "hyderabad", "chennai", "mumbai", "pune"]


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def valid_event(rng: random.Random, devices: int, now: datetime, late: bool) -> dict:
    metric = rng.choice(list(METRICS))
    lo, hi, unit = METRICS[metric]
    dev = rng.randrange(devices)
    age = timedelta(minutes=rng.uniform(60, 180)) if late else timedelta(seconds=rng.uniform(0, 900))
    return {"event_id": str(uuid.UUID(int=rng.getrandbits(128))), "device_id": f"dev-{dev:04d}",
            "site": SITES[dev % len(SITES)], "metric": metric, "value": round(rng.uniform(lo, hi), 2),
            "unit": unit, "event_time": iso(now - age), "schema_version": "1"}


def bad_event(rng: random.Random, devices: int, now: datetime) -> tuple[str, str]:
    """Returns (payload, expected rejection reason)."""
    kind = rng.choice(["malformed_json", "value_out_of_range", "unknown_metric", "missing_device_id",
                       "event_time_in_future"])
    e = valid_event(rng, devices, now, late=False)
    if kind == "malformed_json":
        return json.dumps(e)[: rng.randint(10, 40)], kind          # truncated JSON
    if kind == "value_out_of_range":
        e["metric"], e["value"] = "battery_pct", 250.0
    elif kind == "unknown_metric":
        e["metric"] = "humidity_pct"
    elif kind == "missing_device_id":
        e["device_id"] = None
    elif kind == "event_time_in_future":
        e["event_time"] = iso(now + timedelta(hours=2))
    return json.dumps(e), kind


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="device-telemetry")
    ap.add_argument("--events", type=int, default=20000, help="total records to send (incl. bad + duplicates)")
    ap.add_argument("--devices", type=int, default=200)
    ap.add_argument("--bad-rate", type=float, default=0.03)
    ap.add_argument("--dup-rate", type=float, default=0.02)
    ap.add_argument("--late-rate", type=float, default=0.02)
    ap.add_argument("--rate", type=float, default=0, help="records per second; 0 = as fast as possible")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--expect", default="expectations.json")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    p = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True, "acks": "all",
                  "linger.ms": 20, "compression.type": "zstd"})
    sent_valid: list[dict] = []
    reasons: dict[str, int] = {}
    stats = {"sent": 0, "valid_unique": 0, "duplicates": 0, "late": 0, "bad": 0}
    now = datetime.now(timezone.utc)
    for i in range(args.events):
        r = rng.random()
        if r < args.bad_rate:
            payload, reason = bad_event(rng, args.devices, now)
            key = None
            stats["bad"] += 1
            reasons[reason] = reasons.get(reason, 0) + 1
        elif r < args.bad_rate + args.dup_rate and sent_valid:
            e = rng.choice(sent_valid)
            payload, key = json.dumps(e), e["device_id"]
            stats["duplicates"] += 1
        else:
            late = rng.random() < args.late_rate / max(1e-9, 1 - args.bad_rate - args.dup_rate)
            e = valid_event(rng, args.devices, now, late)
            sent_valid.append(e)
            payload, key = json.dumps(e), e["device_id"]
            stats["valid_unique"] += 1
            stats["late"] += int(late)
        p.produce(args.topic, value=payload.encode(), key=key.encode() if key else None)
        stats["sent"] += 1
        if i % 1000 == 0:
            p.poll(0)
        if args.rate:
            time.sleep(1 / args.rate)
    remaining = p.flush(30)
    if remaining:
        raise SystemExit(f"{remaining} messages not delivered")
    stats["rejects_by_reason"] = reasons
    with open(args.expect, "w") as fh:
        json.dump(stats, fh, indent=2)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
