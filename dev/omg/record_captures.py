"""Records OMG's transmission captures, scrubbed, as L1 fixtures.

    uv run python dev/omg/record_captures.py <out dir> --prefix springfield-payloads/ [--limit N]

Reads the request and response pairs under the prefix in `--bucket` (LocalStack when
AWS_ENDPOINT_URL is set) and writes each pair to `<out dir>/NNN.json`, as
`{"prefix", "name", "request": {"metadata", "document"} or null, "response": {...}}`. The
prefix's label and layout come from `config/systems/omg.yml`. Pairs with a status class or
body shapes not seen yet come first, then the rest in key order, so a small `--limit` takes the
most varied.

Every key and every shape stays, never a value. Each body leaf becomes a fixed placeholder of
the type FIRE's generalizer gives it, keeping keys and list lengths, and a pair whose scrubbed
body fingerprints differently from the original is refused. Header values, `url`, and `error`
become placeholders; `method`, `status_code`, `api_version`, and `error_type` stay. In the key,
each work segment becomes a fake ULID and each order id `ORDER<n>`; `unattributed`, `unknown`,
the action, and the timestamp stay. Metadata keeps `action`, `direction`, `api_version`,
`channel`, and `notification_type`; `work_unit_id` takes the key's fake ULID and `order_id`
becomes a placeholder. Body keys are kept as they are: review each file before committing it,
keys included (dev/omg/README.md, section c).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from fire.adapters.shapes.flat_star import FlatStarShapeGeneralizer
from fire.adapters.shapes.sha256 import Sha256ShapeFingerprinter
from fire.apps.config import S3CaptureSourceConfig, load_system_config
from fire.core.domain.capture import (
    JSON_SUFFIX,
    REQUESTS,
    RESPONSES,
    CaptureLayout,
    CaptureRule,
    checked_action,
    read_capture_key,
)
from fire.core.domain.dossier import ShapeFingerprint, ValueType
from fire.core.domain.observation import StatusClass
from fire.core.domain.operation import is_id

PLACEHOLDER = "redacted"
DEFAULT_BUCKET = "order-management-gateway-transmission-data-dev"
SYSTEM_CONFIG = Path(__file__).resolve().parents[2] / "config" / "systems" / "omg.yml"
# Literals OMG writes in place of a work unit or an order id it does not know.
UNATTRIBUTED = "unattributed"
UNKNOWN_ORDER = "unknown"
FAKE_ULID_HEAD = "0FAKE"
ULID = re.compile(r"[0-7][0-9A-HJKMNP-TV-Z]{25}")
ORDER_PREFIX = "ORDER"
# One fixed example per string form the generalizer refines.
STRING_PLACEHOLDERS = {
    ValueType.UUID: "00000000-0000-0000-0000-000000000000",
    ValueType.EMAIL: "redacted@example.com",
    ValueType.URI: "https://example.com/",
    ValueType.DATE: "1970-01-01",
    ValueType.DATETIME: "1970-01-01T00:00:00Z",
    ValueType.STRING: PLACEHOLDER,
}
REDACTED_FIELDS = frozenset({"url", "error"})
KEPT_METADATA = frozenset({"action", "direction", "api_version", "channel", "notification_type"})
_METHOD = re.compile(r"[A-Z]{1,16}")
_API_VERSION = re.compile(r"[a-z0-9][a-z0-9._-]{0,15}")

GENERALIZER = FlatStarShapeGeneralizer()
FINGERPRINTER = Sha256ShapeFingerprinter()


class Refused(Exception):
    """A pair that cannot be written without a value or a changed shape. Never holds a value."""


@dataclass
class Half:
    metadata: dict[str, str]
    document: dict[str, Any]


@dataclass
class Pair:
    name: str
    request: Half | None
    response: Half


@dataclass
class Pseudonyms:
    """One fake ULID per source work segment and one `ORDER<n>` per order id, in first use."""

    works: dict[str, str] = field(default_factory=dict)
    orders: dict[str, str] = field(default_factory=dict)

    def work(self, value: str) -> str:
        if value == UNATTRIBUTED:
            return value
        if ULID.fullmatch(value) is None:
            raise Refused("work segment is neither a ULID nor unattributed")
        fake = self.works.setdefault(value, f"{FAKE_ULID_HEAD}{len(self.works) + 1:021d}")
        assert ULID.fullmatch(fake)
        return fake

    def order(self, value: str) -> str:
        if value == UNKNOWN_ORDER:
            return value
        return self.orders.setdefault(value, f"{ORDER_PREFIX}{len(self.orders) + 1}")


def scrub_body(value: Any) -> Any:
    """`value` with its keys and list lengths, each leaf a placeholder of its shape type."""
    if isinstance(value, dict):
        return {key: scrub_body(item) for key, item in value.items()}
    if isinstance(value, list):
        return [scrub_body(item) for item in value]
    if value is None:
        return None
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return 0
    if isinstance(value, float):
        return 0.0
    if isinstance(value, str):
        return STRING_PLACEHOLDERS[GENERALIZER.generalize(value).fields[0].value_type]
    return PLACEHOLDER


def scrub_document(document: Mapping[str, Any]) -> dict[str, Any]:
    """The capture document with every value replaced but the kept names and status code."""
    scrubbed: dict[str, Any] = {}
    for key, value in document.items():
        if key == "headers" and isinstance(value, dict):
            scrubbed[key] = dict.fromkeys(value, PLACEHOLDER)
        elif key in REDACTED_FIELDS and isinstance(value, str):
            scrubbed[key] = PLACEHOLDER
        elif key != "body" and _is_kept(key, value):
            scrubbed[key] = value
        else:
            scrubbed[key] = scrub_body(value)
    if "body" in document and _fingerprint(document["body"]) != _fingerprint(scrubbed["body"]):
        raise Refused("the scrubbed body has another shape")
    return scrubbed


def _is_kept(key: str, value: Any) -> bool:
    if key == "status_code":
        return value is None or (isinstance(value, int) and not isinstance(value, bool))
    if not isinstance(value, str):
        return False
    if key == "method":
        return _METHOD.fullmatch(value) is not None
    if key == "api_version":
        return _API_VERSION.fullmatch(value) is not None
    if key == "error_type":
        return value.isidentifier() and len(value) <= 100
    return False


def _fingerprint(body: Any) -> ShapeFingerprint:
    return FINGERPRINTER.fingerprint(GENERALIZER.generalize(body))


def scrub_metadata(metadata: Mapping[str, str], names: Pseudonyms) -> dict[str, str]:
    kept: dict[str, str] = {}
    for key, value in sorted(metadata.items()):
        if key in KEPT_METADATA:
            if checked_action(value) is None:
                raise Refused(f"metadata {key} is not a name")
            kept[key] = value
        elif key == "work_unit_id":
            kept[key] = names.work(value)
        elif key == "order_id":
            kept[key] = PLACEHOLDER if value else value
    return kept


def scrub_name(name: str, layout: CaptureLayout, names: Pseudonyms) -> str:
    """The key after `{prefix}responses/` with its work segment and order id replaced."""
    key = read_capture_key(name, layout)
    if key is None:
        raise Refused(f"the key does not read as {layout}")
    head = name.partition("/")[2].removesuffix(JSON_SUFFIX).removesuffix(f"_{key.stamp}")
    if layout is CaptureLayout.WORK_UNIT_ORDER:
        head = names.order(head)
    else:
        order, _, action = head.partition("_")
        if layout is CaptureLayout.WORK_UNIT_ACTION:
            order, action = "", head
        if checked_action(action) is None:
            raise Refused("the key's action is not a name")
        head = f"{names.order(order)}_{action}" if order else action
    return f"{names.work(key.work)}/{head}_{key.stamp}{JSON_SUFFIX}"


def scrub_pair(pair: Pair, rule: CaptureRule, names: Pseudonyms) -> dict[str, Any]:
    def half(part: Half) -> dict[str, Any]:
        return {
            "metadata": scrub_metadata(part.metadata, names),
            "document": scrub_document(part.document),
        }

    return {
        "prefix": rule.prefix,
        "name": scrub_name(pair.name, rule.layout, names),
        "request": half(pair.request) if pair.request is not None else None,
        "response": half(pair.response),
    }


def status_class(document: Mapping[str, Any]) -> str:
    code = document.get("status_code")
    if isinstance(code, int) and not isinstance(code, bool):
        return str(StatusClass.of(code) or "invalid")
    return str(StatusClass.NO_RESPONSE)


def channel(rule: CaptureRule, pair: Pair) -> str:
    """The channel the pair's response gives, as the tentacle names it."""
    key = read_capture_key(pair.name, rule.layout)
    action = key.action if key else None
    if rule.layout is not CaptureLayout.WORK_UNIT_ACTION:
        action = pair.response.metadata.get("action") or (
            pair.request.metadata.get("action") if pair.request else None
        )
    return (
        f"{rule.label}:{action or '(no action)'} response {status_class(pair.response.document)}"
    )


def id_like_keys(value: Any) -> int:
    """How many body keys look like ids: a map keyed by data puts values into keys."""
    if isinstance(value, dict):
        return sum(is_id(key) + id_like_keys(item) for key, item in value.items())
    if isinstance(value, list):
        return sum(id_like_keys(item) for item in value)
    return 0


def _signature(pair: Pair) -> tuple[Any, ...]:
    request = pair.request.document if pair.request else None
    return (
        status_class(pair.response.document),
        request is None,
        _fingerprint(request.get("body")).schema_hash if request else None,
        _fingerprint(pair.response.document.get("body")).schema_hash,
    )


def by_variety(pairs: list[Pair]) -> list[Pair]:
    """Pairs with a signature not seen yet first, then the rest, each in key order."""
    seen: set[tuple[Any, ...]] = set()
    first, rest = [], []
    for pair in pairs:
        signature = _signature(pair)
        (rest if signature in seen else first).append(pair)
        seen.add(signature)
    return first + rest


def capture_rule(prefix: str, system_config: Path) -> CaptureRule:
    system = load_system_config(system_config)
    rules = [
        rule
        for source in system.sources
        if isinstance(source, S3CaptureSourceConfig)
        for rule in source.captures
    ]
    for rule in rules:
        if rule.prefix == prefix:
            return rule
    declared = ", ".join(rule.prefix for rule in rules)
    raise SystemExit(f"{system_config} declares no capture prefix {prefix}; it has: {declared}")


def s3_client() -> Any:
    if os.environ.get("AWS_ENDPOINT_URL"):
        return boto3.client("s3", config=Config(s3={"addressing_style": "path"}))
    return boto3.client("s3")


def read_half(s3: Any, bucket: str, key: str) -> Half | None:
    try:
        stored = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in {"NoSuchKey", "404"}:
            return None
        raise
    try:
        document = json.loads(stored["Body"].read())
    except ValueError:
        raise Refused("an object is not JSON") from None
    if not isinstance(document, dict):
        raise Refused("an object is not a JSON object")
    return Half(metadata=dict(stored.get("Metadata") or {}), document=document)


def read_pairs(s3: Any, bucket: str, rule: CaptureRule) -> list[Pair]:
    responses = f"{rule.prefix}{RESPONSES}/"
    names = sorted(
        item["Key"].removeprefix(responses)
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=responses)
        for item in page.get("Contents") or []
    )
    pairs = []
    for serial, name in enumerate(names, start=1):
        try:
            response = read_half(s3, bucket, f"{responses}{name}")
            if response is None:
                continue
            request = read_half(s3, bucket, f"{rule.prefix}{REQUESTS}/{name}")
        except Refused as refused:
            print(f"skipped listed pair {serial}: {refused}", file=sys.stderr)
            continue
        pairs.append(Pair(name=name, request=request, response=response))
    return pairs


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("out", type=Path)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--system-config", type=Path, default=SYSTEM_CONFIG)
    args = parser.parse_args()
    rule = capture_rule(args.prefix, args.system_config)
    pairs = by_variety(read_pairs(s3_client(), args.bucket, rule))[: args.limit]
    args.out.mkdir(parents=True, exist_ok=True)
    names = Pseudonyms()
    written = refused = 0
    for pair in pairs:
        try:
            fixture = scrub_pair(pair, rule, names)
        except Refused as reason:
            refused += 1
            print(f"refused a pair: {reason}", file=sys.stderr)
            continue
        written += 1
        path = args.out / f"{written:03d}.json"
        path.write_text(json.dumps(fixture, indent=1) + "\n", encoding="utf-8")
        print(f"recorded {path}: {channel(rule, pair)}", flush=True)
        bodies = [pair.response.document.get("body")]
        if pair.request is not None:
            bodies.append(pair.request.document.get("body"))
        if suspicious := sum(id_like_keys(body) for body in bodies):
            print(f"  review {path}: {suspicious} body keys look like ids", flush=True)
    print(f"{written} recorded, {refused} refused, from {len(pairs)} pairs", flush=True)
    if refused:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
