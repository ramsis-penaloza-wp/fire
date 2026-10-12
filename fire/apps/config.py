"""Load YAML config. Credential values stay as env var *names* until wiring."""

from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from fire.core.domain.baseline import DriftPolicy
from fire.core.domain.envelope import DottedPath, Envelope
from fire.core.domain.flow import PathPolicy
from fire.core.domain.observation import ChannelDirection
from fire.core.domain.operation import OperationRules


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ApiConfig(BaseModel):
    token_env: str = "FIRE_API_TOKEN"


class ReasonerConfig(BaseModel):
    provider: str = "echo"
    base_url_env: str | None = None
    api_key_env: str | None = None


class NarratorConfig(BaseModel):
    provider: str = "template"
    base_url_env: str = "FIRE_NARRATOR_BASE_URL"
    api_key_env: str = "FIRE_NARRATOR_API_KEY"
    model_env: str = "FIRE_NARRATOR_MODEL"


class NotifierConfig(BaseModel):
    provider: str = "console"


class BudgetsConfig(BaseModel):
    tier0_ms: int = 300
    tier1_ms: int = 3000
    tier2_ms: int = 1500


class RetryConfig(_Strict):
    # The first backoff doubles per attempt up to 30 s, with full jitter.
    max_attempts: int = Field(default=6, ge=1)
    backoff_ms: int = Field(default=500, ge=0)


class TentacleConfig(_Strict):
    """How the tentacle sends observations to the body. Contract: docs/specs/tentacle.md."""

    body_url_env: str = Field(default="FIRE_BODY_URL", min_length=1)
    batch_size: int = Field(default=200, ge=1)
    flush_ms: int = Field(default=1000, ge=1)
    buffer_size: int = Field(default=10000, ge=1)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    shutdown_s: float = Field(default=10, gt=0)


class FireConfig(BaseModel):
    api: ApiConfig = Field(default_factory=ApiConfig)
    reasoner: ReasonerConfig = Field(default_factory=ReasonerConfig)
    narrator: NarratorConfig = Field(default_factory=NarratorConfig)
    notifier: NotifierConfig = Field(default_factory=NotifierConfig)
    budgets: BudgetsConfig = Field(default_factory=BudgetsConfig)
    tentacle: TentacleConfig = Field(default_factory=TentacleConfig)


def load_fire_config(path: Path) -> FireConfig:
    raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    return FireConfig.model_validate(raw)


# Per-system config: what a person decides. What the system contains is discovered, never
# listed here. Contract: docs/specs/system-config-and-discovery.md.


class SlackTarget(_Strict):
    # Public channel ids start with C, private ones with G. The id survives channel renames.
    channel_id: str = Field(pattern=r"^[CG][A-Z0-9]{8,}$")
    channel_name: str | None = Field(default=None, pattern=r"^#[a-z0-9_-]{1,80}$")


class NotifyConfig(_Strict):
    slack: SlackTarget


class ManifestConfig(_Strict):
    kind: str = Field(min_length=1)
    path_env: str = Field(min_length=1)


class DiscoveryConfig(_Strict):
    manifest: ManifestConfig | None = None


# Sources: where the tentacle reads. Contract: docs/specs/tentacle.md#sources-in-the-system-yaml.

Slug = Annotated[str, Field(pattern=r"^[a-z0-9][a-z0-9_-]*$")]
EnvName = Annotated[str, Field(min_length=1)]
Regions = Annotated[list[Annotated[str, Field(min_length=1)]], Field(min_length=1)]


class _Source(_Strict):
    # The `source` of every observation read from it; unique within the system.
    id: Slug

    @model_validator(mode="before")
    @classmethod
    def _dedupe_key_was_removed(cls, data: Any) -> Any:
        if isinstance(data, dict) and "dedupe_key" in data:
            raise ValueError("dedupe_key was removed: declare envelope.event_id instead")
        return data

    @property
    def location_env(self) -> str:
        """The env var naming what the source reads: a path, stream, table, topic, bucket,
        upstream, or listen address.
        """
        raise NotImplementedError


class EventSourceConfig(_Source):
    """A source whose records carry events, read through an envelope."""

    direction: ChannelDirection
    envelope: Envelope

    @field_validator("direction")
    @classmethod
    def _events_enter_or_stay(cls, direction: ChannelDirection) -> ChannelDirection:
        if direction is ChannelDirection.OUTBOUND:
            raise ValueError("an event source is inbound or internal")
        return direction


class ReplaySourceConfig(EventSourceConfig):
    kind: Literal["replay"]
    # A .json file (an object or a list), a .jsonl file, or a directory of them.
    path_env: EnvName

    @property
    def location_env(self) -> str:
        return self.path_env


class KinesisSourceConfig(EventSourceConfig):
    kind: Literal["kinesis"]
    stream_env: EnvName
    regions: Regions

    @property
    def location_env(self) -> str:
        return self.stream_env


class DynamoDbStreamFilter(_Strict):
    attribute: DottedPath
    equals: str


class DynamoDbStreamSourceConfig(EventSourceConfig):
    kind: Literal["dynamodb_stream"]
    table_env: EnvName
    regions: Regions
    filter: DynamoDbStreamFilter | None = None

    @property
    def location_env(self) -> str:
        return self.table_env


class SnsFanoutSourceConfig(EventSourceConfig):
    kind: Literal["sns_fanout"]
    topic_env: EnvName
    regions: Regions

    @property
    def location_env(self) -> str:
        return self.topic_env


class CaptureLayout(StrEnum):
    WORK_UNIT_ACTION = "work_unit_action"
    WORK_UNIT_ORDER_ACTION = "work_unit_order_action"
    WORK_UNIT_ORDER = "work_unit_order"


class CaptureConfig(_Strict):
    prefix: str = Field(min_length=1)
    # Starts the channel names of this client's calls.
    label: Slug
    layout: CaptureLayout


class S3CaptureSourceConfig(_Source):
    kind: Literal["s3_capture"]
    bucket_env: EnvName
    captures: list[CaptureConfig] = Field(min_length=1)

    @model_validator(mode="after")
    def _prefixes_are_unique(self) -> S3CaptureSourceConfig:
        prefixes = [capture.prefix for capture in self.captures]
        duplicates = sorted({prefix for prefix in prefixes if prefixes.count(prefix) > 1})
        if duplicates:
            raise ValueError(f"duplicate capture prefixes: {', '.join(duplicates)}")
        return self

    @property
    def location_env(self) -> str:
        return self.bucket_env


class HttpProxySourceConfig(_Source):
    """A reverse proxy in front of an HTTP service. Contract:
    docs/specs/http-and-span-capture.md#inbound-proxy.
    """

    kind: Literal["http_proxy"]
    listen_port: int = Field(ge=1, le=65535)
    listen_host: str = Field(default="127.0.0.1", min_length=1)
    # Holds the upstream's base URL, such as http://localhost:8020.
    upstream_env: EnvName
    openapi_path: str = Field(default="/openapi.json", pattern=r"^/\S*$")
    openapi_refresh_s: float = Field(default=300, gt=0)
    max_body_bytes: int = Field(default=1_048_576, ge=1)

    @property
    def location_env(self) -> str:
        return self.upstream_env


class OtlpSourceConfig(_Source):
    """An OTLP/HTTP JSON receiver for the system's traces. Contract:
    docs/specs/http-and-span-capture.md#otlp-receiver.
    """

    kind: Literal["otlp"]
    # Holds the address the receiver listens on, host:port, such as 127.0.0.1:4318.
    listen_env: EnvName

    @property
    def location_env(self) -> str:
        return self.listen_env


SourceConfig = Annotated[
    ReplaySourceConfig
    | KinesisSourceConfig
    | DynamoDbStreamSourceConfig
    | SnsFanoutSourceConfig
    | S3CaptureSourceConfig
    | HttpProxySourceConfig
    | OtlpSourceConfig,
    Field(discriminator="kind"),
]


class PolicyConfig(_Strict):
    critical: list[str] = Field(default_factory=list)
    ignore: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _critical_and_ignore_are_disjoint(self) -> PolicyConfig:
        both = sorted(set(self.critical) & set(self.ignore))
        if both:
            raise ValueError(f"names both critical and ignored: {', '.join(both)}")
        return self


class FingerprintingConfig(_Strict):
    sample_rate: float = Field(default=1.0, gt=0, le=1)


class SystemConfig(_Strict):
    system: Slug
    display_name: str
    notify: NotifyConfig
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    sources: list[SourceConfig] = Field(default_factory=list)
    # How outbound calls are named. Contract: docs/specs/http-and-span-capture.md#operation-names.
    operations: OperationRules = Field(default_factory=OperationRules)
    policy: PolicyConfig = Field(default_factory=PolicyConfig)
    fingerprinting: FingerprintingConfig = Field(default_factory=FingerprintingConfig)
    drift: DriftPolicy = Field(default_factory=DriftPolicy)
    # When flows settle. Contract: docs/specs/flows-and-paths.md#completion.
    paths: PathPolicy = Field(default_factory=PathPolicy)

    @model_validator(mode="after")
    def _source_ids_are_unique(self) -> SystemConfig:
        ids = [source.id for source in self.sources]
        duplicates = sorted({source_id for source_id in ids if ids.count(source_id) > 1})
        if duplicates:
            raise ValueError(f"duplicate source ids: {', '.join(duplicates)}")
        return self

    @model_validator(mode="after")
    def _listen_addresses_are_unique(self) -> SystemConfig:
        addresses = [
            f"{source.listen_host}:{source.listen_port}"
            for source in self.sources
            if isinstance(source, HttpProxySourceConfig)
        ]
        duplicates = sorted({address for address in addresses if addresses.count(address) > 1})
        if duplicates:
            raise ValueError(f"duplicate listen addresses: {', '.join(duplicates)}")
        return self

    def source_ids(self) -> frozenset[str]:
        return frozenset(source.id for source in self.sources)


def load_system_config(path: Path) -> SystemConfig:
    raw: dict[str, Any] = yaml.safe_load(path.read_text()) or {}
    config = SystemConfig.model_validate(raw)
    if config.system != path.stem:
        raise ValueError(f"{path.name}: system {config.system!r} must match the file name")
    return config


def load_system_configs(directory: Path) -> dict[str, SystemConfig]:
    """Every `<system>.yml` in `directory`; files starting with `_` are skipped."""
    if not directory.is_dir():
        return {}
    configs = [
        load_system_config(path)
        for path in sorted(directory.glob("*.yml"))
        if not path.name.startswith("_")
    ]
    return {config.system: config for config in configs}
