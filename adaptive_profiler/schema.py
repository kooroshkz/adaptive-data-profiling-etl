"""Typed configuration objects parsed from the user's profiling_schema.yml."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class ColumnChecks:
    """Rule-based quality constraints for a single column."""

    type: str | None = None           # expected Python type: "float" | "int" | "str"
    range: tuple[float, float] | None = None  # [min, max] inclusive
    not_null: bool = False

    def violations(self, value: Any) -> list[str]:
        """Return a list of rule violation codes for *value* (empty = clean)."""
        issues: list[str] = []

        is_null = value is None or (isinstance(value, float) and math.isnan(value))
        if self.not_null and is_null:
            return ["null_value"]       # downstream checks are meaningless on null

        if is_null:
            return []

        if self.type in ("float", "int"):
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                issues.append(f"type_error(expected={self.type})")
                return issues           # range check needs a numeric value

            if self.range is not None:
                lo, hi = self.range
                if numeric < lo or numeric > hi:
                    issues.append(f"out_of_range([{lo}, {hi}])")

        return issues

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ColumnChecks":
        r = d.get("range")
        return cls(
            type=d.get("type"),
            range=(float(r[0]), float(r[1])) if r else None,
            not_null=bool(d.get("not_null", False)),
        )


@dataclass(frozen=True)
class ColumnConfig:
    """Schema definition for a single column."""

    name: str
    description: str = ""
    automl: bool = False
    checks: ColumnChecks = field(default_factory=ColumnChecks)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ColumnConfig":
        return cls(
            name=d["name"],
            description=d.get("description", ""),
            automl=bool(d.get("automl", False)),
            checks=ColumnChecks.from_dict(d.get("checks", {})),
        )


@dataclass(frozen=True)
class ModelStoreConfig:
    """Where trained artifacts are persisted."""

    backend: str = "local"             # "s3" | "local"
    bucket: str = ""                   # required for s3
    prefix: str = "models/v1"         # S3 key prefix or ignored for local
    local_dir: str = "./models"        # used when backend=local

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "ModelStoreConfig":
        return cls(
            backend=d.get("backend", "local"),
            bucket=d.get("bucket", ""),
            prefix=d.get("prefix", "models/v1"),
            local_dir=d.get("local_dir", "./models"),
        )


@dataclass(frozen=True)
class TrainingConfig:
    """Optuna hyperparameter search settings."""

    n_trials: int = 30
    seed: int = 42
    min_train_rows: int = 100

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "TrainingConfig":
        return cls(
            n_trials=int(d.get("n_trials", 30)),
            seed=int(d.get("seed", 42)),
            min_train_rows=int(d.get("min_train_rows", 100)),
        )


@dataclass(frozen=True)
class ProfilerConfig:
    """Top-level profiling configuration loaded from YAML."""

    columns: tuple[ColumnConfig, ...]
    model_store: ModelStoreConfig
    training: TrainingConfig

    @property
    def automl_columns(self) -> list[ColumnConfig]:
        return [c for c in self.columns if c.automl]

    @property
    def automl_column_names(self) -> list[str]:
        return [c.name for c in self.automl_columns]

    @classmethod
    def from_yaml(cls, path: str | Path) -> "ProfilerConfig":
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        version = raw.get("version", 1)
        if version != 1:
            raise ValueError(f"Unsupported schema version: {version}. Expected 1.")

        return cls(
            columns=tuple(ColumnConfig.from_dict(c) for c in raw.get("columns", [])),
            model_store=ModelStoreConfig.from_dict(raw.get("model_store", {})),
            training=TrainingConfig.from_dict(raw.get("training", {})),
        )

    def __repr__(self) -> str:
        automl = self.automl_column_names
        return (
            f"ProfilerConfig(columns={len(self.columns)}, automl={automl}, "
            f"store={self.model_store.backend!r}, prefix={self.model_store.prefix!r})"
        )
