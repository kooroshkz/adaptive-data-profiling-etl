"""adaptive_profiler — AutoML anomaly detection and schema-driven data quality profiling.

Quick start
-----------
>>> from adaptive_profiler import Profiler
>>>
>>> profiler = Profiler.from_yaml("profiling_schema.yml")
>>>
>>> # Train per-column models (partition_key can be city, sensor, region, …)
>>> results = profiler.train(partition_key="amsterdam", df=historical_df)
>>>
>>> # Score new data: returns anomaly flags + rule violations
>>> predictions = profiler.score(partition_key="amsterdam", df=new_df)
>>>
>>> # Rule-based quality checks only (no ML)
>>> violations = profiler.check_quality(df=new_df)
"""

from .models import SUPPORTED_MODELS
from .profiler import Profiler
from .quality import QualityViolation, check_dataframe, quality_summary
from .schema import ColumnConfig, ColumnChecks, ModelStoreConfig, ProfilerConfig, TrainingConfig
from .store import ArtifactStore, LocalStore, S3Store, make_store
from .trainer import TrainingResult

__version__ = "0.1.0"
__all__ = [
    # Main entry point
    "Profiler",
    # Config / schema
    "ProfilerConfig",
    "ColumnConfig",
    "ColumnChecks",
    "ModelStoreConfig",
    "TrainingConfig",
    # Storage
    "ArtifactStore",
    "S3Store",
    "LocalStore",
    "make_store",
    # Training
    "TrainingResult",
    # Quality
    "QualityViolation",
    "check_dataframe",
    "quality_summary",
    # Models
    "SUPPORTED_MODELS",
]
