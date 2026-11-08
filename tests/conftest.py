"""Shared fixtures: one local SparkSession and the Gold/Silver rule files."""

import os
from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
# Spark's Python workers must use this interpreter (on Windows the default "python" may be another one)
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

from pyspark.sql import SparkSession  # noqa: E402

from src.gold.silver_union import load_gold_rules  # noqa: E402
from src.silver.rules import load_rules  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("gold-tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def rules():
    return load_gold_rules()


@pytest.fixture(scope="session")
def silver_rules():
    return load_rules()
