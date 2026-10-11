import importlib.util
import pathlib

import pytest

RULES_FILE = pathlib.Path(__file__).resolve(
).parents[1] / "notebooks" / "00_rules.py"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Local Spark with ANSI mode ON, the same strictness as Databricks serverless."""
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (SparkSession.builder.master("local[2]")
               .appName("airline-lakehouse-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .config("spark.sql.ansi.enabled", "true")
               .config("spark.ui.enabled", "false")
               .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
               .getOrCreate())
    yield session
    session.stop()


@pytest.fixture(scope="session")
def rules():
    """Load notebooks/00_rules.py, the exact file the pipeline notebooks run with %run."""
    spec = importlib.util.spec_from_file_location("rules", RULES_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
