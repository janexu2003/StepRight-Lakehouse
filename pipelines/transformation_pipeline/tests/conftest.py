#the name must be conftest

"""
Shared pytest fixtures for unit tests — transformation_pipeline.
 
local_spark: a plain, local SparkSession — no Databricks cluster, no catalog,
no network. This is the concrete, mechanical reason unit tests are fast: a
local Spark session starts in seconds, a real cluster does not. Session-scoped
— built once per test run, reused across every test function, not recreated
per test.
 
sys.path setup: makes transformations/*_logic.py importable directly by name
(e.g. `from gold_revenue_logic import compute_daily_revenue`), matching how
the real Lakeflow pipeline resolves sibling-file imports within its own
transformations folder — same import style in tests as in the real pipeline.
"""

import os
import sys
 
import pytest
from pyspark.sql import SparkSession

_TRANSFORMATIONS_DIR = os.path.join(os.path.dirname(__file__), "..","transformations")
sys.path.insert(0, os.path.abspath(_TRANSFORMATIONS_DIR))
#transformations directory is included in current path
#and we create a fixture for providing sparkSession

#unit test case needs sparkSession
#create one sparkSession and reuse it in all unit test sessions

#unit test can both be run outside the pipeline via CI/CD locally(create SparkSession on your own)
#or via Databricks(Databricks can provide active sparkSession)

@pytest.fixture(scope="session")
def local_spark():
    """
    Environment-aware, on purpose — found via a real Databricks Serverless testing session, not assumed.
 
    Databricks Serverless compute already has its own active SparkSession governing that compute, and won't let you create a second, independent one with .master(...) — it errors out. A bare CI runner (e.g. GitHub Actions, no Databricks at all) has the OPPOSITE problem: no ambient session exists, so .master("local[2]") must be set explicitly, or getOrCreate() fails with "a master URL must be set."
 
    getActiveSession() (stable since Spark 3.0) resolves both: reuse
    whatever's already active (Databricks, serverless or classic), or fall back to a genuinely local session (bare CI runner) if nothing is active yet. Same fixture, same test files, both environments — without this, fixing one environment silently breaks the other.
    """
    active = SparkSession.getActiveSession()
    if active is not None:
        yield active
        # Don't stop() a session we didn't create — Databricks owns its
        # own ambient session's lifecycle, not this fixture.
        return
    
    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("stepright-unit-tests")
        .config("spark.sql.shuffle.partitions", "2")  # small fixtures, no need for 200 default partitions
        .getOrCreate()
    )
    yield spark
    spark.stop()















sys.path.insert(0, os.path.abspath(_TRANSFORMATIONS_DIR))