import pathlib
import sys
import tempfile
from shutil import copytree

import pytest
from airflow.models import DagBag

DAG_DIRS = ["datamigration", "translation"]


@pytest.fixture(scope="package")
def dag_bag() -> str:
    """Copies contents of dags/ folders to a temporary directory"""
    temp_dir = tempfile.mkdtemp()
    for d in DAG_DIRS:
        copytree(
            pathlib.Path(__file__).parent.parent / d / "dags",
            f"{temp_dir}/",
            dirs_exist_ok=True,
        )
    copytree(
        pathlib.Path(__file__).parent.parent / "common_utils",
        f"{temp_dir}/common_utils/",
        dirs_exist_ok=True,
    )
    sys.path.insert(0, temp_dir)
    yield DagBag(dag_folder=temp_dir, include_examples=False)


def test_no_import_errors(dag_bag):
    assert len(dag_bag.import_errors) == 0, "No Import Failures"


def test_dag_id_equals_file_name(dag_bag):
    for dag in dag_bag.dags.values():
        assert dag.dag_id == pathlib.Path(dag.relative_fileloc).stem
