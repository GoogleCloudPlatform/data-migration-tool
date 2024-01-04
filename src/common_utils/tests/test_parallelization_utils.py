import pytest
from airflow.utils import timezone

# from src.common_utils import parallelization_utils
from common_utils import parallelization_utils


def test_make_run_batches_valid():
    id_prefix = timezone.utcnow()
    result = parallelization_utils.make_run_batches(["a", "b", "c"], 1, id_prefix)
    result_list = list(result)
    assert "['a']" in str(result_list[0])
    assert "['b']" in str(result_list[1])
    assert "['c']" in str(result_list[2])


def test_make_run_batches_empty_payload_list():
    id_prefix = timezone.utcnow()
    result = parallelization_utils.make_run_batches([], 1, id_prefix)
    result_list = list(result)
    expected_result = []
    assert expected_result == result_list


def test_make_run_batches_empty_id_prefix():
    result = parallelization_utils.make_run_batches(["a", "b", "c"], 1, "")
    result_list = list(result)
    assert "'-0'" in str(result_list[0])
    assert "'-1'" in str(result_list[1])
    assert "'-2'" in str(result_list[2])


def test_make_run_batches_zero_batch_size():
    id_prefix = timezone.utcnow()
    with pytest.raises(ValueError):
        assert list(
            parallelization_utils.make_run_batches(["a", "b", "c"], 0, id_prefix)
        )


def test_make_run_batches_negative_batch_size():
    id_prefix = timezone.utcnow()
    result = parallelization_utils.make_run_batches(["a", "b", "c"], -1, id_prefix)
    result_list = list(result)
    expected_result = []
    assert expected_result == result_list
