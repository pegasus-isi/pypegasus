import pytest


@pytest.mark.parametrize(
    "actual, expected",
    [(1, 1), (2, 2)],
)
def test_type_check_fail(actual, expected):
    assert actual == expected
