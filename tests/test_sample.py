"""Simple Test for CI Pipeline"""
import pytest


def add(x:int,y:int)-> int:
    return x + y


@pytest.mark.unit
def test_sample():
    assert add(2,3) == 5
