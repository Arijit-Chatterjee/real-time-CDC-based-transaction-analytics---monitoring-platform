from decimal import Decimal

import pytest

from src.streaming.decoder import decode_amount


def test_decode_positive_amount():
    assert decode_amount("BCkG") == Decimal("2726.46")


def test_decode_zero_amount():
    assert decode_amount("AAA=") == Decimal("0.00")


def test_decode_negative_amount():
    assert decode_amount("/w==") == Decimal("-0.01")


def test_decode_invalid_base64_raises_error():
    with pytest.raises(ValueError):
        decode_amount("not-valid-base64!")


def test_decode_amount_with_custom_scale():
    assert decode_amount("BCkG", scale=3) == Decimal("272.646")
