from decimal import Decimal

from src.streaming.decoder import decode_amount


def test_decode_positive_amount():
    assert decode_amount("BCkG") == Decimal("2726.46")


def test_decode_zero_amount():
    assert decode_amount("AAA=") == Decimal("0.00")


def test_decode_negative_amount():
    assert decode_amount("/w==") == Decimal("-0.01")
