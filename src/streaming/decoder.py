import base64
from decimal import Decimal


def decode_amount(encoded_amount: str, scale: int = 2) -> Decimal:
    """Decode a Debezium Base64-encoded decimal into a precise Decimal."""
    amount_bytes = base64.b64decode(encoded_amount, validate=True)
    unscaled_amount = int.from_bytes(
        amount_bytes,
        byteorder="big",
        signed=True,
    )
    return Decimal(unscaled_amount).scaleb(-scale)
