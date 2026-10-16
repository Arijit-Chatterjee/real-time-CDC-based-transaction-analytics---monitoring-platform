"""Generate synthetic retail data for StreamGuard."""

import random


def generate_customers(count: int = 5) -> list[dict]:
    """Create fake customers for testing."""
    regions = ["Kolkata", "Mumbai", "Delhi", "Chennai"]

    customers = []

    for customer_id in range(1, count + 1):
        customer = {
            "customer_id": customer_id,
            "home_region": random.choice(regions),
            "account_status": "active",
        }

        customers.append(customer)

    return customers


def generate_merchants(count: int = 3) -> list[dict]:
    """Create fake merchants for testing."""
    merchant_names = [
        ("Fresh Mart", "grocery"),
        ("Tech World", "electronics"),
        ("Style Hub", "clothing"),
        ("Daily Needs", "convenience"),
        ("Book Corner", "books"),
    ]

    regions = ["Kolkata", "Mumbai", "Delhi", "Chennai"]
    merchants = []

    for merchant_id in range(1, count + 1):
        name, category = merchant_names[merchant_id - 1]

        merchant = {
            "merchant_id": merchant_id,
            "merchant_name": name,
            "merchant_category": category,
            "merchant_region": random.choice(regions),
        }

        merchants.append(merchant)

    return merchants


def generate_transactions(
    customers: list[dict],
    merchants: list[dict],
    count: int = 10,
) -> list[dict]:
    """Create fake retail transactions."""
    transactions = []

    for transaction_id in range(1, count + 1):
        customer = random.choice(customers)
        merchant = random.choice(merchants)

        transaction = {
            "transaction_id": transaction_id,
            "customer_id": customer["customer_id"],
            "merchant_id": merchant["merchant_id"],
            "amount": round(random.uniform(100, 5000), 2),
            "currency": "INR",
            "region": customer["home_region"],
            "status": random.choice(["approved", "declined"]),
        }

        transactions.append(transaction)

    return transactions


def main() -> None:
    customers = generate_customers()
    merchants = generate_merchants()
    transactions = generate_transactions(customers, merchants)

    print("CUSTOMERS")
    for customer in customers:
        print(customer)

    print("\nMERCHANTS")
    for merchant in merchants:
        print(merchant)

    print("\nTRANSACTIONS")
    for transaction in transactions:
        print(transaction)


if __name__ == "__main__":
    main()
