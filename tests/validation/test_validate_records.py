# from batch_data_pipeline.validation.helpers import validate_records
# from batch_data_pipeline.validation.schema.customer import Customer


# def test_validate_records_mixed():
#     raw = [
#         {"customer_id": "1", "first_name": "John", "last_name": "Doe",
#          "email": "john@example.com", "country": "US", "signup_date": "2024-01-01"},
#         {"customer_id": "2", "first_name": "A", "last_name": "X",
#          "email": "bad-email", "country": "US", "signup_date": "2024-01-01"},
#     ]

#     valid, invalid = validate_records(raw, Customer)

#     assert len(valid) == 1
#     assert len(invalid) == 1
#     assert invalid[0]["row_index"] == 1



import pytest
from batch_data_pipeline.validation.helpers import validate_records

from batch_data_pipeline.validation.schema.customer import Customer
from batch_data_pipeline.validation.schema.product import Product
from batch_data_pipeline.validation.schema.order import Order
from batch_data_pipeline.validation.schema.order_item import OrderItem
from batch_data_pipeline.validation.schema.payment import Payment


@pytest.mark.parametrize(
    "Schema, rows",
    [
        # -------------------------------------------------
        # CUSTOMER
        # -------------------------------------------------
        (
            Customer,
            [
                {
                    "customer_id": "1",
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "john@example.com",
                    "country": "US",
                    "signup_date": "2024-01-01",
                },
                {
                    "customer_id": "2",
                    "first_name": "A",        # invalid
                    "last_name": "B",         # invalid
                    "email": "bad-email",     # invalid
                    "country": "US",
                    "signup_date": "2024-01-01",
                },
            ],
        ),

        # -------------------------------------------------
        # PRODUCT
        # -------------------------------------------------
        (
            Product,
            [
                {
                    "product_id": "1",
                    "name": "Laptop",
                    "category": "Electronics",
                    "price": 999.99,
                    "stock_count": 10,
                    "created_at": "2024-01-01T00:00:00",
                    "is_active": True,
                },
                {
                    "product_id": "2",
                    "name": "",
                    "category": "E",
                    "price": "FREE",  # invalid
                    "stock_count": '??',
                    "created_at": "24-01-01T00:00:00",
                    "is_active": 'maybe',
                },
            ],
        ),

        # -------------------------------------------------
        # ORDER
        # -------------------------------------------------
        (
            Order,
            [
                {
                    "order_id": "1",
                    "customer_id": "123",
                    "order_date": "2024-01-01T00:00:00",
                    "status": "new",
                    "total_amount": 120.0,
                    "shipping_cost": 10.0,
                    "shipping_country": "US",
                    "campaign": "Flash Friday",
                },
                {
                    "order_id": "2",
                    "customer_id": "123",
                    "order_date": "20-01-01T00:00:00",
                    "status": "n",
                    "total_amount": -30.0,  # invalid
                    "shipping_cost": -10.0,
                    "shipping_country": "US",
                    "campaign": "Flash Fridayuyjjj",
                },
            ],
        ),

        # -------------------------------------------------
        # ORDER ITEM
        # -------------------------------------------------
        (
            OrderItem,
            [
                {
                    "order_item_id": "1",
                    "order_id": "10",
                    "product_id": "200",
                    "quantity": 2,
                    "unit_price": 50.0,
                    "line_total": 100.0,
                },
                {
                    "order_item_id": "2",
                    "order_id": "",
                    "product_id": "",
                    "quantity": 0,     # invalid
                    "unit_price": -50.0,
                    "line_total": -100.0,
                },
            ],
        ),

        # -------------------------------------------------
        # PAYMENT
        # -------------------------------------------------
        (
            Payment,
            [
                {
                    "payment_id": "1",
                    "order_id": "10",
                    "amount": 80.0,
                    "payment_method": "card",
                    "paid_at": "2024-01-01T00:00:00",
                },
                {
                    "payment_id": "2",
                    "order_id": "",
                    "amount": "FREE",        # invalid
                    "payment_method": "",
                    "paid_at": "2024-000:00:00",
                },
            ],
        ),
    ],
)
def test_validate_records_logic(Schema, rows):
    valid, invalid = validate_records(rows, Schema)

   # Should have exactly one valid and one invalid
    assert len(valid) == 1
    assert len(invalid) == 1

    bad = invalid[0]

    # invalid row should correspond to the 2nd row
    assert bad["row_index"] == 1

    # must contain original raw data
    assert "raw_data" in bad
    assert bad["raw_data"] == rows[1]

    # must contain a list of errors
    assert "errors" in bad
    assert isinstance(bad["errors"], list)
    assert len(bad["errors"]) >= 1
