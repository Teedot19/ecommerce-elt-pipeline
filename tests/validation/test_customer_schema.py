# from datetime import date
# from batch_data_pipeline.validation.schema.customer import Customer
# import pytest
# from pydantic import ValidationError

# def test_customer_valid():
#     obj = Customer(
#         customer_id="123",
#         first_name="John",
#         last_name="Doe",
#         email="john@example.com",
#         country="US",
#         signup_date="2024-01-01",
#     )

#     assert obj.customer_id == "123"
#     assert obj.first_name == "John"
#     assert obj.signup_date == date(2024, 1, 1)


# def test_customer_invalid_email():
#     with pytest.raises(ValidationError):
#         Customer(
#             customer_id="1",
#             first_name="John",
#             last_name="Doe",
#             email="bad-email",
#             country="US",
#             signup_date="2024-01-01",
#         )
import pytest
from pydantic import ValidationError

from batch_data_pipeline.validation.schema.customer import Customer
from batch_data_pipeline.validation.schema.product import Product
from batch_data_pipeline.validation.schema.order import Order
from batch_data_pipeline.validation.schema.order_item import OrderItem
from batch_data_pipeline.validation.schema.payment import Payment


@pytest.mark.parametrize(
    "Schema, valid_sample, invalid_sample",
    [
        (
            Customer,
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
                "first_name": "A",
                "last_name": "B",
                "email": "bad-email",
                "country": "US",
                "signup_date": "2024-01-01",
            },
        ),
        (
            Product,
            {
                "product_id": "1",
                "name": "Laptop",
                "category": "Electronics",
                "price": 1500.50,
                "stock_count": 10,
                "created_at": "2024-01-01T00:00:00",
                "is_active": True,
            },
            {
                "product_id": "2",
                "name": "",
                "category": "E",
                "price": "FREE",  # invalid
                "stock_count": "??",
                "created_at": "20-01-01T00:00:00",
                "is_active": 'maybe',
            },
        ),
        (
            Order,
            {
                "order_id": "1",
                "customer_id": "123",
                "order_date": "2024-01-01T00:00:00",
                "status": "new",
                "total_amount": 100.0,
                "shipping_cost": 10.0,
                "shipping_country": "US",
                "campaign": "Flash Friday",
            },
            {
                "order_id": "1",
                "customer_id": "123",
                "order_date": "2024-01-01T00:00:00",
                "status": "new",
                "total_amount": -50.0,  # invalid
                "shipping_cost": 10.0,
                "shipping_country": "US",
                "campaign": "Flash Friday",
            },
        ),
        (
            OrderItem,
            {
                "order_item_id": "1",
                "order_id": "123",
                "product_id": "456",
                "quantity": 2,
                "unit_price": 10.0,
                "line_total": 20.0,
            },
            {
                "order_item_id": "1",
                "order_id": "123",
                "product_id": "456",
                "quantity": 0,  # invalid
                "unit_price": 10.0,
                "line_total": 20.0,
            },
        ),
        (
            Payment,
            {
                "payment_id": "1",
                "order_id": "10",
                "amount": 100.0,
                "payment_method": "card",
                "paid_at": "2024-01-01T00:00:00",
            },
            {
                "payment_id": "1",
                "order_id": "10",
                "amount": "free",  # invalid
                "payment_method": "card",
                "paid_at": "2024-01-01T00:00:00",
            },
        ),
    ],
)
def test_schema_validation(Schema, valid_sample, invalid_sample):
    # valid sample should pass
    obj = Schema(**valid_sample)
    assert obj is not None

    # invalid sample should raise
    with pytest.raises(ValidationError):
        Schema(**invalid_sample)
