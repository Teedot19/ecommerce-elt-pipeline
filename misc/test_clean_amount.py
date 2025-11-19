from my_module import clean_amount
import pytest
def test_clean_amount_with_valid_number():
    assert clean_amount('10.5') == 10.5


def test_clean_amount_with_none():
    assert clean_amount(None) is None


def test_clean_amount_raises_for_invalid_input():
     with pytest.raises(ValueError):
        clean_amount("abc")
