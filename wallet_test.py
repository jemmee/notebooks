# pip install pytest
#
# pytest wallet_test.py

import pytest
from wallet import Wallet, InsufficientAmount

# 1. FIXTURES: Used to set up a baseline state for tests
@pytest.fixture
def empty_wallet():
    return Wallet()

@pytest.fixture
def wallet_with_20():
    return Wallet(20)

# 2. BASIC TESTS: Using the fixtures as arguments
def test_default_initial_amount(empty_wallet):
    assert empty_wallet.balance == 0

def test_setting_initial_amount(wallet_with_20):
    assert wallet_with_20.balance == 20

def test_wallet_add_cash(wallet_with_20):
    wallet_with_20.add_cash(80)
    assert wallet_with_20.balance == 100

# 3. EXCEPTION TESTING: Verifying that errors happen when they should
def test_wallet_spend_cash_raises_exception_on_insufficient_amount(empty_wallet):
    with pytest.raises(InsufficientAmount):
        empty_wallet.spend_cash(100)