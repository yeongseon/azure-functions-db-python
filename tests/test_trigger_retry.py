import pytest

from azure_functions_db.trigger.retry import RetryPolicy


class TestRetryPolicy:
    def test_defaults(self) -> None:
        policy = RetryPolicy()
        assert policy.max_retries == 3
        assert policy.base_delay_seconds == 1.0
        assert policy.max_delay_seconds == 60.0
        assert policy.exponential_base == 2.0

    def test_delay_for_attempt_zero(self) -> None:
        policy = RetryPolicy()
        assert policy.delay_for_attempt(0) == 1.0

    def test_delay_exponential_growth(self) -> None:
        policy = RetryPolicy(base_delay_seconds=1.0, exponential_base=2.0)
        assert policy.delay_for_attempt(0) == 1.0
        assert policy.delay_for_attempt(1) == 2.0
        assert policy.delay_for_attempt(2) == 4.0
        assert policy.delay_for_attempt(3) == 8.0

    def test_delay_capped_at_max(self) -> None:
        policy = RetryPolicy(base_delay_seconds=1.0, max_delay_seconds=5.0, exponential_base=2.0)
        assert policy.delay_for_attempt(10) == 5.0

    def test_custom_values(self) -> None:
        policy = RetryPolicy(
            max_retries=5,
            base_delay_seconds=0.5,
            max_delay_seconds=30.0,
            exponential_base=3.0,
        )
        assert policy.max_retries == 5
        assert policy.delay_for_attempt(0) == 0.5
        assert policy.delay_for_attempt(1) == 1.5

    def test_negative_max_retries_raises(self) -> None:
        with pytest.raises(ValueError, match="max_retries must be non-negative"):
            RetryPolicy(max_retries=-1)

    def test_zero_base_delay_raises(self) -> None:
        with pytest.raises(ValueError, match="base_delay_seconds must be positive"):
            RetryPolicy(base_delay_seconds=0)

    def test_max_less_than_base_raises(self) -> None:
        with pytest.raises(ValueError, match="max_delay_seconds must be >= base_delay_seconds"):
            RetryPolicy(base_delay_seconds=10.0, max_delay_seconds=5.0)

    def test_exponential_base_less_than_one_raises(self) -> None:
        with pytest.raises(ValueError, match="exponential_base must be >= 1"):
            RetryPolicy(exponential_base=0.5)

    def test_frozen(self) -> None:
        policy = RetryPolicy()
        with pytest.raises(AttributeError):
            policy.max_retries = 10  # type: ignore[misc]
