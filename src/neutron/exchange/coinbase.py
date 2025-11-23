from .ccxt_base import CCXTExchange

class CoinbaseExchange(CCXTExchange):
    """Coinbase exchange implementation."""

    def __init__(self, default_type: str = 'spot'):
        super().__init__("coinbase", default_type=default_type, supports_fetch_since_0=False)
