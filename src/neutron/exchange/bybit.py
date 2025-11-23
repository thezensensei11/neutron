from .ccxt_base import CCXTExchange

class BybitExchange(CCXTExchange):
    """Bybit exchange implementation."""

    def __init__(self, default_type: str = 'spot'):
        # Bybit Unified account handles spot/swap/future
        # Use bytick.com to avoid geo-blocking if needed
        config = {
            'hostname': 'bytick.com',
        }
        super().__init__("bybit", default_type=default_type, config=config, supports_fetch_since_0=False)
