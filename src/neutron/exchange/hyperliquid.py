from .ccxt_base import CCXTExchange

class HyperliquidExchange(CCXTExchange):
    """Hyperliquid exchange implementation."""

    def __init__(self, default_type: str = 'swap'):
        # Hyperliquid is primarily perp
        super().__init__("hyperliquid", default_type=default_type, supports_fetch_since_0=False)
