from .ccxt_base import CCXTExchange

class BitstampExchange(CCXTExchange):
    """Bitstamp exchange implementation."""

    def __init__(self, default_type: str = 'spot'):
        super().__init__("bitstamp", default_type=default_type)

