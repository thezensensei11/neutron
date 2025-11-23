from .ccxt_base import CCXTExchange

class BitmexExchange(CCXTExchange):
    """BitMEX exchange implementation."""

    def __init__(self, default_type: str = 'swap'):
        # BitMEX is primarily swap/future, but has some spot
        super().__init__("bitmex", default_type=default_type)
