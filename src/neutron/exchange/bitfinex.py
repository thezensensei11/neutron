from .ccxt_base import CCXTExchange

class BitfinexExchange(CCXTExchange):
    """Bitfinex exchange implementation."""

    def __init__(self, default_type: str = 'spot'):
        # Bitfinex has strict rate limits (approx 10-90 req/min depending on endpoint)
        # Setting a conservative limit of 3000ms (20 req/min) to be safe
        config = {
            'rateLimit': 3000,
            'enableRateLimit': True,
        }
        super().__init__("bitfinex", default_type=default_type, config=config)
