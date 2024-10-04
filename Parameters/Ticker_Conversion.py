
def TickerConversion_BaseToQuote(exchange: str, quote: str, ticker_base: str):
    if exchange == 'BINANCE':
        ticker_quote = ticker_base.upper() + quote.upper()
    elif exchange == 'UPBIT':
        ticker_quote = quote.upper() + '-' + ticker_base.upper()
    elif exchange == 'COINONE':
        ticker_quote = ticker_base.upper()
    elif exchange == 'BITHUMB':
        ticker_quote = ticker_base.upper() + '_' + quote.upper()
    elif exchange == 'KORBIT':
        ticker_quote = ticker_base.lower() + '_' + quote.lower()

    return ticker_quote


def TickerConversion_QuoteToBase(exchange: str, quote: str, ticker_quote: str):
    if exchange == 'BINANCE':
        ticker_base = ticker_quote.upper().replace(quote.upper(), '', 1)
    elif exchange == 'UPBIT':
        ticker_base = ticker_quote.upper().replace(quote.upper() + '-', '')
    elif exchange == 'COINONE':
        ticker_base = ticker_quote.upper()
    elif exchange == 'BITHUMB':
        ticker_base = ticker_quote.upper().replace('_' + quote.upper(), '')
    elif exchange == 'KORBIT':
        ticker_base = ticker_quote.lower().replace('_' + quote.lower(), '').upper()

    return ticker_base