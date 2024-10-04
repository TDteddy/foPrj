import multiprocessing as mp

class Parameters:
    def __init__(self):
        dict_params = dict()

        dict_params['BINANCE'] = dict()
        dict_params['UPBIT'] = dict()
        dict_params['COINONE'] = dict()
        dict_params['BITHUMB'] = dict()
        dict_params['KORBIT'] = dict()

        dict_params['BINANCE']['public_key'] = ''
        dict_params['BINANCE']['private_key'] = ''
        dict_params['BINANCE']['api_url'] = 'https://api.binance.com'
        dict_params['BINANCE']['wss_url'] = 'wss://stream.binance.com:9443/ws'
        dict_params['BINANCE']['channel'] = dict()
        dict_params['BINANCE']['channel']['Orderbook'] = 'depth@1000ms'
        dict_params['BINANCE']['fee_rate'] = dict()
        dict_params['BINANCE']['fee_rate']['maker'] = 0.00075
        dict_params['BINANCE']['fee_rate']['taker'] = 0.00075

        dict_params['UPBIT']['public_key'] = ''
        dict_params['UPBIT']['private_key'] = ''
        dict_params['UPBIT']['api_url'] = 'https://api.upbit.com'
        dict_params['UPBIT']['wss_url'] = 'wss://api.upbit.com/websocket/v1'
        dict_params['UPBIT']['channel'] = dict()
        dict_params['UPBIT']['channel']['Orderbook'] = 'orderbook'
        dict_params['UPBIT']['channel']['Trade'] = 'trade'
        dict_params['UPBIT']['fee_rate'] = dict()
        dict_params['UPBIT']['fee_rate']['maker'] = 0.0005
        dict_params['UPBIT']['fee_rate']['taker'] = 0.0005
        dict_params['UPBIT']['fee_rate']['BTC'] = 0.0025
        dict_params['UPBIT']['fee_rate']['USDT'] = 0.0025

        dict_params['COINONE']['public_key'] = ''
        dict_params['COINONE']['private_key'] = ''
        dict_params['COINONE']['api_url'] = 'https://api.coinone.co.kr'
        dict_params['COINONE']['wss_url'] = 'wss://stream.coinone.co.kr'
        dict_params['COINONE']['channel'] = dict()
        dict_params['COINONE']['channel']['Orderbook'] = 'ORDERBOOK'
        dict_params['COINONE']['channel']['Trade'] = 'TRADE'
        dict_params['COINONE']['fee_rate'] = dict()
        dict_params['COINONE']['fee_rate']['maker'] = 0.002
        dict_params['COINONE']['fee_rate']['taker'] = 0.002

        dict_params['BITHUMB']['public_key'] = ''
        dict_params['BITHUMB']['private_key'] = ''
        dict_params['BITHUMB']['api_url'] = 'https://api.bithumb.com/'
        dict_params['BITHUMB']['wss_url'] = 'wss://pubwss.bithumb.com/pub/ws'
        dict_params['BITHUMB']['channel'] = dict()
        dict_params['BITHUMB']['channel']['Orderbook'] = 'orderbooksnapshot'
        dict_params['BITHUMB']['channel']['Trade'] = 'transaction'
        dict_params['BITHUMB']['fee_rate'] = dict()
        dict_params['BITHUMB']['fee_rate']['maker'] = 0.0004
        dict_params['BITHUMB']['fee_rate']['taker'] = 0.0004
        dict_params['BITHUMB']['fee_rate']['BTC'] = 0.0004

        dict_params['KORBIT']['public_key'] = ''
        dict_params['KORBIT']['private_key'] = ''
        dict_params['KORBIT']['api_url'] = 'https://api.korbit.co.kr'
        dict_params['KORBIT']['wss_url'] = 'wss://ws2.korbit.co.kr/v1/user/push'
        dict_params['KORBIT']['channel'] = dict()
        dict_params['KORBIT']['channel']['Orderbook'] = 'orderbook'
        dict_params['KORBIT']['channel']['Trade'] = 'transaction'
        dict_params['KORBIT']['fee_rate'] = dict()
        dict_params['KORBIT']['fee_rate']['maker'] = 0.0005
        dict_params['KORBIT']['fee_rate']['taker'] = 0.0005

        self.dict_params = dict_params

        ############################################################################################################

        dict_queue = dict()
        dict_queue['WEBSOCKET'] = dict()
        dict_queue['DATA_COLLECTION'] = dict()
        dict_queue['DATA_COLLECTION']['ORDERBOOK'] = mp.Queue()
        dict_queue['DATA_COLLECTION']['ORDERBOOK_one'] = mp.Queue()


        dict_queue['STRATEGY'] = dict()
        dict_queue['STRATEGY']['ORDERBOOK'] = mp.Queue()
        dict_queue['STRATEGY']['Arb_FOR_Inter'] = mp.Queue()
        dict_queue['STRATEGY']['Arb_FOR_Inter_Trade'] = mp.Queue()

        dict_queue['STRATEGY']['NoArb_convert'] = mp.Queue()
        dict_queue['STRATEGY']['NoArb_KRW_equi'] = mp.Queue()
        dict_queue['STRATEGY']['NoArb_KRW_equi_Trade'] = mp.Queue()

        dict_queue['SIMULATION'] = dict()
        dict_queue['SIMULATION']['NoArb_convert'] = mp.Queue()
        dict_queue['SIMULATION']['NoArb_equi_KRW'] = mp.Queue()
        dict_queue['SIMULATION']['NoArb_equi_FOR'] = mp.Queue()


        dict_queue['TELEGRAM'] = dict()
        dict_queue['TELEGRAM']['Arb_FOR_Inter'] = mp.Queue()
        dict_queue['TELEGRAM']['NoArb_convert'] = mp.Queue()
        dict_queue['TELEGRAM']['NoArb_KRW_equi'] = mp.Queue()
        dict_queue['TELEGRAM']['NoArb_KRW_equi_Trade'] = mp.Queue()

        self. dict_queue = dict_queue

        ############################################################################################################