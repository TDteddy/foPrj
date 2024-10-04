import logging
import time
import typing
import websocket
import json
import queue
import threading
import multiprocessing as mp
from Parameters.Func_Logger import LogStreamer
from Parameters.Global_Parameters import *
from Parameters.Signature_Generator import FUNC_UPBIT_Signature_Generator

logger = logging.getLogger()

class WebsocketApp_Binance:
    def __init__(self, wss_url: str, q: mp.Queue, channel: str, tickers: typing.List):
        self.ws_id = 'ws_' + str(int(time.time()*1000))
        self.wss_url = wss_url
        self.q = q
        self.channel = channel
        self.tickers = tickers

        self.flag_runforever = True

    def start_ws(self):
        self.ws = websocket.WebSocketApp(
            self.wss_url,
            on_open=self._on_open,
            on_close=self._on_close,
            on_error=self._on_error,
            on_message=self._on_message)

        while self.flag_runforever == True:
            try:
                self.ws.run_forever()
            except Exception as e:
                logger.error('[BINANCE] Websocket Error in run_forever() method: %s', e)
            time.sleep(2)

    def _on_open(self, ws):
        logger.info('[BINANCE] Websocket Connection Opened / channel: %s', self.channel)
        self.subscribe_channel(self.channel, self.tickers)

        # if len(self.tickers) > 100:
        #     n = 100
        #     ls_chunk = [self.tickers[i:i + n] for i in range(0, len(self.tickers), n)]
        #
        #     for ls_one in ls_chunk:
        #         self.subscribe_channel(self.channel, ls_one)

    def _on_close(self, ws):
        logger.info('[BINANCE] Websocket Connection Closed / channel: %s', self.channel)

    def _on_error(self, ws, msg: str):
        logger.info('[BINANCE] Websocket Connection Error / channel: %s / %s', self.channel, msg)

    def _on_message(self, ws, msg: str):
        data = json.loads(msg)

        self.q.put(data)

    def subscribe_channel(self, channel: str, tickers: typing.List):
        data = dict()
        data['method'] = 'SUBSCRIBE'

        data['params'] = []

        for ticker in tickers:
            data['params'].append(ticker.lower() + '@' + channel)

        data['id'] = self.ws_id

        try:
            self.ws.send(json.dumps(data))
        except Exception as e:
            logger.error('[BINANCE] Websocket error while subscribing to %s updates: %s', channel, e)

if __name__ == '__main__':
    logger_main = LogStreamer()

    param_keys = Parameters().dict_params
    public_key = param_keys['BINANCE']['public_key']
    private_key = param_keys['BINANCE']['private_key']

    ############################################################################################################

    # wss_url = 'wss://stream.binance.com:9443/ws'
    # channel = 'bookTicker'
    # ls_tickers = ['BTCUSDT', 'ETHUSDT', 'XRPUSDT']
    # q = queue.Queue(maxsize=200)
    #
    # websocket_binance = WebsocketApp_Binance(ws_id=1, wss_url=wss_url, q=q, channel=channel, tickers=ls_tickers)
    #
    # t = threading.Thread(target=websocket_binance.start_ws)
    # t.start()

    ############################################################################################################
    from Clients.Client_Binance import *
    params = Parameters()

    client_binance = ClientBinance(params=params)

    ticker = 'XRPUSDT'
    limit = 100

    dict_orderbook_one = client_binance.get_orderbook_snapshot(ticker, limit)

    ############################################################################################################

    wss_url = 'wss://stream.binance.com:9443/ws'
    channel = 'depth@1000ms'
    ls_tickers = ['XRPUSDT']
    q = queue.Queue(maxsize=200)

    websocket_binance = WebsocketApp_Binance(wss_url=wss_url, q=q, channel=channel, tickers=ls_tickers)

    t = threading.Thread(target=websocket_binance.start_ws)
    t.start()

    ############################################################################################################

    while True:
        try:
            data = q.get()

            for flag in ['ask', 'bid']:
                if flag == 'ask':
                    ls_temp = data['a']

                    if len(ls_temp) > 0:
                        for ls_temp_one in ls_temp:
                            price_one = float(ls_temp_one[0])
                            size_one = float(ls_temp_one[1])

                            if price_one in dict_orderbook_one['ask_price']:
                                idx = np.where(dict_orderbook_one['ask_price'] == price_one)[0][0]
                                dict_orderbook_one['ask_size'][idx] = size_one

                            else:
                                dict_orderbook_one['ask_price'] = \
                                    np.append(dict_orderbook_one['ask_price'], price_one)
                                dict_orderbook_one['ask_size'] = \
                                    np.append(dict_orderbook_one['ask_size'], size_one)

                            sorted_indices = np.argsort(dict_orderbook_one['ask_price'])
                            dict_orderbook_one['ask_price'] = dict_orderbook_one['ask_price'][sorted_indices]
                            dict_orderbook_one['ask_size'] = dict_orderbook_one['ask_size'][sorted_indices]

                            zero_indices = np.where(dict_orderbook_one['ask_size'] == 0)[0]
                            dict_orderbook_one['ask_price'] = np.delete(dict_orderbook_one['ask_price'], zero_indices)
                            dict_orderbook_one['ask_size'] = np.delete(dict_orderbook_one['ask_size'], zero_indices)

                elif flag == 'bid':
                    ls_temp = data['b']

                    if len(ls_temp) > 0:
                        for ls_temp_one in ls_temp:
                            price_one = float(ls_temp_one[0])
                            size_one = float(ls_temp_one[1])

                            if price_one in dict_orderbook_one['bid_price']:
                                idx = np.where(dict_orderbook_one['bid_price'] == price_one)[0][0]
                                dict_orderbook_one['bid_size'][idx] = size_one

                            else:
                                dict_orderbook_one['bid_price'] = \
                                    np.append(dict_orderbook_one['bid_price'], price_one)
                                dict_orderbook_one['bid_size'] = \
                                    np.append(dict_orderbook_one['bid_size'], size_one)

                            sorted_indices = np.argsort(dict_orderbook_one['bid_price'])[::-1]
                            dict_orderbook_one['bid_price'] = dict_orderbook_one['bid_price'][sorted_indices]
                            dict_orderbook_one['bid_size'] = dict_orderbook_one['bid_size'][sorted_indices]

                            zero_indices = np.where(dict_orderbook_one['bid_size'] == 0)[0]
                            dict_orderbook_one['bid_price'] = np.delete(dict_orderbook_one['bid_price'], zero_indices)
                            dict_orderbook_one['bid_size'] = np.delete(dict_orderbook_one['bid_size'], zero_indices)

                price_a_indices = dict_orderbook_one['ask_price'] > dict_orderbook_one['bid_price'][0]
                dict_orderbook_one['ask_price'] = dict_orderbook_one['ask_price'][price_a_indices]
                dict_orderbook_one['ask_size'] = dict_orderbook_one['ask_size'][price_a_indices]

                dict_orderbook_one['ask_price'] = dict_orderbook_one['ask_price'][:100]
                dict_orderbook_one['ask_size'] = dict_orderbook_one['ask_size'][:100]

                price_b_indices = dict_orderbook_one['bid_price'] < dict_orderbook_one['ask_price'][0]
                dict_orderbook_one['bid_price'] = dict_orderbook_one['bid_price'][price_b_indices]
                dict_orderbook_one['bid_size'] = dict_orderbook_one['bid_size'][price_b_indices]

                dict_orderbook_one['bid_price'] = dict_orderbook_one['bid_price'][:100]
                dict_orderbook_one['bid_size'] = dict_orderbook_one['bid_size'][:100]


            print(dict_orderbook_one['ask_price'][0], dict_orderbook_one['ask_size'][0])
            print(dict_orderbook_one['bid_price'][0], dict_orderbook_one['bid_size'][0])
            print('')


        except Exception as e:

            print(e)