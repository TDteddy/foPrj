import uuid
import logging
import time
import typing

import numpy as np
import websocket
import json
import queue
import threading
import multiprocessing as mp
from Parameters.Func_Logger import LogStreamer
from Parameters.Global_Parameters import *
from Parameters.Signature_Generator import FUNC_UPBIT_Signature_Generator

logger = logging.getLogger()

class WebsocketApp_Upbit:
    def __init__(self, wss_url: str, q: mp.Queue, header, channel: str, tickers: typing.List):
        self.wss_url = wss_url
        self.q = q
        self.header = header
        self.channel = channel
        self.tickers = tickers

        self.param_orderbook = 15

        self.flag_runforever = True

    def start_ws(self):
        self.ws = websocket.WebSocketApp(
            self.wss_url,
            header=self.header,
            on_open=self._on_open,
            on_close=self._on_close,
            on_error=self._on_error,
            on_message=self._on_message)

        while self.flag_runforever:
            try:
                self.ws.run_forever()
            except Exception as e:
                logger.error('[UPBIT] error in run_forever() method: %s', e)
            time.sleep(2)

    def _on_open(self, ws):
        logger.info('[UPBIT] Websocket Connection Opened / channel: %s', self.channel)
        self.subscribe_channel(self.channel, self.tickers)

    def _on_close(self, ws):
        logger.info('[UPBIT] Websocket Connection Closed / channel: %s', self.channel)

    def _on_error(self, ws, msg: str):
        logger.info('[UPBIT] Websocket Connection Error / channel: %s / %s', self.channel, msg)

    def _on_message(self, ws, msg: str):
        data = json.loads(msg)

        self.q.put(data)

    def subscribe_channel(self, channel: str, tickers: typing.List):

        if channel == 'orderbook':
            tickers = [x + '.' + str(self.param_orderbook) for x in tickers]

        data = [{
            "ticket": str(uuid.uuid4())
        }, {
            "type": channel,
            "codes": tickers,
            "isOnlyRealtime": True
        }]

        if channel == 'myTrade':
            data = [{
                "ticket": str(uuid.uuid4())
            }, {
                "type": channel
            }]

        try:
            self.ws.send(json.dumps(data))
        except Exception as e:
            logger.error('Websocket error while subscribing to %s updates: %s', channel, e)
            return None

if __name__ == '__main__':
    logger_main = LogStreamer()
    params = Parameters()
    public_key = params.dict_params['UPBIT']['public_key']
    private_key = params.dict_params['UPBIT']['private_key']

    header = FUNC_UPBIT_Signature_Generator(public_key=public_key,
                                            private_key=private_key,
                                            category='websocket', params=None)

    # wss_url = 'wss://api.upbit.com/websocket/v1'
    # channel = 'orderbook'
    # ls_tickers = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP']
    # q = queue.Queue(maxsize=200)
    #
    # websocket_upbit = WebsocketApp_Upbit(wss_url=wss_url, q=q, header=header, channel=channel, tickers=ls_tickers)
    #
    # t = threading.Thread(target=websocket_upbit.start_ws)
    # t.start()

    # while True:
    #     data = q.get()
    #     print(q.qsize())
    # orderbook = data['orderbook_units']
    #
    # dict_orderbook_all = dict()
    #
    # for flag in ['ask', 'bid']:
    #     if flag == 'ask':
    #         dict_orderbook_all['ask_price'] = np.array([x['ask_price'] for x in orderbook])
    #         dict_orderbook_all['ask_size'] = np.array([x['ask_size'] for x in orderbook])
    #
    #     elif flag == 'bid':
    #         dict_orderbook_all['bid_price'] = np.array([x['bid_price'] for x in orderbook])
    #         dict_orderbook_all['bid_size'] = np.array([x['bid_size'] for x in orderbook])
    #
    # threshold = 10000000
    #
    # for flag in ['ask', 'bid']:
    #     if flag == 'ask':
    #         ls_price = dict_orderbook_all['ask_price']
    #         ls_size = dict_orderbook_all['ask_size']
    #
    #     elif flag == 'bid':
    #         ls_price = dict_orderbook_all['bid_price']
    #         ls_size = dict_orderbook_all['bid_size']
    #
    #     ls_amt = ls_price * ls_size
    #     ls_amt_cum = np.cumsum(ls_amt)
    #
    #     i_ask = np.argmax(ls_amt_cum >= threshold)
    #     ls_amt_adj = ls_amt.copy()
    #
    #     if i_ask == 0:
    #         if sum(ls_amt_cum >= threshold) == 0:
    #             price_vwap = np.nan
    #         else:
    #             price_vwap = ls_price[0]
    #
    #     else:
    #         ls_amt_adj[i_ask] = threshold - ls_amt_adj[i_ask - 1]
    #         ls_amt_adj[(i_ask + 1):] = 0
    #
    #         price_vwap = sum(ls_price * ls_amt_adj) / threshold

    wss_url = 'wss://api.upbit.com/websocket/v1'
    channel = 'trade'
    ls_tickers = ['KRW-BTC', 'KRW-ETH', 'KRW-XRP']
    q = queue.Queue(maxsize=200)

    websocket_upbit = WebsocketApp_Upbit(wss_url=wss_url, q=q, header=header, channel=channel, tickers=ls_tickers)

    t = threading.Thread(target=websocket_upbit.start_ws)
    t.start()