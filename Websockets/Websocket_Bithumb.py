import uuid
import logging
import time
import typing
import websocket
import json

import numpy as np

import queue
import threading
import multiprocessing as mp
from Parameters.Func_Logger import LogStreamer
from Parameters.Global_Parameters import *
from Parameters.Signature_Generator import FUNC_UPBIT_Signature_Generator

logger = logging.getLogger()

class WebsocketApp_Bithumb:
    def __init__(self, wss_url: str, q: mp.Queue, channel: str, tickers: typing.List):
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

        while self.flag_runforever:
            try:
                self.ws.run_forever()
            except Exception as e:
                logger.error('[BITHUMB] error in run_forever() method: %s', e)
            time.sleep(2)

    def _on_open(self, ws):
        logger.info('[BITHUMB] Websocket Connection Opened / channel: %s', self.channel)
        self.subscribe_channel(self.channel, self.tickers)

    def _on_close(self, ws):
        logger.info('[BITHUMB] Websocket Connection Closed / channel: %s', self.channel)

    def _on_error(self, ws, msg: str):
        logger.info('[BITHUMB] Websocket Connection Error / channel: %s / %s', self.channel, msg)

    def _on_message(self, ws, msg: str):
        data = json.loads(msg)
        self.data = data

        self.q.put(data)

    def subscribe_channel(self, channel: str, tickers: typing.List):

        data = {
            "type": channel,
            "symbols": tickers
        }

        try:
            self.ws.send(json.dumps(data))
        except Exception as e:
            logger.error('Websocket error while subscribing to %s updates: %s', channel, e)
            return None

if __name__ == '__main__':
    logger_main = LogStreamer()

    params = Parameters()
    public_key = params.dict_params['BITHUMB']['public_key']
    private_key = params.dict_params['BITHUMB']['private_key']

    # wss_url = 'wss://pubwss.bithumb.com/pub/ws'
    # channel = 'orderbooksnapshot'
    # ls_tickers = ['BTC_KRW', 'ETH_KRW', 'XRP_KRW']
    # q = queue.Queue(maxsize=200)
    #
    # websocket_bithumb = WebsocketApp_Bithumb(wss_url=wss_url, q=q, channel=channel, tickers=ls_tickers)
    #
    # t = threading.Thread(target=websocket_bithumb.start_ws)
    # t.start()

    # data = q.get()
    # orderbook = data['orderbook_units']
    #
    # dict_orderbook_all = dict()
    #
    # for flag in ['ask', 'bid']:
    #     if flag == 'ask':
    #         dict_orderbook_all['ask_price'] = np.array([x[0] for x in data['content']['asks']]).astype(float)
    #         dict_orderbook_all['ask_size'] = np.array([x[1] for x in data['content']['asks']]).astype(float)
    #
    #     elif flag == 'bid':
    #         dict_orderbook_all['bid_price'] = np.array([x[0] for x in data['content']['bids']]).astype(float)
    #         dict_orderbook_all['bid_size'] = np.array([x[1] for x in data['content']['bids']]).astype(float)
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

    wss_url = 'wss://pubwss.bithumb.com/pub/ws'
    channel = 'transaction'
    ls_tickers = ['BTC_KRW', 'ETH_KRW', 'XRP_KRW']
    q = queue.Queue(maxsize=200)

    websocket_bithumb = WebsocketApp_Bithumb(wss_url=wss_url, q=q, channel=channel, tickers=ls_tickers)

    t = threading.Thread(target=websocket_bithumb.start_ws)
    t.start()