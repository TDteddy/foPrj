import threading
import multiprocessing as mp
import time
import typing
import queue
import pandas as pd
import numpy as np

from Parameters.Global_Parameters import Parameters
from Parameters.Func_Logger import LogStreamer
from Parameters.Signature_Generator import FUNC_UPBIT_Signature_Generator
from Parameters.Ticker_Conversion import *

from Websockets.Websocket_Binance import WebsocketApp_Binance
from Websockets.Websocket_Upbit import WebsocketApp_Upbit
from Websockets.Websocket_Bithumb import WebsocketApp_Bithumb
from Websockets.Websocket_Coinone import WebsocketApp_Coinone
from Websockets.Websocket_Korbit import WebsocketApp_Korbit
# from Websockets.Websocket_MultiProcess import *
from Clients.__Manager import Manager_Client

class Manager_Wesocket:
    def __init__(self, params: Parameters, manager_client: Manager_Client):
        self.params = params
        self.dict_params = self.params.dict_params
        self.dict_queue = self.params.dict_queue['WEBSOCKET']

        self.manager_client = manager_client

        self.ls_exchange = self.manager_client.ls_exchange
        self.ls_exchange_foreign = self.manager_client.ls_exchange_foreign
        self.ls_exchange_korea = self.manager_client.ls_exchange_korea

        self.dict_tickers = self.manager_client.dict_tickers['WEBSOCKET']

        ############################################################################################################
        self.Init_WebsocketFeed()
        ############################################################################################################

    def Init_WebsocketFeed(self):
        # self.dict_queue = dict()
        self.dict_ws = dict()
        self.dict_process_ws = dict()
        self.dict_thread_data = dict()

        for exchange in self.dict_tickers.keys():
            self.dict_queue[exchange] = dict()
            self.dict_ws[exchange] = dict()
            self.dict_process_ws[exchange] = dict()
            self.dict_thread_data[exchange] = dict()

            for quote in self.dict_tickers[exchange].keys():
                self.dict_queue[exchange][quote] = dict()
                self.dict_ws[exchange][quote] = dict()
                self.dict_process_ws[exchange][quote] = dict()
                self.dict_thread_data[exchange][quote] = dict()

                ls_tickers_one = self.dict_tickers[exchange][quote]
                ls_i = []

                n = 100
                if (exchange == 'BINANCE') & (len(ls_tickers_one) > n):
                    ls_chunk = [ls_tickers_one[i:i + n] for i in range(0, len(ls_tickers_one), n)]

                    for i in range(len(ls_chunk)):
                        ls_i.append((i, ls_chunk[i]))
                else:
                    ls_i.append((0, ls_tickers_one))

                for tuple_one in ls_i:
                    i = tuple_one[0]
                    ls_tickers_one = tuple_one[1]

                    self.dict_queue[exchange][quote][i] = mp.Queue()
                    self.dict_ws[exchange][quote][i] = \
                        self.Init_Websocket(
                            exchange=exchange,
                            public_key=self.dict_params[exchange]['public_key'],
                            private_key=self.dict_params[exchange]['private_key'],
                            q=self.dict_queue[exchange][quote][i],
                            wss_url=self.dict_params[exchange]['wss_url'],
                            channel=self.dict_params[exchange]['channel']['Orderbook'],
                            ls_tickers=ls_tickers_one)

                    self.dict_process_ws[exchange][quote][i] = \
                        mp.Process(target=self.dict_ws[exchange][quote][i].start_ws)
                    self.dict_process_ws[exchange][quote][i].daemon = False
                    self.dict_process_ws[exchange][quote][i].start()
                    self.dict_thread_data[exchange][quote][i] = None

    def Init_Websocket(self, exchange, public_key, private_key, q, wss_url, channel, ls_tickers):
        if exchange == 'BINANCE':
            ws_one = WebsocketApp_Binance(
                wss_url=wss_url, q=q, channel=channel, tickers=ls_tickers)
        elif exchange == 'UPBIT':
            header = FUNC_UPBIT_Signature_Generator(
                public_key=public_key, private_key=private_key, category='websocket', params=None)
            ws_one = WebsocketApp_Upbit(
                wss_url=wss_url, q=q, header=header, channel=channel, tickers=ls_tickers)
        elif exchange == 'COINONE':
            ws_one = WebsocketApp_Coinone(
                wss_url=wss_url, q=q, channel=channel, tickers=ls_tickers)
        elif exchange == 'BITHUMB':
            ws_one = WebsocketApp_Bithumb(
                wss_url=wss_url, q=q, channel=channel, tickers=ls_tickers)
        elif exchange == 'KORBIT':
            ws_one = WebsocketApp_Korbit(
                wss_url=wss_url, q=q, channel=channel, tickers=ls_tickers)

        return ws_one