import json
import threading
import queue
import multiprocessing as mp
import time
import numpy as np
import pandas as pd

from Parameters.Global_Parameters import Parameters
from Parameters.Func_Logger import LogStreamer
from Parameters.Ticker_Conversion import *
from Clients.__Manager import Manager_Client
from Websockets.__Manager import Manager_Wesocket


class Websocket_DataCollect:
    def __init__(self,
                 params: Parameters,
                 manager_client: Manager_Client,
                 manager_ws: Manager_Wesocket):

        self.manager_client = manager_client
        self.dict_tickers = manager_client.dict_tickers

        self.dict_thread_data = manager_ws.dict_thread_data
        self.dict_queue = manager_ws.dict_queue
        self.ls_exchange = manager_ws.ls_exchange
        self.ls_exchange_korea = manager_ws.ls_exchange_korea
        self.ls_exchange_foreign = manager_ws.ls_exchange_foreign

        self.dict_orderbook = dict()
        self.q_dict_orderbook = params.dict_queue['DATA_COLLECTION']['ORDERBOOK']
        self.q_dict_orderbook_one = params.dict_queue['DATA_COLLECTION']['ORDERBOOK_one']

        self.dict_kor_best_price = dict()
        self.dict_kor_best_price['MIN_ASK'] = dict()
        self.dict_kor_best_price['MAX_BID'] = dict()

        self.ls_min_AMT = [100 * 10000, 1000 * 10000]

        self.Init_Data()

    def Init_Data(self):
        for exchange in self.ls_exchange:
            self.dict_orderbook[exchange] = dict()

            for quote in self.dict_tickers['WEBSOCKET'][exchange].keys():
                self.dict_orderbook[exchange][quote] = dict()

                for ticker in self.dict_tickers['WEBSOCKET'][exchange][quote]:
                    ticker_one = TickerConversion_QuoteToBase(exchange=exchange, quote=quote, ticker_quote=ticker)

                    self.dict_orderbook[exchange][quote][ticker_one] = dict()
                    self.dict_orderbook[exchange][quote][ticker_one]['updated'] = False
                    self.dict_orderbook[exchange][quote][ticker_one]['ask_price'] = np.nan
                    self.dict_orderbook[exchange][quote][ticker_one]['ask_size'] = np.nan
                    self.dict_orderbook[exchange][quote][ticker_one]['bid_price'] = np.nan
                    self.dict_orderbook[exchange][quote][ticker_one]['bid_size'] = np.nan

                    self.dict_orderbook[exchange][quote][ticker_one]['orderbook'] = dict()
                    self.dict_orderbook[exchange][quote][ticker_one]['VWAP'] = dict()

                    for min_AMT in self.ls_min_AMT:
                        self.dict_orderbook[exchange][quote][ticker_one]['VWAP'][min_AMT] = dict()

                        self.dict_orderbook[exchange][quote][ticker_one]['VWAP'][min_AMT]['ask_price_VWAP'] = np.nan
                        self.dict_orderbook[exchange][quote][ticker_one]['VWAP'][min_AMT]['ask_size_VWAP'] = np.nan
                        self.dict_orderbook[exchange][quote][ticker_one]['VWAP'][min_AMT]['ask_amt_VWAP'] = np.nan

                        self.dict_orderbook[exchange][quote][ticker_one]['VWAP'][min_AMT]['bid_price_VWAP'] = np.nan
                        self.dict_orderbook[exchange][quote][ticker_one]['VWAP'][min_AMT]['bid_size_VWAP'] = np.nan
                        self.dict_orderbook[exchange][quote][ticker_one]['VWAP'][min_AMT]['bid_amt_VWAP'] = np.nan

        ############################################################################################################

        # exchange = 'BINANCE'
        # limit = 100
        #
        # n = 0
        # for quote in self.dict_tickers['WEBSOCKET'][exchange].keys():
        #     for ticker in self.dict_tickers['WEBSOCKET'][exchange][quote]:
        #         n += 1
        #
        # i = 0
        # for quote in self.dict_tickers['WEBSOCKET'][exchange].keys():
        #     for ticker in self.dict_tickers['WEBSOCKET'][exchange][quote]:
        #         ticker_base = TickerConversion_QuoteToBase(exchange=exchange, quote=quote, ticker_quote=ticker)
        #
        #         self.dict_orderbook[exchange][quote][ticker_base]['orderbook'] = \
        #             self.manager_client.dict_clients[exchange].get_orderbook_snapshot(ticker, limit)
        #
        #         i += 1
        #
        #         print(str(i / n) + ' // ' + ticker)

        ############################################################################################################

    def Init_DataCollect(self):
        self.q_threads = queue.Queue()

        for exchange in self.dict_thread_data.keys():
            for quote in self.dict_thread_data[exchange].keys():
                for i in self.dict_thread_data[exchange][quote].keys():
                    if exchange == 'BINANCE':
                        self.dict_thread_data[exchange][quote][i] = \
                            threading.Thread(target=self.UpdateData_Binance, args=(exchange, quote, i, self.q_threads))
                    elif exchange == 'UPBIT':
                        self.dict_thread_data[exchange][quote][i] = \
                            threading.Thread(target=self.UpdateData_Upbit, args=(exchange, quote, i, self.q_threads))
                    elif exchange == 'COINONE':
                        self.dict_thread_data[exchange][quote][i] = \
                            threading.Thread(target=self.UpdateData_Coinone, args=(exchange, quote, i, self.q_threads))
                    elif exchange == 'BITHUMB':
                        self.dict_thread_data[exchange][quote][i] = \
                            threading.Thread(target=self.UpdateData_Bithumb, args=(exchange, quote, i, self.q_threads))
                    elif exchange == 'KORBIT':
                        self.dict_thread_data[exchange][quote][i] = \
                            threading.Thread(target=self.UpdateData_Korbit, args=(exchange, quote, i, self.q_threads))

                    self.dict_thread_data[exchange][quote][i].start()

        self.t0 = threading.Thread(target=self.SendData_Queue_dict_orderbook, args=(self.q_threads, ))
        self.t0.start()

        self.t1 = threading.Thread(target=self.SendData_Queue_dict_orderbook_one, args=(self.q_threads, ))
        self.t1.start()

    def Func_Check_Thread_Status(self):

        for exchange in self.dict_thread_data.keys():
            for quote in self.dict_thread_data[exchange].keys():
                for i in self.dict_thread_data[exchange][quote].keys():

                    flag_one = self.dict_thread_data[exchange][quote][i].is_alive()

                    if flag_one == False:
                        return False

        return True

    def SendData_Queue_dict_orderbook(self, q_threads):

        time.sleep(1)

        while True:
            self.q_dict_orderbook.put(self.dict_orderbook)

            time.sleep(1)


    def SendData_Queue_dict_orderbook_one(self, q_threads):
        while True:
            data = q_threads.get()
            self.q_dict_orderbook_one.put(data)

    def UpdateData_Binance(self, exchange, quote, i, q_thread):
        while True:
            try:
                data = self.dict_queue[exchange][quote][i].get()

                ticker = data['s']
                ticker_base = TickerConversion_QuoteToBase(exchange=exchange, quote=quote, ticker_quote=ticker)

                dict_orderbook_one = self.dict_orderbook[exchange][quote][ticker_base]['orderbook']

                if dict_orderbook_one == dict():
                    ticker = TickerConversion_BaseToQuote(exchange=exchange, quote=quote, ticker_base=ticker_base)
                    limit = 100

                    self.dict_orderbook[exchange][quote][ticker_base]['orderbook'] = \
                        self.manager_client.dict_clients[exchange].get_orderbook_snapshot(ticker, limit)

                if data['u'] <= dict_orderbook_one['lastUpdateId']:
                    continue

                ############################################################################################################

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

                ############################################################################################################

                sorted_indices_a = np.argsort(dict_orderbook_one['ask_price'])
                dict_orderbook_one['ask_price'] = dict_orderbook_one['ask_price'][sorted_indices_a]
                dict_orderbook_one['ask_size'] = dict_orderbook_one['ask_size'][sorted_indices_a]

                zero_indices_a = np.where(dict_orderbook_one['ask_size'] == 0)[0]
                dict_orderbook_one['ask_price'] = np.delete(dict_orderbook_one['ask_price'], zero_indices_a)
                dict_orderbook_one['ask_size'] = np.delete(dict_orderbook_one['ask_size'], zero_indices_a)

                ############################################################################################################

                sorted_indices_b = np.argsort(dict_orderbook_one['bid_price'])[::-1]
                dict_orderbook_one['bid_price'] = dict_orderbook_one['bid_price'][sorted_indices_b]
                dict_orderbook_one['bid_size'] = dict_orderbook_one['bid_size'][sorted_indices_b]

                zero_indices_b = np.where(dict_orderbook_one['bid_size'] == 0)[0]
                dict_orderbook_one['bid_price'] = np.delete(dict_orderbook_one['bid_price'], zero_indices_b)
                dict_orderbook_one['bid_size'] = np.delete(dict_orderbook_one['bid_size'], zero_indices_b)

                ############################################################################################################

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

                self.dict_orderbook[exchange][quote][ticker_base]['orderbook'] = dict_orderbook_one

                ############################################################################################################

                self.dict_orderbook[exchange][quote][ticker_base]['ask_price'] = dict_orderbook_one['ask_price'][0]
                self.dict_orderbook[exchange][quote][ticker_base]['ask_size'] = dict_orderbook_one['ask_size'][0]
                self.dict_orderbook[exchange][quote][ticker_base]['bid_price'] = dict_orderbook_one['bid_price'][0]
                self.dict_orderbook[exchange][quote][ticker_base]['bid_size'] = dict_orderbook_one['bid_size'][0]

                self.UpdateData_VWAP_BINANCE(exchange, quote, ticker_base)

                result = \
                    {'exchange': exchange,
                     'quote': quote,
                     'ticker_base': ticker_base,
                     'data': self.dict_orderbook[exchange][quote][ticker_base]}

                q_thread.put(result)

            except:
                pass

    def UpdateData_Upbit(self, exchange, quote, i, q_thread):
        ls_quote = list(self.dict_orderbook[exchange].keys())
        ls_quote_noKRW = [quote for quote in ls_quote if quote != 'KRW']

        while True:
            try:
                data = self.dict_queue[exchange][quote][i].get()

                ticker = data['code']
                ticker_base = TickerConversion_QuoteToBase(exchange=exchange, quote=quote, ticker_quote=ticker)

                ask_price = data['orderbook_units'][0]['ask_price']
                ask_size = data['orderbook_units'][0]['ask_size']
                bid_price = data['orderbook_units'][0]['bid_price']
                bid_size = data['orderbook_units'][0]['bid_size']

                self.dict_orderbook[exchange][quote][ticker_base]['ask_price'] = ask_price
                self.dict_orderbook[exchange][quote][ticker_base]['ask_size'] = ask_size
                self.dict_orderbook[exchange][quote][ticker_base]['bid_price'] = bid_price
                self.dict_orderbook[exchange][quote][ticker_base]['bid_size'] = bid_size

                orderbook = data['orderbook_units']
                dict_orderbook_all = dict()

                for flag in ['ask', 'bid']:
                    if flag == 'ask':
                        dict_orderbook_all['ask_price'] = np.array([x['ask_price'] for x in orderbook])
                        dict_orderbook_all['ask_size'] = np.array([x['ask_size'] for x in orderbook])

                    elif flag == 'bid':
                        dict_orderbook_all['bid_price'] = np.array([x['bid_price'] for x in orderbook])
                        dict_orderbook_all['bid_size'] = np.array([x['bid_size'] for x in orderbook])

                self.dict_orderbook[exchange][quote][ticker_base]['orderbook'] = dict_orderbook_all

                self.UpdateData_VWAP_KRW(exchange, quote, ticker_base, dict_orderbook_all)
                self.Update_Arb_InnerExchange(ls_quote_noKRW, exchange, quote, ticker_base)

                self.dict_orderbook[exchange][quote][ticker_base]['updated'] = True

                result = \
                    {'exchange': exchange,
                     'quote': quote,
                     'ticker_base': ticker_base,
                     'data': self.dict_orderbook[exchange][quote][ticker_base]}

                q_thread.put(result)

            except:
                pass


    def UpdateData_Coinone(self, exchange, quote, i, q_thread):
        while True:
            try:
                data = self.dict_queue[exchange][quote][i].get()
                ticker = data['data']['target_currency']
                ticker_base = TickerConversion_QuoteToBase(exchange=exchange, quote=quote, ticker_quote=ticker)

                ask_price = float(data['data']['asks'][-1]['price'])
                ask_size = float(data['data']['asks'][-1]['qty'])
                bid_price = float(data['data']['bids'][0]['price'])
                bid_size = float(data['data']['bids'][0]['qty'])

                self.dict_orderbook[exchange][quote][ticker_base]['ask_price'] = ask_price
                self.dict_orderbook[exchange][quote][ticker_base]['ask_size'] = ask_size
                self.dict_orderbook[exchange][quote][ticker_base]['bid_price'] = bid_price
                self.dict_orderbook[exchange][quote][ticker_base]['bid_size'] = bid_size

                dict_orderbook_all = dict()

                for flag in ['ask', 'bid']:
                    if flag == 'ask':
                        dict_orderbook_all['ask_price'] = \
                            np.array([x['price'] for x in data['data']['asks']])[::-1].astype(float)
                        dict_orderbook_all['ask_size'] = \
                            np.array([x['qty'] for x in data['data']['asks']])[::-1].astype(float)

                    elif flag == 'bid':
                        dict_orderbook_all['bid_price'] = \
                            np.array([x['price'] for x in data['data']['bids']]).astype(float)
                        dict_orderbook_all['bid_size'] = \
                            np.array([x['qty'] for x in data['data']['bids']]).astype(float)

                self.dict_orderbook[exchange][quote][ticker_base]['orderbook'] = dict_orderbook_all

                self.UpdateData_VWAP_KRW(exchange, quote, ticker_base, dict_orderbook_all)

                self.dict_orderbook[exchange][quote][ticker_base]['updated'] = True

                result = \
                    {'exchange': exchange,
                     'quote': quote,
                     'ticker_base': ticker_base,
                     'data': self.dict_orderbook[exchange][quote][ticker_base]}

                q_thread.put(result)

            except:
                pass

    def UpdateData_Bithumb(self, exchange, quote, i, q_thread):
        ls_quote = list(self.dict_orderbook[exchange].keys())
        ls_quote_noKRW = [quote for quote in ls_quote if quote != 'KRW']

        while True:
            try:
                data = self.dict_queue[exchange][quote][i].get()
                ticker = data['content']['symbol']
                ticker_base = TickerConversion_QuoteToBase(exchange=exchange, quote=quote, ticker_quote=ticker)

                ask_price = float(data['content']['asks'][0][0])
                ask_size = float(data['content']['asks'][0][1])
                bid_price = float(data['content']['bids'][0][0])
                bid_size = float(data['content']['bids'][0][1])

                self.dict_orderbook[exchange][quote][ticker_base]['ask_price'] = ask_price
                self.dict_orderbook[exchange][quote][ticker_base]['ask_size'] = ask_size
                self.dict_orderbook[exchange][quote][ticker_base]['bid_price'] = bid_price
                self.dict_orderbook[exchange][quote][ticker_base]['bid_size'] = bid_size

                dict_orderbook_all = dict()

                for flag in ['ask', 'bid']:
                    if flag == 'ask':
                        dict_orderbook_all['ask_price'] = \
                            np.array([x[0] for x in data['content']['asks']]).astype(float)
                        dict_orderbook_all['ask_size'] = \
                            np.array([x[1] for x in data['content']['asks']]).astype(float)

                    elif flag == 'bid':
                        dict_orderbook_all['bid_price'] = \
                            np.array([x[0] for x in data['content']['bids']]).astype(float)
                        dict_orderbook_all['bid_size'] = \
                            np.array([x[1] for x in data['content']['bids']]).astype(float)

                self.dict_orderbook[exchange][quote][ticker_base]['orderbook'] = dict_orderbook_all

                self.UpdateData_VWAP_KRW(exchange, quote, ticker_base, dict_orderbook_all)
                self.Update_Arb_InnerExchange(ls_quote_noKRW, exchange, quote, ticker_base)

                self.dict_orderbook[exchange][quote][ticker_base]['updated'] = True

                result = \
                    {'exchange': exchange,
                     'quote': quote,
                     'ticker_base': ticker_base,
                     'data': self.dict_orderbook[exchange][quote][ticker_base]}

                q_thread.put(result)

            except:
                pass

    def UpdateData_Korbit(self, exchange, quote, i, q_thread):
        while True:
            try:
                data = self.dict_queue[exchange][quote][i].get()
                ticker = data['data']['currency_pair']
                ticker_base = TickerConversion_QuoteToBase(exchange=exchange, quote=quote, ticker_quote=ticker)

                ask_price = float(data['data']['asks'][0]['price'])
                ask_size = float(data['data']['asks'][0]['amount'])
                bid_price = float(data['data']['bids'][0]['price'])
                bid_size = float(data['data']['bids'][0]['amount'])

                self.dict_orderbook[exchange][quote][ticker_base]['ask_price'] = ask_price
                self.dict_orderbook[exchange][quote][ticker_base]['ask_size'] = ask_size
                self.dict_orderbook[exchange][quote][ticker_base]['bid_price'] = bid_price
                self.dict_orderbook[exchange][quote][ticker_base]['bid_size'] = bid_size

                dict_orderbook_all = dict()

                for flag in ['ask', 'bid']:
                    if flag == 'ask':
                        dict_orderbook_all['ask_price'] = \
                            np.array([x['price'] for x in data['data']['asks']]).astype(float)
                        dict_orderbook_all['ask_size'] = \
                            np.array([x['amount'] for x in data['data']['asks']]).astype(float)

                    elif flag == 'bid':
                        dict_orderbook_all['bid_price'] = \
                            np.array([x['price'] for x in data['data']['bids']]).astype(float)
                        dict_orderbook_all['bid_size'] = \
                            np.array([x['amount'] for x in data['data']['bids']]).astype(float)

                self.dict_orderbook[exchange][quote][ticker_base]['orderbook'] = dict_orderbook_all

                self.UpdateData_VWAP_KRW(exchange, quote, ticker_base, dict_orderbook_all)

                self.dict_orderbook[exchange][quote][ticker_base]['updated'] = True

                result = \
                    {'exchange': exchange,
                     'quote': quote,
                     'ticker_base': ticker_base,
                     'data': self.dict_orderbook[exchange][quote][ticker_base]}

                q_thread.put(result)

            except:
                pass

    def Update_Arb_InnerExchange(self, ls_quote_noKRW, exchange, quote, ticker_base):
        if quote == 'KRW':
            ls_quote_use = ls_quote_noKRW
        else:
            ls_quote_use = [quote]

        for quote_one in ls_quote_use:
            try:
                self.dict_orderbook[exchange][quote_one][ticker_base]['Strategy_Buy'] = \
                    self.dict_orderbook[exchange][quote_one][ticker_base]['bid_price'] * \
                    self.dict_orderbook[exchange]['KRW'][quote_one]['bid_price'] / \
                    self.dict_orderbook[exchange]['KRW'][ticker_base]['ask_price'] - 1
            except:
                pass

            try:
                self.dict_orderbook[exchange][quote_one][ticker_base]['Strategy_Sell'] = \
                    1 / self.dict_orderbook[exchange][quote_one][ticker_base]['ask_price'] * \
                    self.dict_orderbook[exchange]['KRW'][ticker_base]['bid_price'] / \
                    self.dict_orderbook[exchange]['KRW'][quote_one]['ask_price'] - 1
            except:
                pass

    def UpdateData_VWAP_KRW(self, exchange, quote, ticker_base, dict_orderbook_one):

        for min_AMT in self.ls_min_AMT:
            if quote == 'KRW':
                for flag in ['ask', 'bid']:
                    if flag == 'ask':
                        ls_price = dict_orderbook_one['ask_price']
                        ls_size = dict_orderbook_one['ask_size']

                    elif flag == 'bid':
                        ls_price = dict_orderbook_one['bid_price']
                        ls_size = dict_orderbook_one['bid_size']

                    ls_amt = ls_price * ls_size
                    ls_amt_cum = np.cumsum(ls_amt)

                    i = np.argmax(ls_amt_cum >= min_AMT) # 만약에 ls_amt_cum
                    ls_amt_adj = ls_amt.copy()

                    if i == 0:
                        if sum(ls_amt_cum >= min_AMT) == 0:
                            price_vwap = np.nan
                            size_vwap = np.nan
                        else:
                            price_vwap = ls_price[i]
                            size_vwap = min_AMT / ls_price[i]

                    else:
                        ls_amt_adj[i] = min_AMT - ls_amt_cum[i - 1]
                        ls_amt_adj[(i + 1):] = 0

                        price_vwap = sum(ls_price * ls_amt_adj) / min_AMT
                        size_vwap = min_AMT / price_vwap

                    if flag == 'ask':
                        # fee_withdraw_rate = fee_withdraw / size_vwap

                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['ask_price_VWAP'] = price_vwap
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['ask_size_VWAP'] = size_vwap
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['ask_amt_VWAP'] = min_AMT

                    elif flag == 'bid':
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['bid_price_VWAP'] = price_vwap
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['bid_size_VWAP'] = size_vwap
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['bid_amt_VWAP'] = min_AMT

            else:
                pass

    def UpdateData_VWAP_BINANCE(self, exchange, quote, ticker_base):
        try:
            dict_orderbook_one = self.dict_orderbook[exchange][quote][ticker_base]['orderbook']

            ls_exchange_kor = list(
                self.dict_tickers['ARBITRAGE']['ARB_INTER_FOREIGN'][exchange][quote][ticker_base].keys())

            for min_AMT in self.ls_min_AMT:
                ls_size_kor = []

                for exchange_kor_one in ls_exchange_kor:
                    ls_size_kor_one = \
                        self.dict_orderbook[exchange_kor_one]['KRW'][ticker_base]['VWAP'][min_AMT]['bid_size_VWAP']
                    ls_size_kor.append(ls_size_kor_one)

                if np.isnan(ls_size_kor).all():
                    return None
                else:
                    size_kor_max = np.nanmax(ls_size_kor)

                for flag in ['ask', 'bid']:

                    if flag == 'ask':
                        ls_price = dict_orderbook_one['ask_price']
                        ls_size = dict_orderbook_one['ask_size']

                    elif flag == 'bid':
                        ls_price = dict_orderbook_one['bid_price']
                        ls_size = dict_orderbook_one['bid_size']

                    ls_size_cum = np.cumsum(ls_size)
                    ls_size_adj = ls_size.copy()
                    i = np.argmax(ls_size_cum >= size_kor_max)  # 만약에 ls_amt_cum

                    if i == 0:
                        if sum(ls_size_cum >= size_kor_max) == 0:
                            price_vwap = np.nan
                            size_vwap = np.nan
                        else:
                            price_vwap = ls_price[i]
                            size_vwap = size_kor_max

                    else:
                        ls_size_adj[i] = size_kor_max - ls_size_cum[i - 1]
                        ls_size_adj[(i + 1):] = 0

                        price_vwap = sum(ls_price * ls_size_adj) / size_kor_max
                        size_vwap = size_kor_max

                    if flag == 'ask':
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['ask_price_VWAP'] = price_vwap
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['ask_size_VWAP'] = size_vwap
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['ask_amt_VWAP'] = min_AMT

                    elif flag == 'bid':
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['bid_price_VWAP'] = price_vwap
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['bid_size_VWAP'] = size_vwap
                        self.dict_orderbook[exchange][quote][ticker_base]['VWAP'][min_AMT]['bid_amt_VWAP'] = min_AMT


        except Exception as e:
            pass

if __name__ == '__main__':
    logger_main = LogStreamer()

    params = Parameters()
    manager_client = Manager_Client(params=params)
    manager_ws = Manager_Wesocket(params=params, manager_client=manager_client)

    data_collection = Websocket_DataCollect(params=params, manager_client=manager_client, manager_ws=manager_ws)
    # data_collection.Init_DataCollect()

    p = mp.Process(target=data_collection.Init_DataCollect)
    p.start()

    while True:
        dict_orderbook_one = params.dict_queue['DATA_COLLECTION']['dict_orderbook_one'].get()
        print(params.dict_queue['DATA_COLLECTION']['dict_orderbook_one'].qsize())

    ############################################################################################################

    for exchange_one in manager_ws.dict_process_ws.keys():
        for quote_one in manager_ws.dict_process_ws[exchange_one].keys():
            for i in manager_ws.dict_process_ws[exchange_one][quote_one].keys():
                manager_ws.dict_process_ws[exchange_one][quote_one][i].terminate()
                manager_ws.dict_process_ws[exchange_one][quote_one][i].join()

    p.terminate()
    p.join()
