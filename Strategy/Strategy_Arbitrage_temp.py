import multiprocessing as mp
import threading
import time
import typing

import pandas as pd

from Websockets.Websocket_DataCollect_Arb import *
from Parameters.Global_Parameters import Parameters

class Strategy_Arbitrage:
    def __init__(self,
                 params: Parameters,
                 data_collect: Websocket_DataCollect):

        self.params = params
        self.dict_queue_DataCollection = params.dict_queue['DATA_COLLECTION']
        self.dict_queue_strategy = params.dict_queue['STRATEGY']
        self.dict_queue_simulation = params.dict_queue['SIMULATION']
        self.dict_queue_telegram = params.dict_queue['TELEGRAM']

        self.data_collect = data_collect
        self.ls_exchange_foreign = self.data_collect.ls_exchange_foreign
        self.ls_exchange_korea = self.data_collect.ls_exchange_korea
        self.dict_orderbook = self.data_collect.dict_orderbook
        self.dict_tickers = self.data_collect.dict_tickers
        self.dict_tickers_ArbIntFor = self.dict_tickers['ARBITRAGE']['ARB_INTER_FOREIGN']
        self.dict_tickers_ArbIntKOR = self.dict_tickers['ARBITRAGE']['ARB_INTER_KOREA']
        self.dict_tickers_ArbInnKOR = self.dict_tickers['ARBITRAGE']['ARB_INNER_KOREA']
        self.dict_fee = self.dict_tickers['FEE']

        self.trade_AMT = 1000000
        self.volume_AMT = 10000000

        self.Init_Data()

    def Init_Data(self):
        ############################################################################################################
        dict_one = dict()

        for ex_for in self.dict_tickers_ArbIntFor.keys():
            dict_one[ex_for] = dict()

            for quote in self.dict_tickers_ArbIntFor[ex_for].keys():
                dict_one[ex_for][quote] = dict()

                for ticker_base in self.dict_tickers_ArbIntFor[ex_for][quote].keys():
                    dict_one[ex_for][quote][ticker_base] = dict()

                    for ex_kor in self.dict_tickers_ArbIntFor[ex_for][quote][ticker_base].keys():
                        dict_one[ex_for][quote][ticker_base][ex_kor] = None

        self.dict_ArbInterForeign = dict_one
        ############################################################################################################
        dict_one = dict()

        for ex_for in self.dict_tickers_ArbIntFor.keys():
            dict_one[ex_for] = dict()

            for quote in self.dict_tickers_ArbIntFor[ex_for].keys():
                dict_one[ex_for][quote] = dict()

                for ticker0 in self.dict_tickers_ArbIntFor[ex_for][quote].keys():
                    dict_one[ex_for][quote][ticker0] = dict()

                    for ex_kor_start in self.dict_tickers_ArbIntFor[ex_for][quote][ticker0].keys():
                        dict_one[ex_for][quote][ticker0][ex_kor_start] = dict()

                        for ticker1 in self.dict_tickers_ArbIntFor[ex_for][quote].keys():
                            dict_one[ex_for][quote][ticker0][ex_kor_start][ticker1] = dict()

                            for ex_kor_end in self.dict_tickers_ArbIntFor[ex_for][quote][ticker1].keys():
                                dict_one[ex_for][quote][ticker0][ex_kor_start][ticker1][ex_kor_end] = None

        self.dict_ArbInterForeign_Trade = dict_one
        ############################################################################################################
        dict_one = dict()

        for ex_start in self.dict_tickers_ArbIntKOR.keys():
            dict_one[ex_start] = dict()

            for ticker_base in self.dict_tickers_ArbIntKOR[ex_start]['KRW'].keys():
                dict_one[ex_start][ticker_base] = dict()
                network_start = self.dict_tickers_ArbIntKOR[ex_start]['KRW'][ticker_base][0]

                for ex_end in self.dict_tickers_ArbIntKOR.keys():
                    if ex_start == ex_end:
                        continue

                    if ticker_base in self.dict_tickers_ArbIntKOR[ex_end]['KRW'].keys():
                        network_end = self.dict_tickers_ArbIntKOR[ex_end]['KRW'][ticker_base][0]

                        if network_start == network_end:
                            dict_one[ex_start][ticker_base][ex_end] = None

        self.dict_ArbInterKorea = dict_one
        ############################################################################################################
        dict_one = dict()

        for exchange in self.dict_tickers_ArbInnKOR.keys():
            dict_one[exchange] = dict()

            for quote in self.dict_tickers_ArbInnKOR[exchange].keys():
                dict_one[exchange][quote] = dict()

                for ticker_base in self.dict_tickers_ArbInnKOR[exchange][quote]:
                    dict_one[exchange][quote][ticker_base] = None


        self.dict_ArbInnerKorea = dict_one
        ############################################################################################################

    def Init_Threads(self):
        self.dict_thread = dict()

        self.dict_thread['ORDERBOOK'] = threading.Thread(target=self.Update_Orderbook)
        self.dict_thread['ORDERBOOK'].start()

        self.dict_thread['ARBITRAGE_raw'] = threading.Thread(target=self.Update_Arbitrage)
        self.dict_thread['ARBITRAGE_raw'].start()

        self.dict_thread['ARBITRAGE_result'] = threading.Thread(target=self.Update_Arbitrage_Result)
        self.dict_thread['ARBITRAGE_result'].start()

    def Update_Arbitrage_Result(self):
        '''
        - df_Arb_FOR_Inter
        - df_Arb_FOR_Inter_Trade
        - df_Arb_KOR_Inter
        - df_Arb_KOR_Inner
        - df_Arb_KOR_Inter_Comp
        '''
        while True:
            start = time.time()
            ############################################################################################################

            ls_dict = list()
            for ex_for in self.dict_ArbInterForeign:
                for quote in self.dict_ArbInterForeign[ex_for].keys():
                    for ticker_base in self.dict_ArbInterForeign[ex_for][quote].keys():
                        for ex_kor_start in self.dict_ArbInterForeign[ex_for][quote][ticker_base].keys():

                            dict_one = self.dict_ArbInterForeign[ex_for][quote][ticker_base][ex_kor_start]

                            if dict_one is None:
                                continue
                            if np.nan in dict_one.values():
                                continue

                            ls_dict.append(dict_one)

            try:
                df_one = pd.DataFrame(ls_dict)
                df_one = df_one.sort_values('convert_start_mm', ascending=True)
                df_one = df_one.dropna().reset_index(drop=True)

                df_Arb_FOR_Inter = df_one

                ls_dict = list()
                for exchange in self.ls_exchange_korea:
                    for quote in ['KRW']:
                        for ticker_base in self.dict_orderbook[exchange][quote].keys():
                            dict_orderbook_one = self.dict_orderbook[exchange][quote][ticker_base]
                            dict_fee_one = self.dict_fee['TRADE'][exchange]['TAKER']

                            dict_one = dict()
                            dict_one['exchange'] = exchange
                            dict_one['ticker_base'] = ticker_base
                            dict_one['fee_rate'] = dict_fee_one
                            dict_one['KRW_ask_p'] = (
                                    dict_orderbook_one['VWAP'][self.trade_AMT]['ask_price_VWAP'] * (
                                    1 + dict_fee_one))
                            dict_one['KRW_bid_p'] = (
                                    dict_orderbook_one['VWAP'][self.trade_AMT]['bid_price_VWAP'] * (
                                    1 - dict_fee_one))
                            dict_one['KRW_ask_p_raw'] = dict_orderbook_one['ask_price']
                            dict_one['KRW_bid_p_raw'] = dict_orderbook_one['bid_price']

                            dict_one['tick_size_ask'] = self.Func_tick_size(
                                exchange=exchange,
                                quote='KRW',
                                ticker_base=ticker_base,
                                price=dict_one['KRW_ask_p_raw'])
                            dict_one['tick_size_bid'] = self.Func_tick_size(
                                exchange=exchange,
                                quote='KRW',
                                ticker_base=ticker_base,
                                price=dict_one['KRW_bid_p_raw'])

                            ls_dict.append(dict_one)

                df_KRW_raw = pd.DataFrame(ls_dict)
                df_KRW_raw = df_KRW_raw.dropna()
                df_KRW = df_KRW_raw[['exchange', 'ticker_base', 'fee_rate', 'KRW_ask_p', 'KRW_bid_p']].copy()
                df_KRW_grp = df_KRW.groupby(['ticker_base'])[['KRW_ask_p', 'KRW_bid_p']].agg({
                    'KRW_ask_p': 'min',
                    'KRW_bid_p': 'max'}).reset_index()

                ############################################################################################################

                ls_dict = list()
                for exchange in self.ls_exchange_foreign:
                    for quote in self.dict_orderbook[exchange].keys():
                        for ticker_base in self.dict_orderbook[exchange][quote].keys():
                            dict_orderbook_one = self.dict_orderbook[exchange][quote][ticker_base]
                            dict_fee_one = self.dict_fee['TRADE'][exchange]['TAKER']

                            dict_one = dict()
                            dict_one['exchange'] = exchange
                            dict_one['quote'] = quote
                            dict_one['ticker_base'] = ticker_base
                            dict_one['fee_rate'] = dict_fee_one
                            dict_one['FOR_ask_p'] = (
                                    dict_orderbook_one['VWAP'][self.trade_AMT]['ask_price_VWAP'] * (
                                        1 + dict_fee_one))
                            dict_one['FOR_bid_p'] = (
                                    dict_orderbook_one['VWAP'][self.trade_AMT]['bid_price_VWAP'] * (
                                        1 - dict_fee_one))
                            dict_one['FOR_ask_p_raw'] = dict_orderbook_one['ask_price']
                            dict_one['FOR_bid_p_raw'] = dict_orderbook_one['bid_price']

                            ls_dict.append(dict_one)

                df_FOR_raw = pd.DataFrame(ls_dict)
                df_FOR = df_FOR_raw[['exchange', 'quote', 'ticker_base', 'fee_rate', 'FOR_ask_p', 'FOR_bid_p']].copy()
                df_FOR_grp = df_FOR.groupby(['quote', 'ticker_base'])[['FOR_ask_p', 'FOR_bid_p']].agg({
                    'FOR_ask_p': 'min',
                    'FOR_bid_p': 'max'}).reset_index()

                ############################################################################################################
                # todo: 임의로 바꿔놓은 부분

                df_noArb_cond = df_KRW
                df_noArb_equi_price_KRW = df_KRW_grp
                df_noArb_equi_price_FOR = df_FOR
                df_Arb_KRW_equi_Trade = df_FOR_grp


            except Exception as e:
                print(' NoArb :  ')
                print(e)

                df_Arb_FOR_Inter = pd.DataFrame(None)

                df_noArb_cond = pd.DataFrame(None)
                df_noArb_equi_price_KRW = pd.DataFrame(None)
                df_noArb_equi_price_FOR = pd.DataFrame(None)
                df_Arb_KRW_equi_Trade = pd.DataFrame(None)

            ############################################################################################################
            ############################################################################################################

            self.dict_queue_strategy['Arb_FOR_Inter'].put(df_Arb_FOR_Inter)

            self.dict_queue_strategy['NoArb_convert'].put(df_noArb_cond)
            self.dict_queue_strategy['NoArb_KRW_equi'].put(df_noArb_equi_price_KRW)
            self.dict_queue_strategy['NoArb_KRW_equi_Trade'].put(df_Arb_KRW_equi_Trade)

            ############################################################################################################

            self.dict_queue_simulation['NoArb_convert'].put(df_noArb_cond)
            self.dict_queue_simulation['NoArb_equi_KRW'].put(df_noArb_equi_price_KRW)
            self.dict_queue_simulation['NoArb_equi_FOR'].put(df_noArb_equi_price_FOR)

            ############################################################################################################

            self.dict_queue_telegram['Arb_FOR_Inter'].put(df_Arb_FOR_Inter)

            self.dict_queue_telegram['NoArb_convert'].put(df_noArb_cond)
            self.dict_queue_telegram['NoArb_KRW_equi'].put(df_noArb_equi_price_KRW)
            self.dict_queue_telegram['NoArb_KRW_equi_Trade'].put(df_Arb_KRW_equi_Trade)

            ############################################################################################################
            ############################################################################################################

            try:
                df_Arb_FOR_Inter_Trade = pd.DataFrame(None)

            except:
                df_Arb_FOR_Inter_Trade = pd.DataFrame(None)

            ############################################################################################################

            ls_dict = list()
            for ex_start in self.dict_ArbInterKorea:
                for ticker_base in self.dict_ArbInterKorea[ex_start].keys():
                    for ex_end in self.dict_ArbInterKorea[ex_start][ticker_base].keys():

                        dict_one = self.dict_ArbInterKorea[ex_start][ticker_base][ex_end]

                        if dict_one is None:
                            continue
                        if np.nan in dict_one.values():
                            continue

                        ls_dict.append(dict_one)

            try:
                df_one = pd.DataFrame(ls_dict)
                df_one = df_one.sort_values('r_mm', ascending=False)
                df_one = df_one.dropna().reset_index(drop=True)
                df_Arb_KOR_Inter = df_one

            except:
                df_Arb_KOR_Inter = pd.DataFrame(None)

            ############################################################################################################

            ls_dict = list()
            for exchange in self.dict_ArbInnerKorea:
                for quote in self.dict_ArbInnerKorea[exchange].keys():
                    for ticker_base in self.dict_ArbInnerKorea[exchange][quote].keys():

                        dict_one = self.dict_ArbInnerKorea[exchange][quote][ticker_base]

                        if dict_one is None:
                            continue
                        if np.nan in dict_one.values():
                            continue

                        ls_dict.append(dict_one)

            try:
                df_one = pd.DataFrame(ls_dict)
                df_one = df_one.sort_values('r_s_mmm', ascending=False)
                df_one = df_one.dropna().reset_index(drop=True)
                df_Arb_KOR_Inner = df_one

            except:
                df_Arb_KOR_Inner = pd.DataFrame(None)

            ############################################################################################################

            ls_dict = list()
            for exchange in self.ls_exchange_korea:
                for quote in ['KRW']:
                    for ticker_base in self.dict_orderbook[exchange][quote].keys():
                        dict_orderbook_one = self.dict_orderbook[exchange][quote][ticker_base]
                        dict_fee_one = self.dict_fee['TRADE'][exchange]['TAKER']

                        dict_one = dict()
                        dict_one['exchange'] = exchange
                        dict_one['ticker_base'] = ticker_base
                        dict_one['fee_rate'] = dict_fee_one
                        dict_one['ask_price_VWAP'] = dict_orderbook_one['VWAP'][self.trade_AMT]['ask_price_VWAP'] * (1 + dict_fee_one)
                        dict_one['bid_price_VWAP'] = dict_orderbook_one['VWAP'][self.trade_AMT]['bid_price_VWAP'] * (1 - dict_fee_one)

                        ls_dict.append(dict_one)

            try:
                df = pd.DataFrame(ls_dict)

                df_pivot = pd.pivot_table(
                    df, values=['ask_price_VWAP', 'bid_price_VWAP'], index=['ticker_base'], columns=['exchange'])

                # df_pivot['minA_exchange'] = df_pivot['ask_price_VWAP'].idxmin(axis=1)

                df_pivot['min_ask_VWAP'] = df_pivot['ask_price_VWAP'].min(axis=1)
                df_pivot['max_ask_VWAP'] = df_pivot['ask_price_VWAP'].max(axis=1)
                df_pivot['min_bid_VWAP'] = df_pivot['bid_price_VWAP'].min(axis=1)
                df_pivot['max_bid_VWAP'] = df_pivot['bid_price_VWAP'].max(axis=1)

                df_pivot['mean_ask_price_VWAP'] = (
                        (df_pivot['ask_price_VWAP'].sum(axis=1) - df_pivot['min_ask_VWAP']) /
                        (df_pivot['ask_price_VWAP'].count(axis=1) - 1))
                df_pivot['mean_bid_price_VWAP'] = (
                        (df_pivot['bid_price_VWAP'].sum(axis=1) - df_pivot['max_bid_VWAP']) /
                        (df_pivot['bid_price_VWAP'].count(axis=1) - 1))

                df_pivot['diff_pct_minA_meanA'] = df_pivot['mean_ask_price_VWAP'] / df_pivot['min_ask_VWAP'] - 1
                df_pivot['diff_pct_minA_meanB'] = df_pivot['mean_bid_price_VWAP'] / df_pivot['min_ask_VWAP'] - 1

                df_Arb_KOR_Inter_Comp = df_pivot.sort_values('diff_pct_minA_meanB', ascending=False)

            except:
                df_Arb_KOR_Inter_Comp = pd.DataFrame(None)

            ############################################################################################################

            self.dict_queue_strategy['Arb_FOR_Inter_Trade'].put(df_Arb_FOR_Inter_Trade)
            # self.dict_queue_strategy['Arb_KOR_Inter_Trade'].put(df_Arb_KOR_Inter)
            # self.dict_queue_strategy['Arb_KOR_Inner_Trade'].put(df_Arb_KOR_Inner)
            # self.dict_queue_strategy['Arb_KOR_Inter_Comp'].put(df_Arb_KOR_Inter_Comp)

            end = time.time()
            print('[Arbitrage Result Calc] Time Elapsed: ' + f'{(end-start):.5f}' + ' seconds')
            time.sleep(1)

            ############################################################################################################

    def Update_Orderbook(self):
        while True:
            data = self.dict_queue_DataCollection['ORDERBOOK'].get()
            # data = self.dict_orderbook
            # self.dict_queue_strategy['ORDERBOOK'].put(data)

    def Update_Arbitrage(self):
        while True:
            data = self.dict_queue_DataCollection['ORDERBOOK_one'].get()

            exchange = data['exchange']
            quote = data['quote']
            ticker_base = data['ticker_base']

            self.dict_orderbook[exchange][quote][ticker_base] = data['data']

            ############################################################################################################
            ################################### Arbitrage Inter Foreign // Each Side ###################################
            ############################################################################################################

            if exchange in self.dict_tickers_ArbIntFor.keys():

                for exchange_kor in self.dict_tickers_ArbIntFor[exchange][quote][ticker_base].keys():

                    dict_ArbIntFor_one = \
                        self.Func_Arb_Inter_For(
                            exchange, quote, ticker_base, exchange_kor, self.dict_orderbook)

            ############################################################################################################

            elif quote == 'KRW':

                for exchange_foreign in self.dict_tickers_ArbIntFor.keys():
                    for quote_one in self.dict_tickers_ArbIntFor[exchange_foreign].keys():

                        if not ticker_base in self.dict_tickers_ArbIntFor[exchange_foreign][quote_one].keys():
                            continue
                        if not exchange in self.dict_tickers_ArbIntFor[exchange_foreign][quote_one][ticker_base].keys():
                            continue

                        dict_ArbIntFor_one = \
                            self.Func_Arb_Inter_For(
                                exchange_foreign, quote_one, ticker_base, exchange, self.dict_orderbook)

                ############################################################################################################
                ########################################## Arbitrage Inter Korea ###########################################
                ############################################################################################################

                if exchange in self.dict_ArbInterKorea.keys():
                    if ticker_base in self.dict_ArbInterKorea[exchange].keys():
                        for exchange_end in self.dict_ArbInterKorea[exchange][ticker_base].keys():
                            dict_ArbInterKor_one = \
                                self.Func_Arb_Inter_Kor(exchange, ticker_base, exchange_end, self.dict_orderbook)
                            dict_ArbInterKor_reverse_one = \
                                self.Func_Arb_Inter_Kor(exchange_end, ticker_base, exchange, self.dict_orderbook)

            ############################################################################################################
            ########################################## Arbitrage Inner Korea ###########################################
            ############################################################################################################

            if exchange in self.dict_ArbInnerKorea.keys():
                for quote_one in self.dict_ArbInnerKorea[exchange].keys():
                    if (quote_one == ticker_base) and (quote == 'KRW'): # ticker_base == BTC or USDT
                        for ticker_base_one in self.dict_ArbInnerKorea[exchange][quote_one].keys():
                            dict_ArbInnerKor_one = \
                                self.Func_Arb_Inner_Kor(exchange, quote_one, ticker_base_one, self.dict_orderbook)

                    elif ticker_base in self.dict_ArbInnerKorea[exchange][quote_one].keys():
                        if (quote == 'KRW') or (quote == quote_one):
                            dict_ArbInnerKor_one = \
                                self.Func_Arb_Inner_Kor(exchange, quote_one, ticker_base, self.dict_orderbook)

    def Func_Arb_Inter_For(self, exchange_foreign, quote, ticker_base, exchange_kor, dict_orderbook):
        try:
            network = self.dict_tickers_ArbIntFor[exchange_foreign][quote][ticker_base][exchange_kor]

            dict_foreign_raw = dict_orderbook[exchange_foreign][quote][ticker_base]
            dict_foreign_vwap = dict_orderbook[exchange_foreign][quote][ticker_base]['VWAP'][self.trade_AMT]
            dict_foreign_vwap_l = dict_orderbook[exchange_foreign][quote][ticker_base]['VWAP'][self.volume_AMT]

            dict_kor_raw = dict_orderbook[exchange_kor]['KRW'][ticker_base]
            dict_kor_vwap = dict_orderbook[exchange_kor]['KRW'][ticker_base]['VWAP'][self.trade_AMT]
            dict_kor_vwap_l = dict_orderbook[exchange_kor]['KRW'][ticker_base]['VWAP'][self.volume_AMT]

            if np.nan in dict_foreign_raw.values():
                return None
            if np.nan in dict_foreign_vwap.values():
                return None
            if np.nan in dict_foreign_vwap_l.values():
                return None

            if np.nan in dict_kor_raw.values():
                return None
            if np.nan in dict_kor_vwap.values():
                return None
            if np.nan in dict_kor_vwap_l.values():
                return None

            #################################################################################################

            fee_trade_kor = self.dict_fee['TRADE'][exchange_kor]['TAKER']
            fee_trade_foreign = self.dict_fee['TRADE'][exchange_foreign]['TAKER']

            fee_withdraw_start = self.dict_fee['WITHDRAW'][exchange_kor][ticker_base][network]
            fee_deposit_start = self.dict_fee['DEPOSIT'][exchange_foreign][ticker_base][network]
            fee_withdraw_end = self.dict_fee['WITHDRAW'][exchange_foreign][ticker_base][network]
            fee_deposit_end = self.dict_fee['DEPOSIT'][exchange_kor][ticker_base][network]

            fee_start = \
                (1 - fee_withdraw_start / dict_kor_vwap['ask_size_VWAP']) * \
                (1 - fee_deposit_start / dict_kor_vwap['ask_size_VWAP']) * \
                (1 - fee_trade_kor) * \
                (1 - fee_trade_foreign)
            fee_end = \
                (1 - fee_withdraw_end / dict_kor_vwap['bid_size_VWAP']) * \
                (1 - fee_deposit_end / dict_kor_vwap['bid_size_VWAP']) * \
                (1 - fee_trade_foreign) * \
                (1 - fee_trade_kor)

            #################################################################################################

            fee_start_l = \
                (1 - fee_withdraw_start / dict_kor_vwap_l['ask_size_VWAP']) * \
                (1 - fee_deposit_start / dict_kor_vwap_l['ask_size_VWAP']) * \
                (1 - fee_trade_kor) * \
                (1 - fee_trade_foreign)
            fee_end_l = \
                (1 - fee_withdraw_end / dict_kor_vwap_l['bid_size_VWAP']) * \
                (1 - fee_deposit_end / dict_kor_vwap_l['bid_size_VWAP']) * \
                (1 - fee_trade_foreign) * \
                (1 - fee_trade_kor)

            #################################################################################################
            dict_one = dict()

            dict_one['exchange_foreign'] = exchange_foreign
            dict_one['quote'] = quote
            dict_one['ticker_base'] = ticker_base
            dict_one['exchange_kor'] = exchange_kor
            dict_one['network'] = network

            dict_one['fee_start'] = 1 - fee_start
            dict_one['fee_end'] = 1 - fee_end
            dict_one['fee_start_v'] = 1 - fee_start_l
            dict_one['fee_end_v'] = 1 - fee_end_l

            dict_one['convert_start_mm'] = 0.0
            dict_one['convert_start_ll'] = 0.0
            dict_one['convert_end_mm'] = 0.0
            dict_one['convert_end_ll'] = 0.0

            dict_one['convert_start_mm_v'] = 0.0
            dict_one['convert_start_ll_v'] = 0.0
            dict_one['convert_end_mm_v'] = 0.0
            dict_one['convert_end_ll_v'] = 0.0

            dict_one['KOR_ask_p_v'] = dict_kor_vwap['ask_price_VWAP']
            dict_one['KOR_bid_p_v'] = dict_kor_vwap['bid_price_VWAP']
            dict_one['KOR_ask_p'] = dict_kor_raw['ask_price']
            dict_one['KOR_bid_p'] = dict_kor_raw['bid_price']
            dict_one['FOR_ask_p_v'] = dict_foreign_vwap['ask_price_VWAP']
            dict_one['FOR_bid_p_v'] = dict_foreign_vwap['bid_price_VWAP']
            dict_one['FOR_ask_p'] = dict_foreign_raw['ask_price']
            dict_one['FOR_bid_p'] = dict_foreign_raw['bid_price']

            dict_one['KOR_ask_s_v'] = dict_kor_vwap['ask_size_VWAP']
            dict_one['KOR_bid_s_v'] = dict_kor_vwap['bid_size_VWAP']
            dict_one['KOR_ask_s'] = dict_kor_raw['ask_size']
            dict_one['KOR_bid_s'] = dict_kor_raw['bid_size']
            dict_one['FOR_ask_s_v'] = dict_foreign_vwap['ask_size_VWAP']
            dict_one['FOR_bid_s_v'] = dict_foreign_vwap['bid_size_VWAP']
            dict_one['FOR_ask_s'] = dict_foreign_raw['ask_size']
            dict_one['FOR_bid_s'] = dict_foreign_raw['bid_size']

            self.dict_ArbInterForeign[exchange_foreign][quote][ticker_base][exchange_kor] = dict_one

            return dict_one

        except Exception as e:
            return None

    def Func_Arb_Inter_Kor(self, exchange_start, ticker_base, exchange_end, dict_orderbook):
        try:
            dict_start_raw = dict_orderbook[exchange_start]['KRW'][ticker_base]
            dict_start = dict_orderbook[exchange_start]['KRW'][ticker_base]['VWAP'][self.trade_AMT]
            network_start = self.dict_tickers_ArbIntKOR[exchange_start]['KRW'][ticker_base][0]

            dict_end_raw = dict_orderbook[exchange_end]['KRW'][ticker_base]
            dict_end = dict_orderbook[exchange_end]['KRW'][ticker_base]['VWAP'][self.trade_AMT]
            network_end = self.dict_tickers_ArbIntKOR[exchange_end]['KRW'][ticker_base][0]

            if np.nan in dict_start_raw.values():
                return None
            if np.nan in dict_start.values():
                return None
            if np.nan in dict_end_raw.values():
                return None
            if np.nan in dict_end.values():
                return None

            if network_start != network_end:
                return None

            network = network_start

            fee_trade_start = self.dict_fee['TRADE'][exchange_start]['TAKER']
            fee_trade_end = self.dict_fee['TRADE'][exchange_end]['TAKER']

            fee_withdraw_start = self.dict_fee['WITHDRAW'][exchange_start][ticker_base][network]
            fee_deposit_end = self.dict_fee['DEPOSIT'][exchange_end][ticker_base][network]

            fee_start = (1 - fee_withdraw_start / dict_start['ask_size_VWAP']) * \
                        (1 - fee_trade_start)
            fee_end = (1 - fee_deposit_end / dict_end['bid_size_VWAP']) * \
                      (1 - fee_trade_end)

            dict_one = dict()
            dict_one['ticker_base'] = ticker_base
            dict_one['exchange_start'] = exchange_start
            dict_one['exchange_end'] = exchange_end
            dict_one['network'] = network

            dict_one['r_mm'] = 0.0
            dict_one['r_lm'] = 0.0
            dict_one['r_ml'] = 0.0
            dict_one['r_ll'] = 0.0

            dict_one['fee_start'] = 1 - fee_start
            dict_one['fee_end'] = 1 - fee_end
            dict_one['fee_all'] = 1 - fee_start * fee_end

            dict_one['ask_price_start_VWAP'] = dict_start['ask_price_VWAP']
            dict_one['bid_price_start'] = dict_start_raw['bid_price']
            dict_one['ask_price_end'] = dict_end_raw['ask_price']
            dict_one['bid_price_end_VWAP'] = dict_end['bid_price_VWAP']

            dict_one['ask_size_start_VWAP'] = dict_start['ask_size_VWAP']
            dict_one['bid_size_start'] = self.trade_AMT / dict_start_raw['bid_price']
            dict_one['ask_size_end'] = self.trade_AMT / dict_end_raw['ask_price']
            dict_one['bid_size_end_VWAP'] = dict_end['bid_size_VWAP']

            self.dict_ArbInterKorea[exchange_start][ticker_base][exchange_end] = dict_one

            return dict_one


        except Exception as e:
            return None

    def Func_Arb_Inner_Kor(self, exchange, quote, ticker_base, dict_orderbook):
        try:
            fee_trade_KRW = self.dict_fee['TRADE'][exchange]['TAKER']
            fee_trade_quote = self.dict_fee['TRADE'][exchange][quote]

            dict_ticker_KRW = dict_orderbook[exchange]['KRW'][ticker_base]
            dict_quote_KRW = dict_orderbook[exchange]['KRW'][quote]
            dict_ticker_quote = dict_orderbook[exchange][quote][ticker_base]

            dict_one = dict()

            dict_one['exchange'] = exchange
            dict_one['quote'] = quote
            dict_one['ticker_base'] = ticker_base

            dict_one['ask_price_KRW'] = dict_ticker_KRW['ask_price']
            dict_one['bid_price_KRW'] = dict_ticker_KRW['bid_price']
            dict_one['ask_price_quote'] = dict_ticker_quote['ask_price']
            dict_one['bid_price_quote'] = dict_ticker_quote['bid_price']
            dict_one['ask_price_quoteKRW'] = dict_quote_KRW['ask_price']
            dict_one['bid_price_quoteKRW'] = dict_quote_KRW['bid_price']

            dict_one['ask_size_KRW'] = dict_ticker_KRW['ask_size']
            dict_one['bid_size_KRW'] = dict_ticker_KRW['bid_size']
            dict_one['ask_size_quote'] = dict_ticker_quote['ask_size']
            dict_one['bid_size_quote'] = dict_ticker_quote['bid_size']
            dict_one['ask_size_quoteKRW'] = dict_quote_KRW['ask_size']
            dict_one['bid_size_quoteKRW'] = dict_quote_KRW['bid_size']

            dict_one['ask_amount_KRW'] = \
                dict_ticker_KRW['ask_size'] * dict_ticker_KRW['ask_price']
            dict_one['bid_amount_KRW'] = \
                dict_ticker_KRW['bid_size'] * dict_ticker_KRW['bid_price']
            dict_one['ask_amount_quote'] = \
                dict_ticker_quote['ask_size'] * dict_ticker_KRW['bid_price']
            dict_one['bid_amount_quote'] = \
                dict_ticker_quote['bid_size'] * dict_ticker_KRW['ask_price']
            dict_one['ask_amount_quoteKRW'] = \
                dict_quote_KRW['ask_size'] * dict_quote_KRW['ask_price']
            dict_one['bid_amount_quoteKRW'] = \
                dict_quote_KRW['bid_size'] * dict_quote_KRW['bid_price']

            dict_one['fee_all'] = 1 - (1 - fee_trade_KRW) * (1 - fee_trade_quote) * (1 - fee_trade_KRW)

            dict_one['r_b_mmm'] = 0.0
            dict_one['r_b_lmm'] = 0.0
            dict_one['r_s_mmm'] = 0.0
            dict_one['r_s_lmm'] = 0.0

            dict_one['b_AMT'] = \
                min(dict_one['ask_amount_KRW'],
                    dict_one['bid_amount_quote'],
                    dict_one['bid_amount_quoteKRW'])
            dict_one['s_AMT'] = \
                min(dict_one['bid_amount_KRW'],
                    dict_one['ask_amount_quote'],
                    dict_one['ask_amount_quoteKRW'])


            self.dict_ArbInnerKorea[exchange][quote][ticker_base] = dict_one

            return dict_one


        except Exception as e:
            return None

    def Func_tick_size(self, exchange: str, quote: str, ticker_base: str, price: float):
        try:
            if pd.isna(price) == True:
                tick_size = None

            elif exchange == 'UPBIT':
                ls_Exception_ticker_base = [
                    'ADA, ALGO, BLUR, CELO, ELF, EOS, GRS, GRT, ICX, MANA, MINA, POL, SAND, SEI, STG, TRX']

                if quote == 'KRW':
                    if price >= 2000000:
                        tick_size = 1000
                    elif price >= 1000000:
                        tick_size = 500
                    elif price >= 500000:
                        tick_size = 100
                    elif price >= 100000:
                        tick_size = 50
                    elif price >= 10000:
                        tick_size = 10
                    elif price >= 1000:
                        tick_size = 1
                    elif price >= 100:
                        if ticker_base in ls_Exception_ticker_base:
                            tick_size = 1
                        else:
                            tick_size = 0.1
                    elif price >= 10:
                        tick_size = 0.01
                    elif price >= 1:
                        tick_size = 0.001
                    elif price >= 0.1:
                        tick_size = 0.0001
                    elif price >= 0.01:
                        tick_size = 0.00001
                    elif price >= 0.001:
                        tick_size = 0.000001
                    elif price >= 0.0001:
                        tick_size = 0.0000001
                    elif price < 0.0001:
                        tick_size = 0.00000001

                elif quote == 'BTC':
                    tick_size = 0.00000001

                elif quote == 'USDT':
                    if price >= 10:
                        tick_size = 0.01
                    elif price >= 1:
                        tick_size = 0.001
                    elif price >= 0.1:
                        tick_size = 0.0001
                    elif price >= 0.01:
                        tick_size = 0.00001
                    elif price >= 0.001:
                        tick_size = 0.000001
                    elif price >= 0.0001:
                        tick_size = 0.0000001
                    elif price < 0.0001:
                        tick_size = 0.00000001

            elif exchange == 'BITHUMB':
                if quote == 'KRW':
                    if price >= 1000000:
                        tick_size = 1000
                    elif price >= 500000:
                        tick_size = 500
                    elif price >= 100000:
                        tick_size = 100
                    elif price >= 50000:
                        tick_size = 50
                    elif price >= 10000:
                        tick_size = 10
                    elif price >= 5000:
                        tick_size = 5
                    elif price >= 1000:
                        tick_size = 1
                    elif price >= 100:
                        tick_size = 1
                    elif price >= 10:
                        tick_size = 0.01
                    elif price >= 1:
                        tick_size = 0.001
                    elif price < 1:
                        tick_size = 0.0001

                elif quote == 'BTC':
                    tick_size = 0.00000001

            elif exchange == 'COINONE':
                if quote == 'KRW':
                    if price >= 1000000:
                        tick_size = 1000
                    elif price >= 500000:
                        tick_size = 500
                    elif price >= 100000:
                        tick_size = 100
                    elif price >= 50000:
                        tick_size = 50
                    elif price >= 10000:
                        tick_size = 10
                    elif price >= 5000:
                        tick_size = 5
                    elif price >= 1000:
                        tick_size = 1
                    elif price >= 100:
                        tick_size = 0.1
                    elif price >= 10:
                        tick_size = 0.01
                    elif price >= 1:
                        tick_size = 0.001
                    elif price < 1:
                        tick_size = 0.0001

            elif exchange == 'KORBIT':
                if quote == 'KRW':
                    if price >= 1000000:
                        tick_size = 1000
                    elif price >= 500000:
                        tick_size = 500
                    elif price >= 100000:
                        tick_size = 100
                    elif price >= 50000:
                        tick_size = 50
                    elif price >= 10000:
                        tick_size = 10
                    elif price >= 5000:
                        tick_size = 5
                    elif price >= 1000:
                        tick_size = 1
                    elif price >= 100:
                        tick_size = 0.1
                    elif price >= 10:
                        tick_size = 0.01
                    elif price >= 1:
                        tick_size = 0.001
                    elif price < 1:
                        tick_size = 0.0001

            return tick_size

        except:
            tick_size = None
            return tick_size


if __name__ == '__main__':
    ############################################################################################################
    logger_main = LogStreamer()

    params = Parameters()
    manager_client = Manager_Client(params=params, ls_exchange=None)

    ############################################################################################################

    manager_ws = Manager_Wesocket(params=params, manager_client=manager_client)

    flag_manager_ws_queue = True

    while flag_manager_ws_queue:
        sum_flag_qsize_one = 0
        num_queue = 0

        for exchange in manager_ws.dict_queue.keys():
            for quote in manager_ws.dict_queue[exchange].keys():
                for num in manager_ws.dict_queue[exchange][quote].keys():
                    flag_qsize_one = (manager_ws.dict_queue[exchange][quote][num].qsize() != 0)
                    sum_flag_qsize_one += flag_qsize_one
                    num_queue += 1

        if num_queue == sum_flag_qsize_one:
            flag_manager_ws_queue = False
            logger_main.logger.info('[WEBSOCKET] Websocket Data Queued!')
        else:
            flag_manager_ws_queue = True
            logger_main.logger.info('[WEBSOCKET] Websocket Data Not Enough! '
                                    '(' + str(sum_flag_qsize_one) + ' / ' + str(num_queue) + ')')
        time.sleep(1)

    data_collection = Websocket_DataCollect(params=params, manager_client=manager_client, manager_ws=manager_ws)
    p = mp.Process(target=data_collection.Init_DataCollect)
    p.daemon = True
    p.start()

    ############################################################################################################

    strategy = Strategy_Arbitrage(
        params=params,
        data_collect=data_collection)
    strategy.Init_Threads()

    ############################################################################################################
    ############################################################################################################

    for exchange_one in manager_ws.dict_process_ws.keys():
        for quote_one in manager_ws.dict_process_ws[exchange_one].keys():
            for i in manager_ws.dict_process_ws[exchange_one][quote_one].keys():
                manager_ws.dict_process_ws[exchange_one][quote_one][i].terminate()
                manager_ws.dict_process_ws[exchange_one][quote_one][i].join()
    p.terminate()
    p.join()