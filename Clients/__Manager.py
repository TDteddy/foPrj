import threading
import time
import typing
import queue

import pandas as pd
import numpy as np

from Parameters.Global_Parameters import Parameters
from Parameters.Func_Logger import LogStreamer
from Parameters.Signature_Generator import FUNC_UPBIT_Signature_Generator
from Parameters.Ticker_Conversion import *

from Clients.Client_Binance import ClientBinance
from Clients.Client_Upbit import ClientUpbit
from Clients.Client_Bithumb import ClientBithumb
from Clients.Client_Coinone import ClientCoinone
from Clients.Client_Korbit import ClientKorbit

class Manager_Client:
    def __init__(self, params: Parameters, ls_exchange=None):
        self.params = params
        self.dict_params = self.params.dict_params

        ls_exchange_foreign = ['BINANCE']

        if ls_exchange is None:
            self.ls_exchange = list(self.dict_params.keys())
            self.ls_exchange_foreign = ls_exchange_foreign.copy()
            self.ls_exchange_korea = self.ls_exchange.copy()

            for exchange_one in self.ls_exchange_foreign:
                self.ls_exchange_korea.remove(exchange_one)
        else:
            self.ls_exchange = ls_exchange
            self.ls_exchange_foreign = self.ls_exchange.copy()
            self.ls_exchange_korea = self.ls_exchange.copy()

            for exchange_one in self.ls_exchange_foreign:
                if ls_exchange_foreign.count(exchange_one)==0:
                    self.ls_exchange_foreign.remove(exchange_one)

            for exchange_one in self.ls_exchange_foreign:
                self.ls_exchange_korea.remove(exchange_one)

        self.Init_Clients()

        if len(self.ls_exchange_foreign) > 0:
            self.Init_Tickers()

    def Init_Clients(self):
        self.dict_clients = dict()
        self.dict_thread = dict()
        self.dict_df_wallet_info = dict()
        self.dict_df_tickers = dict()
        self.dict_tickers = dict()

        for exchange in self.ls_exchange:
            if exchange == 'BINANCE':
                self.dict_clients[exchange] = ClientBinance(params=self.params)
            elif exchange == 'UPBIT':
                self.dict_clients[exchange] = ClientUpbit(params=self.params)
            elif exchange == 'COINONE':
                self.dict_clients[exchange] = ClientCoinone(params=self.params)
            elif exchange == 'BITHUMB':
                self.dict_clients[exchange] = ClientBithumb(params=self.params)
            elif exchange == 'KORBIT':
                self.dict_clients[exchange] = ClientKorbit(params=self.params)

            self.dict_df_wallet_info[exchange] = self.dict_clients[exchange].df_wallet_info
            self.dict_df_tickers[exchange] = self.dict_clients[exchange].df_tickers
            self.dict_tickers[exchange] = self.dict_clients[exchange].dict_tickers

        # self.params.chrome_driver.quit()

    def Init_Tickers(self):
        dict_tickers = dict()

        dict_tickers['FEE'] = dict()
        dict_tickers['FEE']['TRADE'] = dict()
        dict_tickers['FEE']['WITHDRAW'] = dict()
        dict_tickers['FEE']['DEPOSIT'] = dict()

        ############################################################################################################
        ############################################################################################################

        for exchange in self.ls_exchange:
            dict_tickers['FEE']['TRADE'][exchange] = dict()
            dict_tickers['FEE']['TRADE'][exchange]['MAKER'] = self.dict_params[exchange]['fee_rate']['maker']
            dict_tickers['FEE']['TRADE'][exchange]['TAKER'] = self.dict_params[exchange]['fee_rate']['taker']

            ls_quote = list(self.dict_params[exchange]['fee_rate'].keys())
            ls_quote.remove('maker')
            ls_quote.remove('taker')

            if len(ls_quote) != 0:
                for quote_one in ls_quote:
                    dict_tickers['FEE']['TRADE'][exchange][quote_one] = self.dict_params[exchange]['fee_rate'][quote_one]

            dict_tickers['FEE']['WITHDRAW'][exchange] = dict()
            dict_tickers['FEE']['DEPOSIT'][exchange] = dict()

            df_wallet_info = self.dict_df_wallet_info[exchange]

            ls_ticker = list(df_wallet_info['ticker_base'].unique())

            for ticker_one in ls_ticker:
                df_wallet_info_one = df_wallet_info.loc[df_wallet_info['ticker_base'] == ticker_one].copy()
                df_wallet_info_one = df_wallet_info_one.fillna(False)  # TODO: COINONE 때문!

                ls_network = list(df_wallet_info_one['network'].unique())

                dict_tickers['FEE']['WITHDRAW'][exchange][ticker_one] = dict()
                dict_tickers['FEE']['DEPOSIT'][exchange][ticker_one] = dict()

                for network_one in ls_network:
                    fee_withdraw = \
                        df_wallet_info_one[df_wallet_info_one['network'] == network_one]['fee_withdraw'].values[0]
                    fee_deposit = \
                        df_wallet_info_one[df_wallet_info_one['network'] == network_one]['fee_deposit'].values[0]

                    dict_tickers['FEE']['WITHDRAW'][exchange][ticker_one][network_one] = fee_withdraw
                    dict_tickers['FEE']['DEPOSIT'][exchange][ticker_one][network_one] = fee_deposit

        ############################################################################################################
        ############################################################################################################

        dict_tickers['WEBSOCKET'] = dict()

        dict_tickers['ARBITRAGE'] = dict()
        dict_tickers['ARBITRAGE']['ARB_INTER_FOREIGN'] = dict()
        dict_tickers['ARBITRAGE']['ARB_INTER_KOREA'] = dict()
        dict_tickers['ARBITRAGE']['ARB_INNER_KOREA'] = dict()

        for exchange_foreign in self.ls_exchange_foreign:
            ls_df = []

            df_tickers_one = self.dict_df_tickers[exchange_foreign].copy()
            df_tickers_one = df_tickers_one[['ticker_base', 'ticker_quote', 'status_trade']]
            df_tickers_one = df_tickers_one.drop_duplicates()

            df_wallet_info_one = self.dict_df_wallet_info[exchange_foreign].copy()
            df_wallet_info_one = df_wallet_info_one[['ticker_base', 'network', 'status_deposit', 'status_withdraw']]
            df_one = pd.merge(df_wallet_info_one, df_tickers_one, how='inner', on=['ticker_base'])

            df_one['status_trade'] = df_one['status_trade'].astype(bool)
            df_one['status_deposit'] = df_one['status_deposit'].astype(bool)
            df_one['status_withdraw'] = df_one['status_withdraw'].astype(bool)

            df_one[exchange_foreign] = (df_one['status_trade'] & df_one['status_deposit'] & df_one['status_withdraw'])
            df_one = df_one[['ticker_base', 'ticker_quote', 'network', exchange_foreign]]

            df_one = df_one.set_index(['ticker_base', 'ticker_quote', 'network'])
            df_one = df_one.loc[df_one[exchange_foreign] == True]

            ls_df.append(df_one)

            ############################################################################################################

            ls_df_korea = []

            for exchange_korea in self.ls_exchange_korea:
                df_tickers_one = self.dict_df_tickers[exchange_korea].copy()
                df_tickers_one['status_trade'] = df_tickers_one['status_trade'].astype(bool)

                ############################################################################################################
                ############################################################################################################

                df_temp = df_tickers_one.loc[df_tickers_one['status_trade'] == True].copy()

                ls_quote_korea_one = list(df_temp['ticker_quote'].unique())

                if (len(ls_quote_korea_one) - ls_quote_korea_one.count('KRW')) > 0:
                    dict_tickers['ARBITRAGE']['ARB_INNER_KOREA'][exchange_korea] = dict()

                    for quote_one in ls_quote_korea_one:
                        df_temp_KRW = df_temp.loc[df_temp['ticker_quote'] == 'KRW'].copy()
                        ls_tickers_KRW = list(df_temp_KRW['ticker_base'].unique())

                        if quote_one != 'KRW':
                            df_temp_one = df_temp.loc[df_temp['ticker_quote'] == quote_one].copy()

                            ls_tickers_quote = list(df_temp_one['ticker_base'].unique())
                            ls_tickers = list(set(ls_tickers_KRW) & set(ls_tickers_quote))

                            dict_tickers['ARBITRAGE']['ARB_INNER_KOREA'][exchange_korea][quote_one] = ls_tickers

                ############################################################################################################
                ############################################################################################################

                df_tickers_one = df_tickers_one.loc[
                    df_tickers_one['ticker_quote'] == 'KRW', ['ticker_base', 'status_trade']]

                df_wallet_info_one = self.dict_df_wallet_info[exchange_korea].copy()
                df_wallet_info_one = df_wallet_info_one[['ticker_base', 'network', 'status_deposit', 'status_withdraw']]

                df_one = pd.merge(df_wallet_info_one, df_tickers_one, how='inner', on=['ticker_base'])
                df_one['status_deposit'] = df_one['status_deposit'].astype(bool)
                df_one['status_withdraw'] = df_one['status_withdraw'].astype(bool)

                if df_one['status_deposit'].sum() == 0:
                    df_one['status_deposit'] = True
                if df_one['status_withdraw'].sum() == 0:
                    df_one['status_withdraw'] = True

                df_one[exchange_korea] = (df_one['status_trade'] & df_one['status_deposit'] & df_one['status_withdraw'])
                df_one = df_one.drop(columns=['status_trade', 'status_deposit', 'status_withdraw'])

                df_one = df_one.set_index(['ticker_base', 'network'])

                ls_df.append(df_one)
                ls_df_korea.append(df_one)

            ############################################################################################################

            for i in range(len(ls_df)):
                if i == 0:
                    df_all = ls_df[0]
                else:
                    df_all = pd.merge(df_all, ls_df[i], how='left', left_index=True, right_index=True)

            df_all = df_all.fillna(False)
            df_all['FLAG'] = df_all.sum(axis=1) - df_all[exchange_foreign]
            df_all = df_all.loc[df_all['FLAG'] > 0]
            df_all = df_all.sort_values('FLAG', ascending=False)
            df_all = df_all.reset_index()

            ############################################################################################################

            dict_tickers['ARBITRAGE']['ARB_INTER_FOREIGN'][exchange_foreign] = dict()
            dict_tickers['WEBSOCKET'][exchange_foreign] = dict()

            df_for_dict = df_all.copy()

            ls_quote = df_for_dict['ticker_quote'].unique()

            for quote_one in ls_quote:
                df_temp = df_for_dict.loc[df_for_dict['ticker_quote'] == quote_one]
                ls_ticker = list(df_temp['ticker_base'].unique())

                if len(ls_ticker) > 0:  # TODO: 웹소켓 부하 줄이기 위해 30개로 제한하는 하드코딩
                    dict_tickers['WEBSOCKET'][exchange_foreign][quote_one] = \
                        [TickerConversion_BaseToQuote(exchange=exchange_foreign, quote=quote_one,
                                                      ticker_base=ticker_base)
                         for ticker_base in ls_ticker]

                    dict_tickers['ARBITRAGE']['ARB_INTER_FOREIGN'][exchange_foreign][quote_one] = dict()

                    for ticker_one in ls_ticker:
                        dict_tickers['ARBITRAGE']['ARB_INTER_FOREIGN'][exchange_foreign][quote_one][ticker_one] = dict()

                        df_temp_one = df_temp.loc[df_temp['ticker_base'] == ticker_one]

                        for i in range(df_temp_one.shape[0]):
                            network_one = df_temp_one.iloc[i]['network']

                            for exchange_korea in self.ls_exchange_korea:
                                if df_temp_one.iloc[i][exchange_korea] == True:
                                    dict_tickers['ARBITRAGE']['ARB_INTER_FOREIGN'][
                                        exchange_foreign][quote_one][ticker_one][exchange_korea] = network_one

        ############################################################################################################
        ############################################################################################################
        self.ls_df_korea = ls_df_korea
        for i in range(len(ls_df_korea)):
            if i == 0:
                df_all_korea = ls_df_korea[0]
            else:
                df_temp = ls_df_korea[i].sort_index(level=['ticker_base', 'network'])
                df_temp = df_temp[df_temp.index.get_level_values('network').isna() == False]
                df_all_korea = pd.merge(df_all_korea, df_temp, how='outer', left_index=True, right_index=True)

        df_all_korea = df_all_korea.fillna(False)
        df_all_korea['FLAG'] = df_all_korea.sum(axis=1)
        df_all_korea = df_all_korea.sort_values('FLAG', ascending=False)
        df_all_korea = df_all_korea.loc[df_all_korea['FLAG'] > 1]
        # df_all_korea = df_all_korea.loc[((df_all_korea['COINONE'] == True) & (df_all_korea['FLAG'] == 2)) == False]  # TODO: COINONE 때문!!

        for exchange_korea in self.ls_exchange_korea:
            dict_tickers['ARBITRAGE']['ARB_INTER_KOREA'][exchange_korea] = dict()
            dict_tickers['ARBITRAGE']['ARB_INTER_KOREA'][exchange_korea]['KRW'] = dict()

            df_all_korea_temp = df_all_korea.reset_index().copy()
            df_all_korea_temp = df_all_korea_temp[df_all_korea_temp[exchange_korea] == True]
            df_all_korea_temp = df_all_korea_temp.sort_values(['ticker_base'])

            ls_tickers = list(df_all_korea_temp['ticker_base'].unique())

            for ticker_one in ls_tickers:
                df_all_korea_temp_one = df_all_korea_temp[df_all_korea_temp['ticker_base'] == ticker_one]

                ls_network = list(df_all_korea_temp_one['network'].unique())

                dict_tickers['ARBITRAGE']['ARB_INTER_KOREA'][exchange_korea]['KRW'][ticker_one] = ls_network

        ############################################################################################################
        ############################################################################################################

        for exchange_korea in self.ls_exchange_korea:
            dict_tickers['WEBSOCKET'][exchange_korea] = dict()

            ticker_temp_KRW = list(dict_tickers['ARBITRAGE']['ARB_INTER_KOREA'][exchange_korea]['KRW'].keys())

            if exchange_korea in list(dict_tickers['ARBITRAGE']['ARB_INNER_KOREA'].keys()):
                for quote in dict_tickers['ARBITRAGE']['ARB_INNER_KOREA'][exchange_korea].keys():
                    ticker_temp_inner = dict_tickers['ARBITRAGE']['ARB_INNER_KOREA'][exchange_korea][quote]
                    ticker_temp_KRW = list(set(ticker_temp_KRW) | set(ticker_temp_inner))

                    ticker_temp_inner = \
                        [TickerConversion_BaseToQuote(exchange=exchange_korea, quote=quote, ticker_base=ticker_base)
                         for ticker_base in ticker_temp_inner]
                    dict_tickers['WEBSOCKET'][exchange_korea][quote] = ticker_temp_inner

            ticker_temp_KRW = \
                [TickerConversion_BaseToQuote(exchange=exchange_korea, quote='KRW', ticker_base=ticker_base)
                 for ticker_base in ticker_temp_KRW]
            dict_tickers['WEBSOCKET'][exchange_korea]['KRW'] = ticker_temp_KRW

        ############################################################################################################
        ############################################################################################################

        self.dict_tickers = dict_tickers

def Func_Check_Websocket_Queue(params, manager_ws):
    # 웹소켓 큐 상태를 확인하고 빈 큐가 있는지 여부를 반환합니다.
    for exchange_one in manager_ws.dict_queue.keys():
        for quote_one in manager_ws.dict_queue[exchange_one].keys():
            for i in manager_ws.dict_queue[exchange_one][quote_one].keys():
                flag_one = params.dict_queue['WEBSOCKET'][exchange_one][quote_one][i].empty()
                if flag_one:
                    return False
    return True



if __name__ == '__main__':
    logger_main = LogStreamer()

    params = Parameters()
    ls_exchange = None
    client_manager = Manager_Client(params=params, ls_exchange=ls_exchange)