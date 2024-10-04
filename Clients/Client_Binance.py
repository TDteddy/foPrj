import requests
import logging
import time
import typing

from urllib.parse import urlencode

import hmac
import hashlib
import websocket
import json

import threading
import queue
import pandas as pd
import numpy as np

from Parameters.Global_Parameters import Parameters
from Parameters.Func_Logger import LogStreamer

logger = logging.getLogger()

class ClientBinance:
    def __init__(self, params: Parameters):

        self.exchange = 'BINANCE'
        self.params = params.dict_params

        self.public_key = self.params[self.exchange]['public_key']
        self.private_key = self.params[self.exchange]['private_key']
        self._headers = {'X-MBX-APIKEY': self.public_key}

        self.api_url = self.params[self.exchange]['api_url']

        self.get_tickers()
        self.get_all_coin_info()

        self.Init_Data()

        logger.info('[' + self.exchange + '] Client Successfully Initialized!!!')

    def Init_Data(self):
        df_wallet_info = self.df_wallet_info_all
        df_wallet_info = df_wallet_info.loc[
            (df_wallet_info['withdrawAllEnable'] == True) &
            (df_wallet_info['depositAllEnable'] == True) &
            (df_wallet_info['trading'] == True)]

        ls_tickers_transfer = df_wallet_info['ticker_base'].to_list()

        df_tickers = self.df_tickers
        df_tickers = df_tickers[df_tickers['ticker_base'].isin(ls_tickers_transfer)]
        df_tickers = df_tickers[df_tickers['status_trade'] == True].reset_index(drop=True)
        df_tickers = df_tickers.sort_values(['ticker_quote', 'ticker_base'])
        df_tickers_grp = df_tickers.groupby(['ticker_quote'])['ticker_quote'].count()
        df_tickers_grp = df_tickers_grp[df_tickers_grp > 0]
        ls_quote = df_tickers_grp.index.to_list()
        df_tickers = df_tickers.loc[df_tickers['ticker_quote'].isin(ls_quote)]

        dict_tickers = dict()
        dict_tickers['TICKERS'] = dict()

        for quote in ls_quote:
            dict_tickers['TICKERS'][quote] = df_tickers.loc[df_tickers['ticker_quote'] == quote][
                'ticker_base'].to_list()

        dict_tickers['TICKERS_UNION'] = ls_tickers_transfer
        dict_tickers['TICKERS_INTERSECT'] = ls_tickers_transfer
        dict_tickers['WITHDRAW'] = ls_tickers_transfer
        dict_tickers['DEPOSIT'] = ls_tickers_transfer

        self.dict_tickers = dict_tickers

    def _generate_signature(self, data: typing.Dict) -> str:
        return hmac.new(self.private_key.encode(),
                        urlencode(data).encode(),
                        hashlib.sha256).hexdigest()

    def _make_requests(self, method: str, endpoint: str, data: typing.Dict):
        if method == 'GET':
            try:
                response = requests.get(self.api_url + endpoint,
                                        params=data,
                                        headers=self._headers)
            except Exception as e:
                logger.error('Connection error while making %s request to %s %s', method, endpoint, e)
                return None

        elif method == 'POST':
            try:
                response = requests.post(self.api_url + endpoint,
                                         params=data,
                                         headers=self._headers)
                print(response.json())
            except Exception as e:
                logger.error('Connection error while making %s request to %s %s', method, endpoint, e)
                return None

        elif method == 'DELETE':
            try:
                response = requests.delete(self.api_url + endpoint,
                                           params=data,
                                           headers=self._headers)
            except Exception as e:
                logger.error('Connection error while making %s request to %s %s', method, endpoint, e)
                return None

        else:
            raise ValueError()

        if response.status_code == 200:
            return response.json()
        else:
            logger.error('Error While making %s requests to %s: (ErrorCode %s)',
                         method, endpoint, response.status_code)
            return None

    def get_tickers(self) -> pd.DataFrame:
        exchange_info = self._make_requests('GET', '/api/v3/exchangeInfo', dict())
        df_exchange_info = \
            pd.DataFrame(
                columns=['ticker', 'ticker_base', 'ticker_quote', 'status_trade',
                         'prec_baseA', 'prec_quoteA', 'prec'])

        for info_one in exchange_info['symbols']:
            for filter in info_one['filters']:
                if filter['filterType'] == 'PRICE_FILTER':
                    tick_size = filter['tickSize']
                elif filter['filterType'] == 'LOT_SIZE':
                    step_size = filter['stepSize']

            df_one = \
                pd.DataFrame(
                    {'ticker': [info_one['symbol']],
                     'ticker_base': [info_one['baseAsset']],
                     'ticker_quote': [info_one['quoteAsset']],
                     'status_trade': [info_one['status']],
                     'prec_baseA': [info_one['baseAssetPrecision']],
                     'prec_quoteA': [info_one['quoteAssetPrecision']],
                     'prec': [info_one['quotePrecision']],
                     'tick_size': [float(tick_size)],
                     'step_size': [float(step_size)]})

            df_exchange_info = pd.concat([df_exchange_info, df_one])

        df_exchange_info.loc[df_exchange_info['status_trade'] == 'TRADING', 'status_trade'] = True
        df_exchange_info.loc[df_exchange_info['status_trade'] != True, 'status_trade'] = False

        self.df_tickers = df_exchange_info.reset_index(drop=True)

        logger.info('[Binance] Information(Ticker) Extracted')


    def get_all_coin_info(self):
        data = dict()
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        all_coin_info = self._make_requests('GET', '/sapi/v1/capital/config/getall', data)

        ls_wallet_info_all = []
        ls_wallet_info = []

        for info_one in all_coin_info:
            ls_wallet_info_all.append(
                {'ticker_base': info_one['coin'],
                 'name': info_one['name'],
                 'trading': info_one['trading'],
                 'depositAllEnable': info_one['depositAllEnable'],
                 'withdrawAllEnable': info_one['withdrawAllEnable']})

            for network in info_one['networkList']:
                ls_wallet_info.append(
                    {'ticker_base': network['coin'],
                     'network_name': network['name'],
                     'network_type': network['network'],
                     'network': network['network'],
                     '2ndAddress': network['sameAddress'],
                     'status_deposit': network['depositEnable'],
                     'status_withdraw': network['withdrawEnable'],
                     'fee_deposit': 0,
                     'fee_withdraw': network['withdrawFee'],
                     'minAmt_withdraw': network['withdrawMin'],
                     'max_precision': network['withdrawIntegerMultiple'],
                     'confirm_count': network['minConfirm'],
                     'estimatedArrivalTime': network['estimatedArrivalTime']})

        df_wallet_info_all = pd.DataFrame.from_dict(ls_wallet_info_all)
        df_wallet_info_all = df_wallet_info_all.reset_index(drop=True)
        self.df_wallet_info_all = df_wallet_info_all

        for column in self.df_wallet_info_all.columns:
            try:
                self.df_wallet_info_all[column] = self.df_wallet_info_all[column].astype(float)
            except:
                pass

        df_wallet_info = pd.DataFrame.from_dict(ls_wallet_info)
        df_wallet_info = df_wallet_info.reset_index(drop=True)
        self.df_wallet_info = df_wallet_info

        for column in self.df_wallet_info.columns:
            try:
                self.df_wallet_info[column] = self.df_wallet_info[column].astype(float)
            except:
                pass

        logger.info('[Binance] Information(Network/Deposit/Withdraw) Extracted')

    def get_orderbook_snapshot(self, ticker, limit):
        data = dict()
        data['symbol'] = ticker
        data['limit'] = limit

        orderbook_raw = self._make_requests('GET', '/api/v3/depth', data)

        dict_orderbook_all = dict()
        dict_orderbook_all['lastUpdateId'] = orderbook_raw['lastUpdateId']

        for flag in ['asks', 'bids']:
            data = orderbook_raw[flag]

            if flag == 'asks':
                dict_orderbook_all['ask_price'] = \
                    np.array([x[0] for x in data]).astype(float)
                dict_orderbook_all['ask_size'] = \
                    np.array([x[1] for x in data]).astype(float)

            elif flag == 'bids':
                dict_orderbook_all['bid_price'] = \
                    np.array([x[0] for x in data]).astype(float)
                dict_orderbook_all['bid_size'] = \
                    np.array([x[1] for x in data]).astype(float)

        return dict_orderbook_all


    def ORDER_POST(self, ticker, side, order_type, price, quantity, timeinforce=None) -> typing.Dict:
        dict_ticker_info = self.df_tickers.loc[self.df_tickers['ticker'] == ticker].to_dict('records')[0]
        tick_size = dict_ticker_info['tick_size']
        step_size = dict_ticker_info['step_size']

        data = dict()
        data['symbol'] = dict_ticker_info['ticker']
        data['side'] = side.upper()
        data['quantity'] = round(quantity / step_size) * step_size
        data['type'] = order_type.upper()

        if price is not None:
            data['price'] = round(price / tick_size) * tick_size

        if timeinforce is not None:
            data['timeInForce'] = timeinforce

        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('POST', '/api/v3/order', data)

        logger.info('[Binance][Order] Order Placed!!! %s %s %s %s',
                    result['symbol'], result['side'], result['price'], result['origQty'])

        return result

    def ORDER_DEL(self, ticker: str, order_id: int) -> typing.Dict:
        data = dict()
        data['symbol'] = ticker
        data['orderId'] = order_id
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('DELETE', '/api/v3/order', data)

        logger.info('[Binance][Order] Order Cancelled!!! %s %s %s %s',
                    result['symbol'], result['side'], result['price'], result['origQty'])

        return result

    def ORDER_GET_Status_oneTicker_OneOrder(self, ticker, order_id: int) -> typing.Dict:
        data = dict()
        data['symbol'] = ticker
        data['orderId'] = order_id
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('GET', '/api/v3/order', data)

        logger.info('[Binance][Order] Got an Order Status!!! %s %s %s %s',
                    result['symbol'], result['side'], result['price'], result['origQty'])

        return result

    def ORDER_GET_Status_oneTicker_AllOrders(self, ticker) -> typing.Dict:
        data = dict()
        data['symbol'] = ticker
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('GET', '/api/v3/allOrders', data)

        logger.info('[Binance][Order] Got ALL Order Status!!!')

        return result

    def ORDER_GET_OpenOrders(self, ticker) -> typing.Dict:
        data = dict()
        if ticker != None:
            data['symbol'] = ticker

        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('GET', '/api/v3/openOrders', data)

        if ticker != None:
            logger.info('[Binance][Order][%s] Got ALL Open Order Status!!!', ticker)
        else:
            logger.info('[Binance][Order][ALL] Got ALL Open Order Status!!!')

        return result

    def ACCOUNT_GET_Info(self) -> typing.Dict:
        data = dict()
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('GET', '/api/v3/account', data)

        logger.info('[Binance][ACCOUNT] Account Info Refreshed!!!')

        return result

    def WITHDRAW_POST(self, coin, network, address, addressTag, amount):
        data = dict()
        data['coin'] = coin
        data['network'] = network
        data['address'] = address
        data['addressTag'] = addressTag
        data['amount'] = amount

        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('GET', '/sapi/v1/capital/withdraw/apply', data)

        logger.info('[Binance][ACCOUNT] Withdraw Requested!!!')

        return result

    def WITHDRAW_GET_History(self, coin):
        data = dict()
        if coin != None:
            data['coin'] = coin
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('GET', '/sapi/v1/capital/withdraw/history', data)

        logger.info('[Binance][ACCOUNT] Withdraw History Refreshed!!!')

        return result

    def DEPOSIT_GET_History(self, coin):
        data = dict()
        if coin != None:
            data['coin'] = coin
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('GET', '/sapi/v1/capital/deposit/hisrec', data)

        logger.info('[Binance][ACCOUNT] Deposit History Refreshed!!!')

        return result

    def DEPSOIT_GET_Address(self, coin, network):
        data = dict()
        data['coin'] = coin
        data['network'] = network
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('GET', '/sapi/v1/capital/deposit/address', data)

        logger.info('[Binance][WALLET][%s] Got Deposit Address!!!', coin)

        return result

    def CONVERT_GET_ExchangeInfo(self, fromAsset, toAsset):
        data = dict()
        if fromAsset != None:
            data['fromAsset'] = fromAsset

        if toAsset != None:
            data['toAsset'] = toAsset

        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('GET', '/sapi/v1/convert/exchangeInfo', data)

        logger.info('[Binance][Convert] From[%s] -> to[%s]', fromAsset, toAsset)

        return result

    def CONVERT_GET_PrecisionInfo(self):
        data = dict()
        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('GET', '/sapi/v1/convert/assetInfo', data)

        logger.info('[Binance][Convert] Precision Information Extracted')

        return result

    def CONVERT_POST_getQuote(self, fromAsset, toAsset, fromAmount, toAmount):
        data = dict()
        data['fromAsset'] = fromAsset
        data['toAsset'] = toAsset

        if toAmount == None:
            data['fromAmount'] = fromAmount
        if fromAmount == None:
            data['toAmount'] = toAmount

        data['timestamp'] = int(time.time() * 1000)
        data['signature'] = self._generate_signature(data)

        result = self._make_requests('POST', '/sapi/v1/convert/getQuote', data)

        logger.info('[Binance][Convert] Quotation Arrived / From[%s] -> To[%s]', fromAsset, toAsset)

        return result


if __name__ == '__main__':
    logger_main = LogStreamer()
    params = Parameters()

    client_binance = ClientBinance(params=params)

    ticker = 'XRPUSDT'
    limit = 100

    dict_orderbook_raw = client_binance.get_orderbook_snapshot(ticker, limit)