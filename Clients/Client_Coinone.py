import base64
import hashlib
import hmac
import uuid

import json

import requests
import logging
import time
import datetime
import typing

from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

import pandas as pd

from Parameters.Global_Parameters import Parameters
from Parameters.Func_Logger import LogStreamer

logger = logging.getLogger()

class ClientCoinone:
    def __init__(self, params: Parameters):

        self.exchange = 'COINONE'
        self.params = params
        self.dict_params = params.dict_params

        self.public_key = self.dict_params[self.exchange]['public_key']
        self.private_key = self.dict_params[self.exchange]['private_key']

        self.api_url = self.dict_params[self.exchange]['api_url']

        self.Init_Data()

        logger.info('[' + self.exchange + '] Client Successfully Initialized!!!')

    def Init_Data(self):
        self.dict_balance = \
            self.ACCOUNT_POST_Balance_All()

        self.dict_fee = \
            self.ACCOUNT_POST_TradeFee()
        self.dict_range_unit = \
            self.PUBLIC_GET_InfoRangeUnits()
        dict_InfoMarketsByQuoteCurrency = \
            self.PUBLIC_GET_InfoMarketsByQuoteCurrency(quote_currency='KRW')
        dict_InfoCurrencies = \
            self.PUBLIC_GET_InfoCurrencies()

        #################################################################################################

        self.df_tickers = \
            pd.DataFrame.from_dict(dict_InfoMarketsByQuoteCurrency)

        self.df_tickers = self.df_tickers.rename(
            columns={'quote_currency': 'ticker_quote',
                     'target_currency': 'ticker_base',
                     'trade_status': 'status_trade',
                     'maintenance_status': 'status_maintenance'})
        self.df_tickers['ticker'] = self.df_tickers['ticker_base']

        for column in self.df_tickers.columns:
            try:
                self.df_tickers[column] = self.df_tickers[column].astype(float)
            except:
                pass

        # self.df_tickers['status_trade'] = self.df_tickers['status_trade'].astype(int)
        # self.df_tickers['status_maintenance'] = self.df_tickers['status_maintenance'].astype(int)
        #
        # self.df_tickers.loc[self.df_tickers['status_trade'] == 1, 'status_trade'] = True
        # self.df_tickers.loc[self.df_tickers['status_trade'] != True, 'status_trade'] = False
        # self.df_tickers.loc[self.df_tickers['status_maintenance'] == 1, 'status_maintenance'] = True
        # self.df_tickers.loc[self.df_tickers['status_maintenance'] != True, 'status_maintenance'] = False

        self.df_tickers['status_trade'] = self.df_tickers['status_trade'].astype(bool)
        self.df_tickers['status_maintenance'] = self.df_tickers['status_maintenance'].astype(bool)

        self.df_tickers.loc[self.df_tickers['status_trade'] != True, 'status_trade'] = False
        self.df_tickers.loc[self.df_tickers['status_maintenance'] != True, 'status_maintenance'] = False

        self.df_tickers = \
            self.df_tickers[['ticker', 'ticker_base', 'ticker_quote', 'status_trade', 'status_maintenance',
                             'price_unit', 'qty_unit',
                             'max_order_amount', 'max_price', 'max_qty',
                             'min_order_amount', 'min_price', 'min_qty',
                             'order_book_units', 'order_types']]


        #################################################################################################

        self.df_wallet_info = \
            pd.DataFrame.from_dict(dict_InfoCurrencies)

        self.df_wallet_info = self.df_wallet_info.rename(
            columns={'symbol': 'ticker_base',
                     'deposit_status': 'status_deposit',
                     'withdraw_status': 'status_withdraw',
                     'deposit_fee': 'fee_deposit',
                     'withdrawal_fee': 'fee_withdraw',
                     'deposit_confirm_count': 'confirm_count',
                     'withdrawal_min_amount': 'minAmt_withdraw'})

        df_network = self.CRAWLING_Network_and_Fee()

        self.df_wallet_info = pd.merge(self.df_wallet_info, df_network, how='left', on=['ticker_base'])

        self.df_wallet_info['network_name'] = self.df_wallet_info['network']
        self.df_wallet_info['network_type'] = self.df_wallet_info['network']

        self.df_wallet_info.loc[self.df_wallet_info['status_deposit'] == 'normal', 'status_deposit'] = True
        self.df_wallet_info.loc[self.df_wallet_info['status_deposit'] != True, 'status_deposit'] = False
        self.df_wallet_info.loc[self.df_wallet_info['status_withdraw'] == 'normal', 'status_withdraw'] = True
        self.df_wallet_info.loc[self.df_wallet_info['status_withdraw'] != True, 'status_withdraw'] = False

        for column in self.df_wallet_info.columns:
            try:
                self.df_wallet_info[column] = self.df_wallet_info[column].astype(float)
            except:
                pass

        self.df_wallet_info = \
            self.df_wallet_info[['ticker_base', 'name',
                                 'network_name', 'network_type', 'network',
                                 'status_deposit', 'status_withdraw', 'fee_deposit', 'fee_withdraw',
                                 'minAmt_withdraw', 'max_precision', 'confirm_count']]

        check_unique_network = self.df_wallet_info.groupby('ticker_base')['network_name'].count()
        check_unique_network = check_unique_network.reset_index().rename(columns={'network_name': 'check'})
        check_unique_network = check_unique_network.loc[check_unique_network['check'] > 1]

        if check_unique_network.shape[0] != 0:
            self.df_wallet_info['FLAG'] = \
                self.df_wallet_info.groupby('ticker_base')['fee_withdraw'].transform('min') == \
                self.df_wallet_info['fee_withdraw']
            self.df_wallet_info = self.df_wallet_info.loc[self.df_wallet_info['FLAG'] == True].drop(columns=['FLAG'])

            check_unique_network = self.df_wallet_info.groupby('ticker_base')['network_name'].count()
            check_unique_network = check_unique_network.reset_index().rename(columns={'network_name': 'check'})
            check_unique_network = check_unique_network.loc[check_unique_network['check'] > 1]

            if check_unique_network.shape[0] != 0:
                print(check_unique_network)

        #################################################################################################

        ls_tickers_deposit = \
            list(self.df_wallet_info.loc[self.df_wallet_info['status_deposit'] == True]['ticker_base'].unique())
        ls_tickers_withdraw = \
            list(self.df_wallet_info.loc[self.df_wallet_info['status_withdraw'] == True]['ticker_base'].unique())
        ls_tickers_union = list(set(ls_tickers_withdraw) | set(ls_tickers_deposit))
        ls_tickers_intersect = list(set(ls_tickers_withdraw) & set(ls_tickers_deposit))

        #################################################################################################

        dict_tickers = dict()
        dict_tickers['TICKERS'] = dict()

        ls_quote = self.df_tickers['ticker_quote'].unique().tolist()
        df_tickers = self.df_tickers
        df_tickers = df_tickers.loc[(df_tickers['status_trade'] == True) & (df_tickers['status_maintenance'] == False)]

        for quote in ls_quote:
            dict_tickers['TICKERS'][quote] = \
                df_tickers.loc[df_tickers['ticker_quote'] == quote]['ticker_base'].to_list()

        dict_tickers['TICKERS_UNION'] = ls_tickers_union
        dict_tickers['TICKERS_INTERSECT'] = ls_tickers_intersect
        dict_tickers['DEPOSIT'] = ls_tickers_deposit
        dict_tickers['WITHDRAW'] = ls_tickers_withdraw

        self.dict_tickers = dict_tickers

        #################################################################################################


    def FUNC_Generate_Signature(self, request_type: str, payload: typing.Dict) -> typing.Dict:
        if request_type == 'public':
            headers = {
                'Content-type': 'application/json'
            }

        elif request_type == 'token':
            headers = {
                "accept": "application/json",
                "content-type": "application/json"
            }

        elif request_type == 'private':
            payload['access_token'] = self.public_key
            payload['nonce'] = str(uuid.uuid4())
            dumped_json = json.dumps(payload)
            encoded_payload = base64.b64encode(bytes(dumped_json, 'utf-8'))
            signature = hmac.new(bytes(self.private_key, 'utf-8'), encoded_payload, hashlib.sha512).hexdigest()

            headers = {
                'Content-type': 'application/json',
                'X-COINONE-PAYLOAD': encoded_payload,
                'X-COINONE-SIGNATURE': signature,
            }

            return headers

    def FUNC_Make_Requests(self, method: str, endpoint: str, data: typing.Dict, headers: typing.Dict):
        if method == 'GET':
            try:
                response = requests.get(self.api_url + endpoint,
                                        params=data,
                                        headers=headers)
            except Exception as e:
                logger.error('Connection error while making %s request to %s %s', method, endpoint, e)
                return None

        elif method == 'POST':
            try:
                response = requests.post(self.api_url + endpoint,
                                         params=data,
                                         headers=headers)
                self.res = response

            except Exception as e:
                logger.error('Connection error while making %s request to %s %s', method, endpoint, e)
                return None

        elif method == 'DELETE':
            try:
                response = requests.delete(self.api_url + endpoint,
                                           params=data,
                                           headers=headers)
            except Exception as e:
                logger.error('Connection error while making %s request to %s %s', method, endpoint, e)
                return None

        else:
            raise ValueError()

        if response.status_code in [200, 201]:
            return response.json()
        else:
            logger.error('Error While making %s requests to %s: (ErrorCode %s)',
                         method, endpoint, response.status_code)
            return None

    def PUBLIC_GET_InfoRangeUnits(self):
        request_type = 'public'
        data = dict()

        header = self.FUNC_Generate_Signature(request_type=request_type, payload=data)
        result = self.FUNC_Make_Requests('GET', '/public/v2/range_units', data, header)

        return result['range_price_units']

    def PUBLIC_GET_InfoMarketsByQuoteCurrency(self, quote_currency: str):
        request_type = 'public'
        data = dict()

        header = self.FUNC_Generate_Signature(request_type=request_type, payload=data)
        result = self.FUNC_Make_Requests('GET', '/public/v2/markets' + '/' + quote_currency, data, header)

        return result['markets']

    def PUBLIC_GET_InfoTickerByQuoteCurrency(self, quote_currency: str):
        request_type = 'public'
        data = dict()

        header = self.FUNC_Generate_Signature(request_type=request_type, payload=data)
        result = self.FUNC_Make_Requests('GET', '/public/v2/ticker_new' + '/' + quote_currency, data, header)

        return result['tickers']

    def PUBLIC_GET_InfoCurrencies(self):
        request_type = 'public'
        data = dict()

        header = self.FUNC_Generate_Signature(request_type=request_type, payload=data)
        result = self.FUNC_Make_Requests('GET', '/public/v2/currencies', data, header)

        return result['currencies']

    def ACCOUNT_POST_Balance_All(self):
        request_type = 'private'
        data = dict()

        header = self.FUNC_Generate_Signature(request_type=request_type, payload=data)
        result = self.FUNC_Make_Requests('POST', '/v2.1/account/balance/all', data, header)

        return result['balances']

    def ACCOUNT_POST_TradeFee(self):
        request_type = 'private'
        data = dict()

        header = self.FUNC_Generate_Signature(request_type=request_type, payload=data)
        result = self.FUNC_Make_Requests('POST', '/v2.1/account/trade_fee', data, header)

        return result['fee_rates']

    # def KEYS_POST_RefreshToken(self):
    #     # TODO: 왜 안되는지는 모르겠지만 안됨... 코인원 토큰은 30일마다 업데이트가 필요함!
    #     request_type = 'token'
    #     data = dict()
    #
    #     header = self.FUNC_Generate_Signature(request_type=request_type, payload=data)
    #     result = self.FUNC_Make_Requests('POST', '/oauth/refresh_token', data, header)
    #
    #     return result

    def CRAWLING_Network_and_Fee(self):
        # driver = self.params.chrome_driver
        # driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
        # CHROME_DRIVER_PATH = 'C://Users//cydki//.wdm//drivers//chromedriver//win64//122.0.6261.128//chromedriver-win32//chromedriver.exe'
        # service = Service(executable_path=CHROME_DRIVER_PATH)
        options = webdriver.ChromeOptions()
        options.add_argument("headless")
        # driver = webdriver.Chrome(service=service, options=options)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        driver.get(
            'https://support.coinone.co.kr/support/solutions/articles/31000163237-%EC%BD%94%EC%9D%B8%EC%9B%90%EC%97%90%EC%84%9C-%EC%A7%80%EC%9B%90-%EC%A4%91%EC%9D%B8-%EA%B0%80%EC%83%81%EC%9E%90%EC%82%B0-%EC%A2%85%EB%A5%98-%EB%B0%8F-%EB%84%A4%ED%8A%B8%EC%9B%8C%ED%81%AC-%EC%9C%A0%ED%98%95')

        success_df_status = False

        while success_df_status == False:
            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                table_html = str(soup.find('table'))

                df = pd.read_html(StringIO(table_html))[0]
                df = df.iloc[1:]
                df = df.rename(columns={0: 'name_KOR', 1: 'ticker_base', 2: 'network'})
                df = df[['ticker_base', 'network']]

                if df.shape[0] <= 1:
                    success_df_status = False
                    time.sleep(1)

                else:
                    success_df_status = True

            except:
                time.sleep(1)
                success_df_status = False

        driver.quit()

        return df

if __name__ == '__main__':
    logger_main = LogStreamer()
    params = Parameters()

    client_coinone = ClientCoinone(params=params)

    # result_balance = \
    #     client_coinone.ACCOUNT_POST_Balance_All()
    # result_fee = \
    #     client_coinone.ACCOUNT_POST_TradeFee()
    # result_range_unit = \
    #     client_coinone.PUBLIC_GET_InfoRangeUnits()

    # result_InfoMarketsByQuoteCurrency = \
    #     client_coinone.PUBLIC_GET_InfoMarketsByQuoteCurrency(quote_currency='KRW')
    # result_InfoTickerByQuoteCurrency = \
    #     client_coinone.PUBLIC_GET_InfoTickerByQuoteCurrency(quote_currency='KRW')
    # result_InfoCurrencies = \
    #     client_coinone.PUBLIC_GET_InfoCurrencies()

    # result_RefreshToken = \
    #     client_coinone.KEYS_POST_RefreshToken()
