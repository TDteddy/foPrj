import base64
import hashlib
import hmac
import urllib

import json

import requests
import logging
import time
import typing
import math

from io import StringIO
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

import pandas as pd

from Parameters.Global_Parameters import Parameters
from Parameters.Func_Logger import LogStreamer

logger = logging.getLogger()

class ClientKorbit:
    def __init__(self, params: Parameters):

        self.exchange = 'KORBIT'
        self.params = params
        self.dict_params = params.dict_params

        self.public_key = self.dict_params[self.exchange]['public_key']
        self.private_key = self.dict_params[self.exchange]['private_key']

        self.api_url = self.dict_params[self.exchange]['api_url']

        self.access_token = None
        self.refresh_token = None
        self.headers = None

        self.Init_Data()

        logger.info('[' + self.exchange + '] Client Successfully Initialized!!!')

    def Init_Data(self):
        self.CRAWLING_Network_and_Fee()

        #################################################################################################

        dict_tickerAll = self.PUBLIC_GET_TickerAll()
        df_tickerAll = pd.DataFrame.from_dict(dict_tickerAll).T.reset_index().rename(columns={'index': 'ticker'})

        self.df_tickers = df_tickerAll
        self.df_tickers['ticker_base'] = self.df_tickers['ticker'].map(lambda row: row.split('_')[0]).str.upper()
        self.df_tickers['ticker_quote'] = self.df_tickers['ticker'].map(lambda row: row.split('_')[1]).str.upper()
        self.df_tickers['status_trade'] = True
        self.df_tickers = self.df_tickers[['ticker', 'ticker_base', 'ticker_quote', 'status_trade']]

        #################################################################################################

        ls_tickers_KRW_base = self.df_tickers['ticker_base'].to_list()

        self.dict_tickers = dict()
        self.dict_tickers['TICKERS'] = dict()
        self.dict_tickers['TICKERS']['KRW'] = ls_tickers_KRW_base

        self.dict_tickers['TICKERS_UNION'] = ls_tickers_KRW_base
        self.dict_tickers['TICKERS_INTERSECT'] = ls_tickers_KRW_base

    def FUNC_Make_Requests(self, method: str, endpoint: str, data: typing.Dict, params: typing.Dict, headers: typing.Dict):
        if method == 'GET':
            try:
                response = requests.get(self.api_url + endpoint,
                                        data=data,
                                        params=params,
                                        headers=headers)
            except Exception as e:
                logger.error('Connection error while making %s request to %s %s', method, endpoint, e)
                return None

        elif method == 'POST':
            try:
                response = requests.post(self.api_url + endpoint,
                                         data=data,
                                         params=params,
                                         headers=headers)
            except Exception as e:
                logger.error('Connection error while making %s request to %s %s', method, endpoint, e)
                return None

        elif method == 'DELETE':
            try:
                response = requests.delete(self.api_url + endpoint,
                                           data=data,
                                           params=params,
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

    def Issue_AccessToken(self):
        """
        access token을 처음 발급하는 메서드
        """
        endpoint = "/v1/oauth2/access_token"

        data = {
            "client_id": self.public_key,
            "client_secret": self.private_key,
            "grant_type": "client_credentials",
        }

        result = self.FUNC_Make_Requests(method='POST', endpoint=endpoint, data=data, headers=None)

        if result != None:
            self.access_token = result.get('access_token')
            self.refresh_token = result.get('refresh_token')
            self.headers = {"Authorization": "Bearer " + self.access_token}

            logger.info('[' + self.exchange + '] AccessToken Issue SUCCESS!!!')
        else:
            logger.error('[' + self.exchange + '] AccessToken Issue FAILED!!!')

    def Refresh_AccessToken(self):
        """
        발급받은 access_token과 refresh_token을 사용해서 access_token을 갱신하는 메서드
        """

        endpoint = "/v1/oauth2/access_token"

        data = {"client_id": self.public_key,
                "client_secret": self.private_key,
                "grant_type": "refresh_token",
                "refresh_token": self.refresh_token}

        result = self.FUNC_Make_Requests(method='POST', endpoint=endpoint, data=data, headers=None)

        if result != None:
            self.access_token = result.get('access_token')
            self.refresh_token = result.get('refresh_token')
            self.headers = {"Authorization": "Bearer " + self.access_token}

            logger.info('[' + self.exchange + '] AccessToken Refresh SUCCESS!!!')
        else:
            logger.error('[' + self.exchange + '] AccessToken Refresh FAILED!!!')

    def PUBLIC_GET_TickerAll(self):
        endpoint = '/v1/ticker/detailed/all'

        result = self.FUNC_Make_Requests(method='GET', endpoint=endpoint, params=None, data=None, headers=None)

        if result != None:
            logger.info('[' + self.exchange + '] PUBLIC_GET_TickerAll SUCCESS!!!')
            return result
        else:
            logger.error('[' + self.exchange + '] PUBLIC_GET_TickerAll FAILED!!!')
            return None

    def INFO_GET_TRV_Fee(self):
        currency_pair = 'all'
        endpoint = '/v1/user/volume'

        data = dict()
        data['currency_pair'] = currency_pair

        result = self.FUNC_Make_Requests(method='GET', endpoint=endpoint, params=None, data=data, headers=self.headers)

        if result != None:
            logger.info('[' + self.exchange + '] INFO_GET_TRV_Fee SUCCESS!!!')
            return result
        else:
            logger.error('[' + self.exchange + '] INFO_GET_TRV_Fee FAILED!!!')
            return None

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
            'https://lightning.korbit.co.kr/faq/list/?category=3zucid8CZBwHlajexgn6KX&article=5SrSC3yggkWhcSL0O1KSz4')

        success_df_status = False

        while success_df_status == False:
            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                table_html = str(soup.find('table'))
                df = pd.read_html(StringIO(table_html))[0]

                if df.shape[0] <= 1:
                    success_df_status = False
                    time.sleep(1)

                else:
                    success_df_status = True

            except:
                time.sleep(1)
                success_df_status = False

        driver.quit()

        df.columns = ['ticker_base', 'network_name', '2nd_address', 'fee_withdraw', 'minAmt_withdraw']
        df['ticker_base'] = df['ticker_base'].str.split('(', expand=True)[0]

        df.loc[df['ticker_base']=='PCI', 'fee_withdraw'] = 20 # TODO: 하드코딩 파트 // 얘만 %단위 출금수수료 있음
        df['fee_withdraw'] = df['fee_withdraw'].astype(float)
        df['minAmt_withdraw'] = df['minAmt_withdraw'].astype(float)
        df.loc[df['2nd_address'] == '-', '2nd_address'] = None

        df['status_deposit'] = None
        df['status_withdraw'] = None
        df['fee_deposit'] = 0
        df['network_type'] = None
        df['max_precision'] = None
        df['confirm_count'] = None

        df['network'] = None  # TODO: 하드코딩 파트 // 바이낸스 기준으로 통일 시키기 // 칼럼: network
        df.loc[df['network_name'] == 'Ethereum', 'network'] = 'ETH'
        df.loc[df['network_name'] == 'Klaytn', 'network'] = 'KLAY'
        df.loc[df['network_name'] == 'Klaytn (KCT token)', 'network'] = 'KLAY'
        df.loc[df['network_name'] == 'Optimism', 'network'] = 'OPTIMISM'
        df.loc[df['network_name'] == 'OPMainnet', 'network'] = 'OPTIMISM'
        df.loc[df['network_name'] == 'Solana', 'network'] = 'SOL'
        df.loc[df['network_name'] == 'XRPL', 'network'] = 'XRP'
        df.loc[df['network_name'] == 'Arbitrum', 'network'] = 'ARBITRUM'
        df.loc[df['network_name'] == 'Stellar', 'network'] = 'XLM'
        df.loc[df['network_name'] == 'Tron', 'network'] = 'TRX'
        df.loc[df['network_name'] == 'Songbird', 'network'] = 'SGB'
        df.loc[df['network_name'] == 'Theta', 'network'] = 'THETA'
        df.loc[df['network_name'] == 'AVAX C-Chain', 'network'] = 'AVAXC'
        df.loc[df['network_name'] == 'Wax', 'network'] = 'WAX'
        df.loc[df['network_name'] == 'Chiliz', 'network'] = 'CHZ2'
        df.loc[df['network'].isnull(), 'network'] = \
            df.loc[df['network'].isnull(), 'ticker_base']

        df = df[['ticker_base',
                 'network_name', 'network_type', 'network', '2nd_address',
                 'status_deposit', 'status_withdraw', 'fee_deposit', 'fee_withdraw',
                 'minAmt_withdraw', 'max_precision', 'confirm_count']]

        check_unique_network = df.groupby('ticker_base')['network_name'].count()
        check_unique_network = check_unique_network.reset_index().rename(columns={'network_name': 'check'})
        check_unique_network = check_unique_network.loc[check_unique_network['check'] > 1]

        if check_unique_network.shape[0] != 0:
            df['FLAG'] = df.groupby('ticker_base')['fee_withdraw'].transform('min') == df['fee_withdraw']
            df = df.loc[df['FLAG'] == True].drop(columns=['FLAG'])

            check_unique_network = df.groupby('ticker_base')['network_name'].count()
            check_unique_network = check_unique_network.reset_index().rename(columns={'network_name': 'check'})
            check_unique_network = check_unique_network.loc[check_unique_network['check'] > 1]

            if check_unique_network.shape[0] != 0:
                print(check_unique_network)

        self.df_wallet_info = df

        return df




if __name__ == '__main__':
    logger_main = LogStreamer()
    params = Parameters()

    client_korbit = ClientKorbit(params=params)
    # client_korbit.Issue_AccessToken()
    # client_korbit.Refresh_AccessToken()
    # x = client_korbit.INFO_GET_TRV_Fee()


    # endpoint = '/v1/orderbook'
    # data = dict()
    # data['currency_pair'] = ['xrp_krw', 'btc_krw']
    # # result = client_korbit.FUNC_Make_Requests('GET', endpoint, data=data, headers=None)
    # result = requests.get(client_korbit.api_url + endpoint, params=data, headers=None).json()

    endpoint = '/v1/transactions'
    data = dict()
    data['currency_pair'] = ['btc_krw']
    data['time'] = 'day'
    # result = client_korbit.FUNC_Make_Requests('GET', endpoint, data=data, headers=None)
    result = requests.get(client_korbit.api_url + endpoint, params=data, headers=None).json()
    df_result = pd.DataFrame(result)