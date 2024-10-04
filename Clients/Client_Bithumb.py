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

class ClientBithumb:
    def __init__(self, params: Parameters):

        self.exchange = 'BITHUMB'
        self.params = params
        self.dict_params = params.dict_params

        self.public_key = self.dict_params[self.exchange]['public_key']
        self.private_key = self.dict_params[self.exchange]['private_key']

        self.api_url = self.dict_params[self.exchange]['api_url']
        self.Init_Data()

        logger.info('[' + self.exchange + '] Client Successfully Initialized!!!')

    def Init_Data(self):
        dict_assetStatus = self.PUBLIC_GET_AssetStatusAll()
        df_assetStatus = pd.DataFrame.from_dict(dict_assetStatus).T.reset_index().rename(columns={'index': 'ticker'})

        #################################################################################################

        self.df_wallet_info = self.CRAWLING_Network_and_Fee()

        #################################################################################################

        ls_quote = ['KRW', 'BTC']
        ls_temp = []

        for quote_one in ls_quote:
            dict_price_one = self.PUBLIC_GET_TickerAllbyQuote(quote_one)
            del dict_price_one['date']

            df_one = pd.DataFrame.from_dict(dict_price_one, orient='index')
            df_one['ticker_quote'] = quote_one
            ls_temp.append(df_one)

        df_tickers = pd.concat(ls_temp)
        df_tickers = df_tickers.reset_index()

        df_tickers = df_tickers.rename(
            columns={'index': 'ticker_base'})
        df_tickers['ticker'] = df_tickers['ticker_base'].str.upper() + '_' + df_tickers['ticker_quote'].str.upper()
        df_tickers['status_trade'] = True
        df_tickers = df_tickers[['ticker', 'ticker_base', 'ticker_quote', 'status_trade']]
        self.df_tickers = df_tickers

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
        df_tickers = df_tickers.loc[(df_tickers['status_trade'] == True)]

        for quote in ls_quote:
            dict_tickers['TICKERS'][quote] = \
                df_tickers.loc[df_tickers['ticker_quote'] == quote]['ticker_base'].to_list()

        dict_tickers['TICKERS_UNION'] = ls_tickers_union
        dict_tickers['TICKERS_INTERSECT'] = ls_tickers_intersect
        dict_tickers['DEPOSIT'] = ls_tickers_deposit
        dict_tickers['WITHDRAW'] = ls_tickers_withdraw

        self.dict_tickers = dict_tickers

        #################################################################################################

        # df_prices_KRW = pd.DataFrame.from_dict(dict_prices_KRW).T.reset_index().rename(columns={'index': 'ticker'})
        # df_prices_BTC = pd.DataFrame.from_dict(dict_prices_BTC).T.reset_index().rename(columns={'index': 'ticker'})
        # df_prices_KRW = df_prices_KRW.loc[df_prices_KRW['ticker'] != 'date']
        # df_prices_BTC = df_prices_BTC.loc[df_prices_BTC['ticker'] != 'date']
        #
        # ls_tickers_KRW_base = list(df_prices_KRW['ticker'].unique())
        # ls_tickers_BTC_base = list(df_prices_BTC['ticker'].unique())
        # ls_tickers_withdraw_raw = df_assetStatus.loc[df_assetStatus['withdrawal_status'] == 1]['ticker'].to_list()
        # ls_tickers_deposit_raw = df_assetStatus.loc[df_assetStatus['deposit_status'] == 1]['ticker'].to_list()
        #
        # ls_tickers_KRW = ls_tickers_KRW_base
        # ls_tickers_BTC = ls_tickers_BTC_base
        #
        # ls_tickers_all = list(set(ls_tickers_KRW) | set(ls_tickers_BTC))
        # ls_tickers_withdraw = list(set(ls_tickers_KRW_base) & set(ls_tickers_withdraw_raw) & set(ls_tickers_all))
        # ls_tickers_deposit = list(set(ls_tickers_KRW_base) & set(ls_tickers_deposit_raw) & set(ls_tickers_all))
        # ls_ticker_union = list(set(ls_tickers_withdraw) | set(ls_tickers_deposit))
        # ls_ticker_intersect = list(set(ls_tickers_withdraw) & set(ls_tickers_deposit))
        #
        # self.dict_tickers = dict()
        # self.dict_tickers['TICKERS'] = dict()
        # self.dict_tickers['TICKERS']['KRW'] = ls_tickers_KRW
        # self.dict_tickers['TICKERS']['BTC'] = ls_tickers_BTC
        #
        # self.dict_tickers['TICKERS_UNION'] = ls_ticker_union
        # self.dict_tickers['TICKERS_INTERSECT'] = ls_ticker_intersect
        # self.dict_tickers['WITHDRAW'] = ls_tickers_withdraw
        # self.dict_tickers['DEPOSIT'] = ls_tickers_deposit

    def FUNC_Generate_Signature(self, request_type: str, endpoint: str, payload: typing.Dict) -> typing.Dict:
        if request_type == 'public':
            headers = {
                "accept": "application/json"
            }

        elif request_type == 'private':
            nonce = str(int(time.time()*1000))
            query_string = endpoint + chr(0) + urllib.parse.urlencode(payload) + chr(0) + nonce
            utf8_data = query_string.encode('utf-8')

            key = self.private_key
            utf8_key = key.encode('utf-8')

            h = hmac.new(bytes(utf8_key), utf8_data, hashlib.sha512)
            hex_output = h.hexdigest()
            utf8_hex_output = hex_output.encode('utf-8')
            api_sign = base64.b64encode(utf8_hex_output)
            utf8_api_sign = api_sign.decode('utf-8')

            headers = {
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
                "Api-Key": self.public_key,
                "Api-Nonce": nonce,
                "Api-Sign": utf8_api_sign
            }

        return headers

    def FUNC_Make_Requests(self, method: str, endpoint: str, data: typing.Dict, headers: typing.Dict):
        if method == 'GET':
            try:
                response = requests.get(self.api_url + endpoint,
                                        data=data,
                                        headers=headers)
            except Exception as e:
                logger.error('Connection error while making %s request to %s %s', method, endpoint, e)
                return None

        elif method == 'POST':
            try:
                response = requests.post(self.api_url + endpoint,
                                         data=data,
                                         headers=headers)
            except Exception as e:
                logger.error('Connection error while making %s request to %s %s', method, endpoint, e)
                return None

        elif method == 'DELETE':
            try:
                response = requests.delete(self.api_url + endpoint,
                                           data=data,
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

    def get_tick_size(self, price, method="floor"):

        """원화마켓 주문 가격 단위

        Args:
            price (float]): 주문 가격
            method (str, optional): 주문 가격 계산 방식. Defaults to "floor".

        Returns:
            float: 업비트 원화 마켓 주문 가격 단위로 조정된 가격
        """

        if method == "floor":
            func = math.floor
        elif method == "round":
            func = round
        else:
            func = math.ceil

        if price >= 1000000:
            price_adj = func(price / 1000) * 1000
        elif price >= 500000:
            price_adj = func(price / 500) * 500
        elif price >= 100000:
            price_adj = func(price / 100) * 100
        elif price >= 50000:
            price_adj = func(price / 50) * 50
        elif price >= 10000:
            price_adj = func(price / 10) * 10
        elif price >= 5000:
            price_adj = func(price / 5) * 5
        elif price >= 1000:
            price_adj = func(price / 1) * 1
        elif price >= 100:
            price_adj = func(price / 1) * 1
        elif price >= 10:
            price_adj = func(price / 0.01) / 100
        elif price >= 1:
            price_adj = func(price / 0.001) / 1000
        else:
            price_adj = func(price / 0.0001) / 10000

        return price_adj

    def PUBLIC_GET_AssetStatusAll(self):
        request_type = 'public'
        endpoint = '/public/assetsstatus/ALL'

        data = dict()

        header = self.FUNC_Generate_Signature(request_type=request_type, endpoint=None, payload=None)
        result = self.FUNC_Make_Requests('GET', endpoint, data, header)

        if result['status'] == '0000':
            return result['data']
        else:
            return None

    def PUBLIC_GET_TickerAllbyQuote(self, quote: str):
        request_type = 'public'
        endpoint = '/public/ticker/ALL_' + quote

        data = dict()

        header = self.FUNC_Generate_Signature(request_type=request_type, endpoint=None, payload=None)
        result = self.FUNC_Make_Requests('GET', endpoint, data, header)

        if result['status'] == '0000':
            return result['data']
        else:
            return None

    def INFO_POST_Balance(self):
        request_type = 'private'
        endpoint = '/info/balance'

        data = dict()
        data['endpoint'] = endpoint
        data['order_currency'] = 'ALL'

        header = self.FUNC_Generate_Signature(request_type=request_type, endpoint=endpoint, payload=data)
        result = self.FUNC_Make_Requests('POST', endpoint, data, header)

        if result['status'] == '0000':
            return result['data']
        else:
            return None

    def INFO_POST_WalletAddress(self, ticker_base):
        request_type = 'private'
        endpoint = '/info/wallet_address'

        data = dict()
        data['endpoint'] = endpoint
        data['currency'] = ticker_base

        header = self.FUNC_Generate_Signature(request_type=request_type, endpoint=endpoint, payload=data)
        result = self.FUNC_Make_Requests('POST', endpoint, data, header)

        if result['status'] == '0000':
            return result['data']
        else:
            return None

    def CRAWLING_Network_and_Fee(self):
        #################################################################################################
        # driver = self.params.chrome_driver
        # driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))
        # CHROME_DRIVER_PATH = 'C://Users//cydki//.wdm//drivers//chromedriver//win64//122.0.6261.128//chromedriver-win32//chromedriver.exe'
        # service = Service(executable_path=CHROME_DRIVER_PATH)
        options = webdriver.ChromeOptions()
        # options.add_argument("headless")
        # driver = webdriver.Chrome(service=service, options=options)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        driver.get('https://www.bithumb.com/react/info/fee/inout')

        success_df_fee = False

        while success_df_fee == False:
            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                table_html = str(soup.find('table', {'class': 'InoutFee_inout-fee-table__Ak+jZ'}))
                df_fee = pd.read_html(StringIO(table_html))[0]

                if df_fee.shape[0] <= 1:
                    success_df_fee = False
                    time.sleep(1)

                else:
                    success_df_fee = True
            except:
                time.sleep(1)
                success_df_fee = False

        df_fee.columns = ['ticker', 'network', 'fee_deposit', 'fee_withdraw']
        df_fee['ticker'] = df_fee['ticker'].str.split('(', expand=True)[1]
        df_fee['ticker'] = df_fee['ticker'].str.replace(')', '', regex=False)
        df_fee = df_fee.loc[df_fee['ticker'] != 'KRW']
        df_fee = df_fee.sort_values('fee_deposit')
        df_fee.loc[(df_fee['fee_deposit'].str.find('무료') != -1), 'fee_deposit'] = '무료'
        df_fee.loc[(df_fee['fee_withdraw'].str.find('무료') != -1), 'fee_withdraw'] = '무료'

        for column in ['fee_deposit', 'fee_withdraw']:
            df_fee.loc[df_fee[column] == '무료', column] = 0
            df_fee.loc[df_fee[column] == '무료소액입금수수료 부과', column] = 0
            df_fee.loc[df_fee[column] == '무료소액입금 수수료 부과', column] = 0
            df_fee.loc[df_fee[column] == '-', column] = 0
            df_fee[column] = df_fee[column].astype(float)

        #################################################################################################

        driver.get('https://www.bithumb.com/react/info/inout-condition')

        success_df_status = False

        while success_df_status == False:
            try:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                table_html = str(soup.find('table', {'class': 'InoutCondition_inout-condition-table__tWVtr'}))
                df_status = pd.read_html(StringIO(table_html))[0]

                if df_status.shape[0] <= 1:
                    success_df_status = False
                    time.sleep(1)

                else:
                    success_df_status = True

            except:
                time.sleep(1)
                success_df_status = False

        driver.quit()

        df_status.columns = ['ticker', 'network', 'num_confirm', 'status_deposit', 'status_withdraw', 'reason',
                             'option']
        df_status['ticker'] = df_status['ticker'].str.split('(', expand=True)[1]
        df_status['ticker'] = df_status['ticker'].str.replace(')', '', regex=False)
        df_status['num_confirm'] = df_status['num_confirm'].astype(float)

        df_status.loc[df_status['status_deposit'] == '정상', 'status_deposit'] = True
        df_status.loc[df_status['status_deposit'] != True, 'status_deposit'] = False
        df_status.loc[df_status['status_withdraw'] == '정상', 'status_withdraw'] = True
        df_status.loc[df_status['status_withdraw'] != True, 'status_withdraw'] = False

        #################################################################################################

        df_wallet = pd.merge(df_fee, df_status, how='outer', on=['ticker', 'network'])
        df_wallet = df_wallet.rename(
            columns={'ticker': 'ticker_base',
                     'network': 'network_name',
                     'num_confirm': 'confirm_count'})

        check_unique_network = df_wallet.groupby('ticker_base')['network_name'].count()
        check_unique_network = check_unique_network.reset_index().rename(columns={'network_name': 'check'})
        check_unique_network = check_unique_network.loc[check_unique_network['check'] > 1]

        if check_unique_network.shape[0] != 0:
            df_wallet['FLAG'] = df_wallet.groupby('ticker_base')['fee_withdraw'].transform('min') == df_wallet[
                'fee_withdraw']
            df_wallet = df_wallet.loc[df_wallet['FLAG'] == True].drop(columns=['FLAG'])

            check_unique_network = df_wallet.groupby('ticker_base')['network_name'].count()
            check_unique_network = check_unique_network.reset_index().rename(columns={'network_name': 'check'})
            check_unique_network = check_unique_network.loc[check_unique_network['check'] > 1]

            if check_unique_network.shape[0] != 0:
                print(check_unique_network)

        #################################################################################################

        df_wallet['network'] = None # TODO: 하드코딩 파트 // 바이낸스 기준으로 통일 시키기 // 칼럼: network
        df_wallet.loc[df_wallet['network_name'] == 'Ethereum', 'network'] = 'ETH'
        df_wallet.loc[df_wallet['network_name'] == 'Binance Smart Chain', 'network'] = 'BSC'
        df_wallet.loc[df_wallet['network_name'] == 'Klaytn', 'network'] = 'KLAY'
        df_wallet.loc[df_wallet['network_name'] == 'EOS', 'network'] = 'EOS'
        df_wallet.loc[df_wallet['network_name'] == 'Tron', 'network'] = 'TRX'
        df_wallet.loc[df_wallet['network_name'] == 'Arbitrum One', 'network'] = 'ARBITRUM'
        df_wallet.loc[df_wallet['network_name'] == 'Solana', 'network'] = 'SOL'
        df_wallet.loc[df_wallet['network_name'] == 'Luniverse', 'network'] = 'LMT'
        df_wallet.loc[df_wallet['network_name'] == 'Optimism', 'network'] = 'OPTIMISM'
        df_wallet.loc[df_wallet['network_name'] == 'Avalanche C-Chain', 'network'] = 'AVAXC'
        df_wallet.loc[df_wallet['network_name'] == 'Ontology', 'network'] = 'ONT'
        df_wallet.loc[df_wallet['network_name'] == 'Polygon', 'network'] = 'MATIC'
        df_wallet.loc[df_wallet['network_name'] == 'Stellar', 'network'] = 'XLM'
        df_wallet.loc[df_wallet['network_name'] == 'Theta Network', 'network'] = 'THETA'
        df_wallet.loc[df_wallet['network_name'] == 'XRP', 'network'] = 'XRP'
        df_wallet.loc[df_wallet['network_name'] == 'IOST', 'network'] = 'IOST'
        df_wallet.loc[df_wallet['network_name'] == 'Neo', 'network'] = 'NEO'
        df_wallet.loc[df_wallet['network_name'] == 'Qtum', 'network'] = 'QTUM'
        df_wallet.loc[df_wallet['network_name'] == 'Stacks', 'network'] = 'STX'
        df_wallet.loc[df_wallet['network_name'] == 'Waves', 'network'] = 'WAVES'
        df_wallet.loc[df_wallet['network_name'] == 'aelf', 'network'] = 'ELF'
        df_wallet.loc[df_wallet['network_name'] == 'Aergo', 'network'] = 'AERGO'
        df_wallet.loc[df_wallet['network_name'] == 'Algorand', 'network'] = 'ALGO'
        df_wallet.loc[df_wallet['network_name'] == 'Aptos', 'network'] = 'APT'
        df_wallet.loc[df_wallet['network_name'] == 'Ark', 'network'] = 'ARK'
        df_wallet.loc[df_wallet['network_name'] == 'Astar', 'network'] = 'ASTR'
        df_wallet.loc[df_wallet['network_name'] == 'Bifrost', 'network'] = 'BFC'
        df_wallet.loc[df_wallet['network_name'] == 'Bitcoin', 'network'] = 'BTC'
        df_wallet.loc[df_wallet['network_name'] == 'Bitcoin Cash', 'network'] = 'BCH'
        df_wallet.loc[df_wallet['network_name'] == 'Bitcoin Gold', 'network'] = 'BTG'
        df_wallet.loc[df_wallet['network_name'] == 'Bitcoin SV', 'network'] = 'BSV'
        df_wallet.loc[df_wallet['network_name'] == 'BNB', 'network'] = 'BNB'
        df_wallet.loc[df_wallet['network_name'] == 'Cardano', 'network'] = 'ADA'
        df_wallet.loc[df_wallet['network_name'] == 'Casper', 'network'] = 'CSPR'
        df_wallet.loc[df_wallet['network_name'] == 'Celestia', 'network'] = 'TIA'
        df_wallet.loc[df_wallet['network_name'] == 'Celo', 'network'] = 'CELO'
        df_wallet.loc[df_wallet['network_name'] == 'Chiliz Chain', 'network'] = 'CHZ'
        df_wallet.loc[df_wallet['network_name'] == 'Conflux', 'network'] = 'CFXEVM'
        df_wallet.loc[df_wallet['network_name'] == 'Cortex', 'network'] = 'CTXC'
        df_wallet.loc[df_wallet['network_name'] == 'Cosmos', 'network'] = 'ATOM'
        df_wallet.loc[df_wallet['network_name'] == 'Cronos', 'network'] = 'CRO'
        df_wallet.loc[df_wallet['network_name'] == 'Dogecoin', 'network'] = 'DOGE'
        df_wallet.loc[df_wallet['network_name'] == 'eCash', 'network'] = 'XEC'
        df_wallet.loc[df_wallet['network_name'] == 'Enjin', 'network'] = 'ENJ'
        df_wallet.loc[df_wallet['network_name'] == 'Ethereum Classic', 'network'] = 'ETC'
        df_wallet.loc[df_wallet['network_name'] == 'EthereumFair', 'network'] = 'ETHF'
        df_wallet.loc[df_wallet['network_name'] == 'EthereumPoW', 'network'] = 'ETHW'
        df_wallet.loc[df_wallet['network_name'] == 'Fantom', 'network'] = 'FTM'
        df_wallet.loc[df_wallet['network_name'] == 'FINSCHIA', 'network'] = 'FNSA'
        df_wallet.loc[df_wallet['network_name'] == 'FirmaChain', 'network'] = 'FCT2'
        df_wallet.loc[df_wallet['network_name'] == 'Flare', 'network'] = 'FLR'
        df_wallet.loc[df_wallet['network_name'] == 'Flow', 'network'] = 'FLOW'
        df_wallet.loc[df_wallet['network_name'] == 'HBAR', 'network'] = 'HBAR'
        df_wallet.loc[df_wallet['network_name'] == 'Hive', 'network'] = 'HIVE'
        df_wallet.loc[df_wallet['network_name'] == 'ICON', 'network'] = 'ICX'
        df_wallet.loc[df_wallet['network_name'] == 'IoTeX', 'network'] = 'IOTX'
        df_wallet.loc[df_wallet['network_name'] == 'Kava', 'network'] = 'KAVA'
        df_wallet.loc[df_wallet['network_name'] == 'Kusama', 'network'] = 'KSM'
        df_wallet.loc[df_wallet['network_name'] == 'Lisk', 'network'] = 'LSK'
        df_wallet.loc[df_wallet['network_name'] == 'Manta Pacific', 'network'] = 'MANTA'
        df_wallet.loc[df_wallet['network_name'] == 'MediBloc', 'network'] = 'MED'
        df_wallet.loc[df_wallet['network_name'] == 'Metadium', 'network'] = 'META'
        df_wallet.loc[df_wallet['network_name'] == 'MEVerse', 'network'] = 'MEV'
        df_wallet.loc[df_wallet['network_name'] == 'Mina', 'network'] = 'MINA'
        df_wallet.loc[df_wallet['network_name'] == 'MultiverseX', 'network'] = 'EGLD'
        df_wallet.loc[df_wallet['network_name'] == 'Nervos Network', 'network'] = 'CKB'
        df_wallet.loc[df_wallet['network_name'] == 'Oasys', 'network'] = 'OAS'
        df_wallet.loc[df_wallet['network_name'] == 'Osmosis', 'network'] = 'OSMO'
        df_wallet.loc[df_wallet['network_name'] == 'Polkadot', 'network'] = 'DOT'
        df_wallet.loc[df_wallet['network_name'] == 'Proton', 'network'] = 'XPR'
        df_wallet.loc[df_wallet['network_name'] == 'Raven', 'network'] = 'RVN'
        df_wallet.loc[df_wallet['network_name'] == 'REI Network', 'network'] = 'REI'
        df_wallet.loc[df_wallet['network_name'] == 'Sei Network', 'network'] = 'SEI'
        df_wallet.loc[df_wallet['network_name'] == 'Shentu', 'network'] = 'CTK'
        df_wallet.loc[df_wallet['network_name'] == 'Snow', 'network'] = 'ICZ'
        df_wallet.loc[df_wallet['network_name'] == 'Solar', 'network'] = 'SXP'
        df_wallet.loc[df_wallet['network_name'] == 'Songbird', 'network'] = 'SGB'
        df_wallet.loc[df_wallet['network_name'] == 'Steem', 'network'] = 'STEEM'
        df_wallet.loc[df_wallet['network_name'] == 'Stratis', 'network'] = 'STRAX'
        df_wallet.loc[df_wallet['network_name'] == 'Sui', 'network'] = 'SUI'
        df_wallet.loc[df_wallet['network_name'] == 'Terra', 'network'] = 'LUNA2'
        df_wallet.loc[df_wallet['network_name'] == 'Tezos', 'network'] = 'XTZ'
        df_wallet.loc[df_wallet['network_name'] == 'VeChain', 'network'] = 'VET'
        df_wallet.loc[df_wallet['network_name'] == 'WAX', 'network'] = 'WAX'
        df_wallet.loc[df_wallet['network_name'] == 'WEMIX', 'network'] = 'WEMIX'
        df_wallet.loc[df_wallet['network_name'] == 'XPLA', 'network'] = 'XPLA'
        df_wallet.loc[df_wallet['network_name'] == 'Zilliqa', 'network'] = 'ZIL'

        df_wallet['network_type'] = df_wallet['network_name']
        df_wallet['minAmt_withdraw'] = None
        df_wallet['max_precision'] = None

        self.df_wallet_info = df_wallet.copy()

        self.df_wallet_info = \
            self.df_wallet_info[['ticker_base',
                                 'network_name', 'network_type', 'network',
                                 'status_deposit', 'status_withdraw', 'fee_deposit', 'fee_withdraw',
                                 'minAmt_withdraw', 'max_precision', 'confirm_count']]

        return self.df_wallet_info

if __name__ == '__main__':
    logger_main = LogStreamer()
    params = Parameters()

    client_bithumb = ClientBithumb(params=params)
    # res = client_bithumb.INFO_POST_Balance()
    # res = client_bithumb.INFO_POST_WalletAddress('BTC')
    # res = client_bithumb.PUBLIC_GET_AssetStatusAll()
    # res = client_bithumb.PUBLIC_GET_TickerAllbyQuote('BTC')

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
