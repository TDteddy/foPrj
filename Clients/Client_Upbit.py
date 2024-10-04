import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

class ClientUpbit:
    def __init__(self, params):
        self.params = params
        self.api_url = "https://api.upbit.com"
        self.Init_Data()

    def Init_Data(self):
        self.CRAWLING_Network_and_Fee()

    def CRAWLING_Network_and_Fee(self):
        # 크롬 옵션 설정 (헤드리스 모드)
        options = webdriver.ChromeOptions()
        options.add_argument("headless")  # 브라우저를 띄우지 않고 백그라운드에서 실행
        options.add_argument("no-sandbox")
        options.add_argument("disable-dev-shm-usage")
        options.add_argument("disable-gpu")

        # 웹드라이버 설정 (ChromeDriverManager를 통해 드라이버 자동 설치)
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

        # 크롤링할 페이지로 이동
        driver.get('https://upbit.com/service_center/guide')

        success_df_status = False

        while not success_df_status:
            try:
                # HTML 테이블 데이터 가져오기
                table = driver.find_element(By.CLASS_NAME, 'css-1fwzubj')
                table_html = table.get_attribute('outerHTML')

                # BeautifulSoup을 사용하여 테이블 파싱
                soup = BeautifulSoup(table_html, features="lxml")
                headers = [header.text for header in soup.find_all('th')]

                # 테이블의 행 추출
                rows = []
                for tr in soup.find('tbody').find_all('tr'):
                    row_data = []
                    div_count = 0
                    for td in tr.find_all('td'):
                        divs = td.find_all('div')
                        if divs:
                            row_data.append([div.text for div in divs])
                            div_count = max(div_count, len(divs))
                        else:
                            row_data.append([td.text])

                    # 행 데이터 확장
                    for i in range(div_count):
                        row = []
                        for data in row_data:
                            if i < len(data):
                                row.append(data[i])
                            else:
                                row.append(data[0])
                        rows.append(row)

                # 데이터프레임 생성
                df = pd.DataFrame(rows, columns=headers)
                df.columns = ['ticker_base', 'network_name', 'minAmt_withdraw', 'fee_deposit', 'fee_withdraw']

                # 데이터프레임이 비어있는 경우 재시도
                if df.shape[0] <= 1:
                    success_df_status = False
                    time.sleep(1)
                else:
                    success_df_status = True

            except Exception as e:
                print(f"Error while crawling: {e}")
                time.sleep(1)
                success_df_status = False

        driver.quit()

        # 데이터 처리
        for column in df.columns:
            if column not in ['ticker_base', 'network_name']:
                df[column] = df[column].str.replace(',', '')
                df.loc[df[column] == '무료', column] = 0
                df.loc[df[column] == '-', column] = 0
                df[column] = df[column].astype(float)

        self.df_network_and_fee = df
        print("Data successfully crawled and processed")

if __name__ == '__main__':
    # 테스트용
    params = {}
    client_upbit = ClientUpbit(params=params)
    print(client_upbit.df_network_and_fee)
