import pandas as pd
import asyncio
import logging
import multiprocessing as mp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import List

from Clients.__Manager import Manager_Client, Func_Check_Websocket_Queue
from Simulation.Simulator import Simulator
from Strategy.Strategy_Arbitrage_temp import Strategy_Arbitrage
from Websockets.Websocket_DataCollect_Arb import Websocket_DataCollect
from Parameters.Global_Parameters import Parameters
from Websockets.__Manager import Manager_Wesocket

# FastAPI 인스턴스 생성
app = FastAPI()

# ConnectionManager 클래스: 활성 웹소켓 연결 관리
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    # 새로운 웹소켓 연결을 수락하고 관리
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    # 웹소켓 연결이 끊어졌을 때 연결 목록에서 제거
    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    # 개인에게 메시지 전송
    async def send_personal_message(self, message: str, websocket: WebSocket):
        await websocket.send_text(message)

    # 모든 연결된 클라이언트에게 메시지 방송
    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_json(message)

manager = ConnectionManager()

# 실시간 데이터 업데이트를 위한 웹소켓 엔드포인트
@app.websocket("/ws/data")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await asyncio.sleep(1)  # 실시간 데이터 푸시를 시뮬레이션 (1초 간격)
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# Arbitrage 전략 업데이트 및 방송 처리 함수
async def update_arbitrage_result(data: pd.DataFrame):
    message = data.to_dict(orient="records")  # DataFrame을 JSON-like 형식으로 변환
    await manager.broadcast(message)

# Arbitrage 전략 업데이트를 시뮬레이션하는 루프
async def simulate_arbitrage_updates(strategy: Strategy_Arbitrage):
    while True:
        try:
            df_arb_result = strategy.Update_Arbitrage_Result()  # Arbitrage 계산을 시뮬레이션
            await update_arbitrage_result(df_arb_result)
            await asyncio.sleep(1)  # 1초 간격으로 업데이트
        except Exception as e:
            logging.error(f"simulate_arbitrage_updates 에러: {e}")

# Simulator 작업을 시뮬레이션하는 루프
async def simulate_simulator_updates(simulator: Simulator):
    while True:
        try:
            df_simulator_result = simulator.Update_Simulator_Result()  # Simulator 업데이트를 시뮬레이션
            await update_arbitrage_result(df_simulator_result)
            await asyncio.sleep(1)
        except Exception as e:
            logging.error(f"simulate_simulator_updates 에러: {e}")

# Arbitrage와 Simulator 백그라운드 작업 시작
def start_background_tasks(strategy: Strategy_Arbitrage, simulator: Simulator):
    asyncio.run(simulate_arbitrage_updates(strategy))
    asyncio.run(simulate_simulator_updates(simulator))

# FastAPI 서버 및 백그라운드 프로세스 시작
if __name__ == '__main__':
    logger_main = logging.getLogger()

    # 파라미터 설정
    params = Parameters()
    manager_client = Manager_Client(params=params)
    manager_ws = Manager_Wesocket(params=params, manager_client=manager_client)

    # 웹소켓 큐 상태 확인
    while True:
        flag_ws = Func_Check_Websocket_Queue(params, manager_ws)
        if flag_ws:
            logger_main.info('[WEBSOCKET] 데이터가 큐에 있습니다!')
            break
        else:
            logger_main.info('[WEBSOCKET] 데이터가 큐에 없습니다!')
            asyncio.sleep(1)

    # 데이터 수집 프로세스 시작
    data_collection = Websocket_DataCollect(params=params, manager_client=manager_client, manager_ws=manager_ws)
    p = mp.Process(target=data_collection.Init_DataCollect)
    p.start()

    # Arbitrage 전략 백그라운드 프로세스 시작
    strategy = Strategy_Arbitrage(params=params, data_collect=data_collection)
    pp = mp.Process(target=start_background_tasks, args=(strategy, Simulator(params=params)))
    pp.start()

    # FastAPI 서버 시작
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

    # 종료 시 프로세스 정리
    p.terminate()
    p.join()
    pp.terminate()
    pp.join()
