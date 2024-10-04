import logging
import typing
from fastapi import FastAPI, WebSocket
import asyncio
#from PyQt5 import QtWidgets

#class ListHandler(logging.Handler):
#    def __init__(self, history: typing.List):
#        super().__init__()
#        self.history = history

#    def emit(self, record: logging.LogRecord):
#        self.history.append({'log': record.asctime + ' : ' + record.getMessage(), 'displayed': False})


#class textBrowserHandler(logging.Handler):
#    def __init__(self, history: typing.List, textBrowser: QtWidgets.QTextBrowser):
#        super().__init__()
#        self.history = history
#        self.textBrowser = textBrowser

#    def emit(self, record: logging.LogRecord):
#        for i in range(len(self.history)):
#            msg = self.history[i]['log']
#            displayed = self.history[i]['displayed']

#            if displayed == False:
#                self.textBrowser.append(msg)
#                self.history[i]['displayed'] = True

#class LogStreamer(logging.Handler):
#    def __init__(self):
#        super().__init__()

#        self.history = []

#        self.logger = logging.getLogger()
#        self.logger.setLevel(logging.INFO)

#        self.stream_handler = logging.StreamHandler()
#        formatter = logging.Formatter('%(asctime)s - %(name)s :: %(levelname)s :: %(message)s')
#        self.stream_handler.setFormatter(formatter)
#        self.stream_handler.setLevel(logging.INFO)
#        self.list_handler = ListHandler(self.history)

#        self.logger.addHandler(self.stream_handler)
#        self.logger.addHandler(self.list_handler)


#if __name__ == '__main__':
#    logger = LogStreamer()
#    for i in range(10):
#        logger.logger.error('This is an info message.' + str(i))


class ListHandler(logging.Handler):
    def __init__(self, history: typing.List):
        super().__init__()
        self.history = history

    def emit(self, record: logging.LogRecord):
        self.history.append({'log': record.asctime + ' : ' + record.getMessage(), 'displayed': False})


class LogStreamer(logging.Handler):
    def __init__(self):
        super().__init__()

        self.history = []

        self.logger = logging.getLogger()
        self.logger.setLevel(logging.INFO)

        self.stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s :: %(levelname)s :: %(message)s')
        self.stream_handler.setFormatter(formatter)
        self.stream_handler.setLevel(logging.INFO)
        self.list_handler = ListHandler(self.history)

        self.logger.addHandler(self.stream_handler)
        self.logger.addHandler(self.list_handler)


app = FastAPI()


@app.websocket("/ws/log")
async def websocket_log_endpoint(websocket: WebSocket):
    await websocket.accept()
    logger = LogStreamer()  # LogStreamer 인스턴스 생성

    for i in range(10):
        logger.logger.error('This is an info message.' + str(i))

    while True:
        if logger.history:
            # 웹소켓으로 로그 전송
            await websocket.send_json(logger.history)
        await asyncio.sleep(1)  # 로그가 쌓일 때마다 전송