import os
import pandas as pd
from datetime import datetime
import threading

from Parameters.Global_Parameters import Parameters

class Simulator:
    def __init__(self, params: Parameters):
        self.directory = 'data/'
        self.dict_queue_simulation = params.dict_queue['SIMULATION']
        self.dict_queue_strategy = params.dict_queue['STRATEGY']
        self.dict_ls_df = dict()
        self.dict_threads = dict()

        for key in self.dict_queue_simulation.keys():
            self.dict_ls_df[key] = list()
            self.dict_threads[key] = None

        self.param_num_toSave = 100

    def Init_Threads(self):
        for key in self.dict_threads.keys():
            self.dict_threads[key] = threading.Thread(target=self.Func_Data_Save, args=(key,))
            self.dict_threads[key].start()

        t = threading.Thread(target=self.Func_TEMP, args=('temp',))
        t.start()

    def Func_TEMP(self, flag):
        temp = flag
        while True:
            data = self.dict_queue_strategy['Arb_FOR_Inter'].get()

    def Func_Data_Save(self, data_flag: str):

        directory = self.directory + data_flag + '/'

        if not os.path.exists(directory):
            os.makedirs(directory)

        while True:
            try:
                df = self.dict_queue_simulation[data_flag].get()

                n_data = len(self.dict_ls_df[data_flag])
                timestamp = datetime.now().strftime('%Y%m%d%H%M%S') + '_' + str(n_data)
                df['timestamp'] = timestamp

                self.dict_ls_df[data_flag].append(df)
                n_data += 1

                if n_data == self.param_num_toSave:
                    ls_one = self.dict_ls_df[data_flag].copy()
                    self.dict_ls_df[data_flag] = []

                    df_toSave = pd.concat(ls_one).reset_index(drop=True)
                    df_toSave = df_toSave.sort_values(by=['timestamp'], ascending=True)

                    max_timestamp = df_toSave['timestamp'].max()

                    df_toSave.to_csv(directory + data_flag + '_' + max_timestamp + '.csv', index=False)

            except Exception as e:
                print('[Simulation] Data_flag : ' + data_flag)
                print(e)
