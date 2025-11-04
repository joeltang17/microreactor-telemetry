import pandas as pd
import numpy as np
import json
import time

# ['AT', 'V', 'AP', 'RH', 'PE']: Temperature, Exhaust Vacuum, Ambient Pressure, Relative Humidity, Electrical Energy Output

df = pd.read_csv('simulator/dataset/Power Plant Data.csv')

def generate_reactor_reading(dataframe):
    reading = {}
    for col in dataframe.columns:
        mean, std = dataframe[col].mean(), dataframe[col].std()
        reading[col] = np.random.normal(mean, std)
    return reading

while True: # TODO make timer and send it to flink
    record = generate_reactor_reading(df)
    