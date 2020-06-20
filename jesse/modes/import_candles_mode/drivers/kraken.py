import requests

import jesse.helpers as jh
from jesse import exceptions
from .interface import CandleExchange
import pandas as pd
import time
from parse import parse


class Kraken(CandleExchange):
    """
    Kraken endpoint for candle data works with timestamps in seconds
    while jesse works with milliseconds
    """

    def __init__(self):
        super().__init__('Kraken', 2000, 6)
        self.endpoint = 'https://api.kraken.com/0/public/Trades'

    def init_backup_exchange(self):
        self.backup_exchange = None

    def get_starting_time(self, symbol):
        data = self._request(symbol, 000)
        return data[0][2] * 1000

    def _request(self, symbol, start):
        payload = {'pair': symbol, 'since': start}
        response = requests.get(self.endpoint, params=payload)
        self._handle_errors(response)
        rspJson = response.json()
        return rspJson["result"][self._topair(symbol, rspJson["result"].keys())]

    def fetch(self, symbol, start_timestamp):
        df = self._fetchDF(symbol, start_timestamp)
        if df is None or df.empty:
            return []
        return df.to_dict(orient="records")

    def _fetchDF(self, symbol, start_timestampEpoch):
        now = pd.Timestamp.now() - pd.Timedelta("1day")
        start_timestamp = pd.to_datetime(start_timestampEpoch, unit="ms")
        if start_timestamp > now:
            return None
        end_timestamp_1 = start_timestamp + pd.Timedelta("{}min".format(self.count))
        if end_timestamp_1 > now:
            end_timestamp_1 = now
        data = self._request(symbol, start_timestampEpoch * 10**6)
        candlesData = self._tradeDataToDF(data)
        lastTstamp = candlesData.index[-1]
        while end_timestamp_1 > lastTstamp:
            time.sleep(self.sleep_time)
            nextTstamp = lastTstamp - pd.Timedelta("1s")  # going one sec, again, to play on safe side
            nextTstampEpoch = (nextTstamp - pd.Timestamp("1970-01-01")) // pd.Timedelta("1ns")
            data = self._request(symbol, nextTstampEpoch)
            nextCandlesData = self._tradeDataToDF(data)
            nextCandlesData.drop(candlesData.index, inplace=True, errors="ignore")
            candlesData = candlesData.append(nextCandlesData, sort=True)
            lastTstamp = candlesData.index[-1]
        return self._tradeconversion(candlesData, symbol)

    def _topair(self, symbol, possibleKeys):
        for key in possibleKeys:
            if key == symbol:
                return key
            prsd = parse("X{}Z{}", key)
            if prsd is None:
                continue
            if prsd[0] == symbol[:len(prsd[0])] and prsd[1] == symbol[len(prsd[0]):]:
                return key

        return "X{}Z{}".format(symbol[:3], symbol[3:]).upper()

    def _tradeDataToDF(self, data):
        trades = []
        volumes = []
        tstamps = []
        for trade in data:
            trades.append(float(trade[0]))
            volumes.append(float(trade[1]))
            tstamps.append(pd.to_datetime(trade[2], unit="s"))
        tradeSeries = pd.Series(data=trades, index=tstamps, name="trade")
        volumeSeries = pd.Series(data=volumes, index=tstamps, name="volume")
        return tradeSeries.to_frame().join(volumeSeries.to_frame()).sort_index(inplace=False)

    def _tradeconversion(self, dataDF, symbol):
        grouper = pd.Grouper(freq="1Min", base=0)
        vols = dataDF["volume"].groupby(grouper).sum()
        trds = dataDF["trade"].groupby(grouper).ohlc()
        candls = trds
        candls.loc[:, "volume"] = vols
        # always discard the last candle anyway
        candls = candls[0:-1]
        candls = candls.fillna(method="ffill")
        candls.loc[:, "id"] = [jh.generate_unique_id() for idx in range(len(candls.index))]
        candls.loc[:, "symbol"] = symbol
        candls.loc[:, "exchange"] = self.name
        candls.loc[:, "timestamp"] = (candls.index - pd.Timestamp("1970-01-01")) // pd.Timedelta("1ms")
        return candls

    @staticmethod
    def _handle_errors(response):
        # Exchange In Maintenance
        if response.status_code == 502:
            raise exceptions.ExchangeInMaintenance('ERROR: 502 Bad Gateway. Please try again later')
        # unsupported symbol
        if response.status_code == 404:
            raise ValueError(response.json()['message'])
        # generic error
        if response.status_code != 200:
            raise Exception(response.content)
        # error in body
        if response.json()["error"]:
            raise Exception(response.json()["error"])
