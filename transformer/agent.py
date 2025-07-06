import datetime

import pandas as pd


class Agent:

    def __init__(self, data):
        self.data = data

    def transform(self):
        self.df = pd.DataFrame(list(self.data.values()))
        self.df["timestamp_created_utc"] = datetime.datetime.utcnow()
        return self.df
