from abc import ABC, abstractmethod
import pandas as pd


class Presenter(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def show(self, df: pd.DataFrame):
        pass
