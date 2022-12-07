from abc import ABC, abstractmethod
from pandas import DataFrame

class CuttingStrategey(ABC):

    @abstractmethod
    def cut_heading(self,data: DataFrame, buffer: int) -> None:
        
        """[This Abstract Base Class sets a template to take in a pandas DataFrame and optional a buffer. It then only keeps the 
        rows in which they have a heading based on the strategy subclass employed]

        Args:
            data (DataFrame): [DataFrame that is passed into the class containing magnetic survey values]
            buffer (int, optional): [buffer value to make heading cutting less strict]. Defaults to 5.

        Raises:
            AttributeError: [Input is not a DataFrame]
        """

class NorthSouthCut(CuttingStrategey):

    def cut_heading(self,data: DataFrame, buffer: int = 5) -> None:

        length = len(data.index)
        cond = []

        for i in range(length):
            if data.Heading[i] < 0+buffer or data.Heading[i] > 360-buffer\
                    or data.Heading[i] > 180-buffer and data.Heading[i] < 180+buffer:

                cond.append(True)
            else:
                cond.append(False)

        return data[cond].reset_index(drop=True)


class EastWestCut(CuttingStrategey):

    def cut_heading(self,data: DataFrame, buffer: int = 5) -> None:

        length = len(data.index)
        cond = []

        for i in range(length):
            if data.Heading[i] > 270-buffer and data.Heading[i] < 270+buffer\
                or data.Heading[i] > 90-buffer and data.Heading[i] < 90+buffer:

                cond.append(True)
            else:
                cond.append(False)

        return data[cond].reset_index(drop=True)

class NorthWestCut(CuttingStrategey):

    def cut_heading(data: DataFrame, buffer: int = 5) -> None:
        
        length = len(data.index)
        cond = []

        for i in range(length):
            if data.Heading[i] < 340+buffer or data.Heading[i] > 290-buffer\
                    or data.Heading[i] > 110-buffer and data.Heading[i] < 200+buffer:

                cond.append(True)
            else:
                cond.append(False)

        return data[cond].reset_index(drop=True)

class NorthEastCut(CuttingStrategey):

    def cut_heading(self,data: DataFrame, buffer: int = 5) -> None:

        length = len(data.index)
        cond = []

        for i in range(length):
            if data.Heading[i] < 70+buffer or data.Heading[i] > 20-buffer\
                    or data.Heading[i] > 160-buffer and data.Heading[i] < 250+buffer:

                cond.append(True)
            else:
                cond.append(False)

        return data[cond].reset_index(drop=True)
