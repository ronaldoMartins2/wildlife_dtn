from enum import Enum

class DataField(Enum):
    ID = "ID"
    LATITUDE = "LATITUDE"
    LONGITUDE = "LONGITUDE"
    DATETIME = "DATETIME"
    DATETIME_MASK = "DATETIME_MASK"
    
    def __str__(self):
        return self.value