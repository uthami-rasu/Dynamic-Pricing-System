import os 
import requests 
from datetime import datetime 
from dotenv import load_dotenv 
from pyspark.sql.functions import udf 
from pyspark.sql.types import StringType 
from utils.custom_logging import Logger

load_dotenv() 


# @Logger.log
def fetch_annual_holidays(year, country="IN"):
    API_KEY = os.getenv("HOLIDAYS_API_KEY")
    END_POINT = "https://calendarific.com/api/v2/holidays"
    try:
        national_holidays = requests.get(END_POINT,
            params={"api_key":API_KEY,"country": "IN", "year": year, "type": "national"},
        )

        religious_holidays = requests.get(
            END_POINT,
            params={"api_key":API_KEY,"country": "IN", "year": year, "type": "religious"},
        )
        result = national_holidays.json().get("response").get("holidays")
        result.extend(religious_holidays.json().get("response").get("holidays"))

        # print(result)
        # holidays = [
        #     {"name": i.get("name"), "date": i.get("date", {}).get("iso")}
        #     for i in result
        # ]
        holidays = list(
            map(
                lambda rec: datetime.strptime(rec["date"]["iso"], "%Y-%m-%d").date(),
                result,
            )
        )

        return holidays
    except Exception as ex:
        print(ex)
        pass 
holidays_2024 =fetch_annual_holidays(2024)

@udf(StringType())
def isHoliday(date):
  
    #x = f"{date} {type(date)}, {date in holidays_2024.value}"
    return date in holidays_2024 
