from asyncio.streams import StreamReader, StreamWriter
from dotenv import load_dotenv
import asyncio
import os
import json
import re
from datetime import datetime, timedelta

import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe

import logging
from typing import Awaitable, Callable, Union
from collections import deque

def get_daily() -> dict:
    dailyValue: dict = {}
    try:
        yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
        topic = "/energy/hist-" + yesterday
        msg = subscribe.simple(topic, hostname=os.getenv("MQTT_BROKER"))
        #print("Yesterday total %s %s" % (msg.topic, msg.payload))
        decoded_message = str(msg.payload.decode("utf-8"))
        lastValues = json.loads(decoded_message)
        dailyValue['electricityImportedT1Yesterday'] = lastValues['electricityImportedT1'] 
        dailyValue['electricityImportedT2Yesterday'] = lastValues['electricityImportedT2'] 
        dailyValue['electricityExportedT1Yesterday'] = lastValues['electricityExportedT1'] 
        dailyValue['electricityExportedT2Yesterday'] = lastValues['electricityExportedT2'] 
        dailyValue['gasYesterday'] = lastValues['gas'] 
        dailyValue['date'] = yesterday
        logging.info("Yesterday total %s " % json.dumps(dailyValue, indent=4, sort_keys=True))
    except Exception as err:
        logging.error(f"Unable to publish telegram on MQTT: {err}")
        dailyValue['date'] = datetime.strftime(datetime.now(), '%Y%m%d')
    return dailyValue

def get_phases() -> dict:
    lastPhase: dict = {}
    try:
        topic = "/energy/meter"
        msg = subscribe.simple(topic, hostname=os.getenv("MQTT_BROKER"))
        decoded_message = str(msg.payload.decode("utf-8"))
        lastValues = json.loads(decoded_message)
        lastPhase['electricityImportedL1'] = lastValues['electricityImportedL1'] 
        lastPhase['electricityImportedL2'] = lastValues['electricityImportedL2'] 
        lastPhase['electricityImportedL3'] = lastValues['electricityImportedL3'] 
        lastPhase['electricityExportedL1'] = lastValues['electricityExportedL1'] 
        lastPhase['electricityExportedL2'] = lastValues['electricityExportedL2'] 
        lastPhase['electricityExportedL3'] = lastValues['electricityExportedL3'] 
        lastPhase['timestamp'] = lastValues['timestamp'] 
        logging.info("Phases last %s " % json.dumps(lastPhase, indent=4, sort_keys=True))
    except Exception as err:
        logging.error(f"Unable to getting history from on MQTT: {err}")
        lastPhase['electricityImportedL1'] = 0.0 
        lastPhase['electricityImportedL2'] = 0.0
        lastPhase['electricityImportedL3'] = 0.0
        lastPhase['electricityExportedL1'] = 0.0
        lastPhase['electricityExportedL2'] = 0.0
        lastPhase['electricityExportedL3'] = 0.0
        lastPhase['timestamp'] = int(datetime.now().timestamp())
    return lastPhase

load_dotenv()
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    level=logging.DEBUG if os.getenv("LOG_LEVEL") == "DEBUG" else logging.INFO,
)


P1_ADDRESS: str = os.getenv("P1_ADDRESS", "")
obis: list = json.load(open(os.path.join(os.path.dirname(__file__), "obis.json")))[
    "obis_fields"
]
mqtt_client = mqtt.Client()
mqtt_client.connect(os.getenv("MQTT_BROKER"), 1883, 60)
mqtt_client.loop_start()

dailyValues: dict = get_daily() 
phaseMeter : dict = get_phases()


def calc_crc(telegram: list[bytes]) -> str:
    telegram_str = b"".join(telegram)
    telegram_cut = telegram_str[0 : telegram_str.find(b"!") + 1]
    x = 0
    y = 0
    crc = 0
    while x < len(telegram_cut):
        crc = crc ^ telegram_cut[x]
        x = x + 1
        y = 0
        while y < 8:
            if (crc & 1) != 0:
                crc = crc >> 1
                crc = crc ^ (int("0xA001", 16))
            else:
                crc = crc >> 1
            y = y + 1
    return hex(crc)


def parse_hex(str) -> str:
    try:
        result = bytes.fromhex(str).decode()
    except ValueError:
        result = str
    return result


async def send_telegram(telegram: list[bytes]) -> None:
    def format_value(value: str, type: str, unit: str) -> Union[str, float]:
        # Timestamp has message of format "YYMMDDhhmmssX"
        multiply = 1
        if (len(unit) > 0 and unit[0]=='k'):
            multiply = 1000
        format_functions: dict = {
            "float": lambda str: float(str) * multiply,
            "int": lambda str: int(str) * multiply,
            "timestamp": lambda str: int(
                datetime.strptime(str[:-1], "%y%m%d%H%M%S").timestamp()
            ),
            "string": lambda str: parse_hex(str),
            "unknown": lambda str: str,
        }
        return_value = format_functions[type](value.split("*")[0])
        return return_value

    telegram_formatted: dict = {}
    line: str
    for line in [line.decode() for line in telegram]:
        matches: list[[]] = re.findall("(^.*?(?=\\())|((?<=\\().*?(?=\\)))", line)
        if len(matches) > 0:
            obis_key: str = matches[0][0]
            obis_item: Union[dict, None] = next(
                (item for item in obis if item.get("key", "") == obis_key), None
            )
            if obis_item is not None:
                item_type: str = obis_item.get("type", "")
                #logging.debug("Key %s  Name: %s Unit: %s <-- %s" %  (obis_item.get("key", "") ,  obis_item.get("name", "") , obis_item.get("unit", "no unit") , line. strip()) )
                unit = obis_item.get("unit", "no unit")
                item_value_position: Union[int, None] = obis_item.get("valuePosition")
                telegram_formatted[obis_item.get("name")] = (
                    format_value(matches[1][1], item_type, unit )
                    if len(matches) == 2
                    else (
                        "|".join(
                            [
                                str(
                                    format_value(
                                        match[1],
                                        item_type[index]
                                        if type(item_type) == list
                                        else item_type,
                                        unit
                                    )
                                )
                                for index, match in enumerate(matches[1:])
                            ]
                        )
                        if item_value_position is None
                        else format_value(
                            matches[2][1],
                            item_type[item_value_position],
                            unit
                        )
                    )
                )

    global dailyValues
    yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y%m%d')
    if ( dailyValues['date'] != yesterday):
        logging.error ( "No MQTT HISTORY... getting update")
        dailyValues = get_daily()
    if ( dailyValues['date'] == datetime.strftime(datetime.now(), '%Y%m%d')):
        logging.error ( "No MQTT HISTORY... restting")
        dailyValues['electricityImportedT1Yesterday'] = telegram_formatted['electricityImportedT1'] 
        dailyValues['electricityImportedT2Yesterday'] = telegram_formatted['electricityImportedT2'] 
        dailyValues['electricityExportedT1Yesterday'] = telegram_formatted['electricityExportedT1'] 
        dailyValues['electricityExportedT2Yesterday'] = telegram_formatted['electricityExportedT2']
        dailyValue['gasYesterday'] = telegram_formatted['gas']  
        dailyValues['date'] = yesterday

    telegram_formatted['electricityImportedT1Today'] = telegram_formatted['electricityImportedT1'] - dailyValues['electricityImportedT1Yesterday']
    telegram_formatted['electricityImportedT2Today'] = telegram_formatted['electricityImportedT2'] -  dailyValues['electricityImportedT2Yesterday']
    telegram_formatted['electricityImportedToday'] = telegram_formatted['electricityImportedT1Today'] + telegram_formatted['electricityImportedT2Today']
    telegram_formatted['electricityImported'] = telegram_formatted['electricityImportedT1'] + telegram_formatted['electricityImportedT2']

    telegram_formatted['electricityExportedT1Today'] =  telegram_formatted['electricityExportedT1'] - dailyValues['electricityExportedT1Yesterday'] 
    telegram_formatted['electricityExportedT2Today'] =  telegram_formatted['electricityExportedT2'] - dailyValues['electricityExportedT2Yesterday']
    telegram_formatted['electricityExportedToday'] =  telegram_formatted['electricityExportedT1Today'] + telegram_formatted['electricityExportedT2Today']
    telegram_formatted['electricityExported'] =  telegram_formatted['electricityExportedT1'] + telegram_formatted['electricityExportedT2']

    global phaseMeter
    sumPeriod = 60.0 * 60.0 / ( telegram_formatted['timestamp'] - phaseMeter["timestamp"] )
    logging.info ("timedelata %d", ( telegram_formatted['timestamp'] - phaseMeter["timestamp"] ))
    
    phaseSumImport = phaseMeter["electricityImportedL1"] + phaseMeter["electricityImportedL2"] + phaseMeter["electricityImportedL3"]
    deltaImport = telegram_formatted['electricityImported'] - phaseSumImport
    if ( deltaImport > 1000 ):
        logging.info ( "Delta phaseSumImport too big, resetting sum: %d  - real: %d " % (phaseSumImport, telegram_formatted['electricityImported'] ))
        phaseMeter["electricityImportedL1"] = phaseMeter["electricityImportedL1"]  + (deltaImport / 3)
        phaseMeter["electricityImportedL2"] = phaseMeter["electricityImportedL2"]  + (deltaImport / 3)
        phaseMeter["electricityImportedL3"] = phaseMeter["electricityImportedL3"]  + (deltaImport / 3)
    else:
        phaseMeter["electricityImportedL1"] = phaseMeter["electricityImportedL1"] + (telegram_formatted['instantaneousActivePowerL1Plus'] / sumPeriod)
        phaseMeter["electricityImportedL2"] = phaseMeter["electricityImportedL2"] + (telegram_formatted['instantaneousActivePowerL2Plus'] / sumPeriod)
        phaseMeter["electricityImportedL3"] = phaseMeter["electricityImportedL3"] + (telegram_formatted['instantaneousActivePowerL3Plus'] / sumPeriod)
    telegram_formatted['electricityImportedL1'] = phaseMeter["electricityImportedL1"] 
    telegram_formatted['electricityImportedL2'] = phaseMeter["electricityImportedL2"] 
    telegram_formatted['electricityImportedL3'] = phaseMeter["electricityImportedL3"] 

    phaseSumExport = phaseMeter["electricityExportedL1"] + phaseMeter["electricityExportedL2"] + phaseMeter["electricityExportedL3"]
    deltaExport = telegram_formatted['electricityExported'] - phaseSumExport
    phaseMeter["timestamp"] = telegram_formatted['timestamp']
    if ( deltaExport > 1000 ):
        logging.info ( "Delta phaseSumExport too big, resetting sum: %d  - real: %d  delta: %d " % (phaseSumExport, telegram_formatted['electricityExported'],deltaExport ))
        phaseMeter["electricityExportedL1"] = phaseMeter["electricityExportedL1"]  + (deltaExport / 3)
        phaseMeter["electricityExportedL2"] = phaseMeter["electricityExportedL2"]  + (deltaExport / 3)
        phaseMeter["electricityExportedL3"] = phaseMeter["electricityExportedL3"]  + (deltaExport / 3)
    else:
        phaseMeter["electricityExportedL1"] = phaseMeter["electricityExportedL1"] + (telegram_formatted['instantaneousActivePowerL1Min'] / sumPeriod)
        phaseMeter["electricityExportedL2"] = phaseMeter["electricityExportedL2"] + (telegram_formatted['instantaneousActivePowerL2Min'] / sumPeriod)
        phaseMeter["electricityExportedL3"] = phaseMeter["electricityExportedL3"] + (telegram_formatted['instantaneousActivePowerL3Min'] / sumPeriod)
    telegram_formatted['electricityExportedL1'] = phaseMeter["electricityExportedL1"] 
    telegram_formatted['electricityExportedL2'] = phaseMeter["electricityExportedL2"] 
    telegram_formatted['electricityExportedL3'] = phaseMeter["electricityExportedL3"] 
    logging.debug("deltaImport: %d , deltaExport: %d \nPhases last %s " % (deltaImport,deltaExport, json.dumps(phaseMeter, indent=4, sort_keys=True)))


    telegram_formatted['gasToday'] =  telegram_formatted['gas'] - dailyValues['gasYesterday'] 
    
    try:    
        result = mqtt_client.publish(
            os.getenv("MQTT_TOPIC"), payload=json.dumps(telegram_formatted), retain=True
        )
        if result.rc == 0:
            logging.info("Telegram published on MQTT")
        else:
            logging.error(f"Telegram not published (return code {result.rc})")
        
        result = mqtt_client.publish("/energy/hist-"+datetime.fromtimestamp(telegram_formatted['timestamp']).strftime("%Y%m%d"), payload=json.dumps(telegram_formatted), retain=True
        )
        if result.rc != 0:
            logging.error(f"Telegram not published (return code {result.rc})")
    except Exception as err:
        logging.error(f"Unable to publish telegram on MQTT: {err}")

async def process_lines(reader):
    telegram: Union[list, None] = None
    iteration_limit: int = 10
    i: int = 0
    while True:
        if i > iteration_limit:
            raise Exception(f"Exceeded iteration limit: {iteration_limit} iteration(s)")
        data: bytes = await reader.readline()
        logging.debug(data)
        if data.startswith(b"/"):
            telegram = []
            i = i + 1
            logging.debug("New telegram")
        if telegram is not None:
            telegram.append(data)
            if data.startswith(b"!"):
                crc: str = hex(int(data[1:], 16))
                calculated_crc: str = calc_crc(telegram)
                if crc == calculated_crc:
                    logging.info(f"CRC verified ({crc}) after {i} iteration(s)")
                    await send_telegram(telegram)
                    break
                else:
                    raise Exception("CRC check failed")


async def read_telegram():
    reader: StreamReader
    writer: StreamWriter
    reader, writer = await asyncio.open_connection(P1_ADDRESS, 2000)
    try:
        await process_lines(reader)
    except Exception as err:
        logging.debug(err)
    finally:
        writer.close()


async def read_p1():
    async def timeout(awaitable: Callable, timeout: float) -> Union[Awaitable, None]:
        try:
            return await asyncio.wait_for(awaitable(), timeout=timeout)
        except Exception as err:
            logging.error(
                f"Unable to read data from {P1_ADDRESS}: {str(err) or err.__class__.__name__}"
            )

    while True:
        logging.info("Read P1 reader")
        await asyncio.gather(
            asyncio.sleep(int(os.getenv("INTERVAL", 5))),
            timeout(read_telegram, timeout=10),
        )


if __name__ == "__main__":
    asyncio.run(read_p1())
