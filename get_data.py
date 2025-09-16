pip install gdown
pip install --upgrade gdown
pip install requests pytz azure-servicebus


import os
import glob
import zipfile
import io
import csv
import json
from datetime import datetime
import pytz
import gdown
import asyncio
from azure.servicebus.aio import ServiceBusClient
from azure.servicebus import ServiceBusMessage

FOLDER_URL = "https://drive.google.com/drive/folders/***************************"
DOWNLOAD_DIR = "zipped_data"

# İki farklı eventstream connection string, her dosya için bir tane
CONN_STR_DATA = (
    "Endpoint=sb://*************.servicebus.windows.net/;"
    "SharedAccessKeyName=key_***********************;"
    "SharedAccessKey=********************;"
    "EntityPath=es_****************************f"

)

CONN_STR_FARE = (
    "Endpoint=sb://*************************.servicebus.windows.net/;"
    "SharedAccessKeyName=key_**************************;"
    "SharedAccessKey=****************************;"
    "EntityPath=es_******************************"
)

TARGET_FILES = {
    "trip_data_10.csv": CONN_STR_DATA,
    "trip_fare_10.csv": CONN_STR_FARE
}

BATCH_SIZE = 500
SEND_INTERVAL = 0.2

def extract_entity_path(conn_str):
    for p in conn_str.split(";"):
        if p.startswith("EntityPath="):
            return p.split("=", 1)[1]
    return None

async def stream_csv(inner_bytes: bytes, csv_name: str, conn_str: str):
    entity_path = extract_entity_path(conn_str)
    try:
        sb_client = ServiceBusClient.from_connection_string(conn_str)
        async with sb_client:
            async with sb_client.get_queue_sender(queue_name=entity_path) as sender:
                batch = []
                sent_batches = 0
                with zipfile.ZipFile(io.BytesIO(inner_bytes), "r") as zf_inner:
                    if csv_name not in zf_inner.namelist():
                        print(f" {csv_name} ZIP içinde yok.")
                        return
                    with zf_inner.open(csv_name) as f:
                        reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                        for row in reader:
                            try:
                                row["ingest_datetime"] = datetime.now(pytz.timezone("Australia/Sydney")).isoformat()
                                row["csv_name"] = csv_name
                                batch.append(ServiceBusMessage(json.dumps(row)))
                            except Exception as e:
                                print(f" Row işlem hatası [{csv_name}]: {e}")

                            if len(batch) >= BATCH_SIZE:
                                try:
                                    await sender.send_messages(batch)
                                    print(f" {csv_name} batch #{sent_batches + 1} ({len(batch)} rows) sent")
                                    batch.clear()
                                    sent_batches += 1
                                    if SEND_INTERVAL:
                                        await asyncio.sleep(SEND_INTERVAL)
                                except Exception as e:
                                    print(f" Send HATASI [{csv_name} / batch #{sent_batches+1}]: {e}")

                if batch:
                    try:
                        await sender.send_messages(batch)
                        print(f" {csv_name} → final batch #{sent_batches + 1} ({len(batch)} rows) sent")
                    except Exception as e:
                        print(f" Final batch send hatası [{csv_name}]: {e}")
    except Exception as e:
        print(f" Genel HATA stream_csv içinde → {csv_name}: {e}")

async def main():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    print(f"Downloading folder → {DOWNLOAD_DIR}/ …")
    gdown.download_folder(url=FOLDER_URL, output=DOWNLOAD_DIR, use_cookies=False, quiet=False)

    top_zips = glob.glob(os.path.join(DOWNLOAD_DIR, "*.zip"))
    print(f"Found {len(top_zips)} ZIP(s): {top_zips}")

    tasks = []

    for top_zip in top_zips:
        try:
            with zipfile.ZipFile(top_zip, "r") as zf_outer:
                inner_zips = [n for n in zf_outer.namelist() if n.lower().endswith(".zip")]
                for inner_name in inner_zips:
                    try:
                        inner_bytes = zf_outer.read(inner_name)
                        for csv_candidate, conn_str in TARGET_FILES.items():
                            with zipfile.ZipFile(io.BytesIO(inner_bytes), "r") as zf_check:
                                if csv_candidate in zf_check.namelist():
                                    print(f" Streaming {csv_candidate} from {inner_name} to its EventStream")
                                    tasks.append(stream_csv(inner_bytes, csv_candidate, conn_str))
                    except Exception as e:
                        print(f" Inner ZIP hata [{inner_name}] in {top_zip}: {e}")
        except Exception as e:
            print(f" Outer ZIP hata [{top_zip}]: {e}")

    if tasks:
        await asyncio.gather(*tasks)
    else:
        print(" No CSV tasks scheduled.")

    print(" All streams finished.")

await main()
