import os
import sys
import isodate
from datetime import datetime
from dotenv import load_dotenv

from database import dbWrapper


load_dotenv()
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
ISO_FORMAT = os.getenv("ISO_FORMAT")
INTERVALS = ['hour', 'month', 'year']


def get_isoformat(dt_from: str, dt_upto: str):
    try:
        iso_dt_from = datetime.strptime(dt_from, ISO_FORMAT)
        iso_dt_upto = datetime.strptime(dt_upto, ISO_FORMAT)
        return iso_dt_from, iso_dt_upto
    except Exception as e:
        print(f'Ошибка во время конвертации даты в ISO формат - {e}')
        raise e


def validate_interval(interval: str):
    if interval not in INTERVALS:
        err = f'Неизвестный интервал: {interval}'
        print(err)
        raise Exception(err)


def prepare_pipeline(db_wrapper: dbWrapper , dt_from: str, dt_upto: str, interval: str):
    iso_dt_from, iso_dt_upto = get_isoformat(dt_from, dt_upto)
    pipeline, date_format = db_wrapper.get_aggregation_pipeline(iso_dt_from, iso_dt_upto, interval)
    return pipeline, date_format


def generate_response(result, interval: str, date_format: str):
    dataset = []
    labels = []

    for doc in result:
        dataset.append(doc["total_value"])
        if interval == "hour":
            date_iso_format = datetime.strptime(
                f'{doc["_id"]["year"]}-{doc["_id"]["month"]}-{doc["_id"]["day"]} {doc["_id"]["hour"]}',
                date_format
                ).isoformat()
            labels.append(date_iso_format)
        elif interval == "day":
            date_iso_format = datetime.strptime(
                f'{doc["_id"]["year"]}-{doc["_id"]["month"]}-{doc["_id"]["day"]}',
                date_format
                ).isoformat()
            labels.append(date_iso_format)
        elif interval == "month":
            date_iso_format = datetime.strptime(f'{doc["_id"]["year"]}-{doc["_id"]["month"]}',
                                                date_format
                                                ).isoformat()
            labels.append(date_iso_format)
        else:
            raise Exception(f'Неизвестный интревал: {interval}')
        # print(doc)

    response = {"dataset": dataset, "labels": labels}
    print(response)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print(f'Usage: {sys.argv[0]} <date_from> <date_to> <interval>')
        exit(0)

    dt_from = sys.argv[1]
    dt_upto = sys.argv[2]
    interval = sys.argv[3]
    validate_interval(interval)

    db_wrapper = dbWrapper(MONGO_URL, DB_NAME)
    collection = db_wrapper.db[COLLECTION_NAME]

    pipeline, date_format = prepare_pipeline(db_wrapper, dt_from, dt_upto, interval)

    result = collection.aggregate(pipeline=pipeline)

    generate_response(result, interval, date_format)

