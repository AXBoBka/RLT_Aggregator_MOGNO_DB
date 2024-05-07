import os
import sys
import isodate
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from database import dbWrapper
from parse_input_file import parse_input_file


load_dotenv()
MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
ISO_FORMAT = os.getenv("ISO_FORMAT")
INPUT_DATA_PATH = os.getenv("INPUT_DATA_PATH")
INTERVALS = ['hour', 'day', 'month']


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


def prepare_pipeline(db_wrapper: dbWrapper, dt_from: str, dt_upto: str, interval: str):
    iso_dt_from, iso_dt_upto = get_isoformat(dt_from, dt_upto)
    if interval == 'hour':
        total_hours = int((iso_dt_upto - iso_dt_from).total_seconds() / 3600)  # Вычисляем общее количество часов
        date_range = [iso_dt_from + timedelta(hours=i) for i in range(total_hours + 1)]
    elif interval == 'day':
        date_range = [iso_dt_from + timedelta(days=i) for i in range((iso_dt_upto - iso_dt_from).days + 1)]
    else:
        date_range = [iso_dt_from + relativedelta(months=i) for i in range((iso_dt_upto.year - iso_dt_from.year) * 12 + iso_dt_upto.month - iso_dt_from.month + 1)]


    pipeline, date_format = db_wrapper.get_aggregation_pipeline(
        iso_dt_from, iso_dt_upto, interval)
    return pipeline, date_format, date_range


def generate_response(result, interval: str, date_format: str, date_range: list[datetime]):
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
        
    for date in date_range:
        date_iso = date.isoformat()
        if date_iso not in labels:
            for i, date_label in enumerate(labels):
                if date_label > date_iso:
                    labels.insert(i, date_iso)
                    dataset.insert(i, 0)
                    break

    response = {"dataset": dataset, "labels": labels}
    print(response)


if __name__ == "__main__":
    input_dict = parse_input_file(INPUT_DATA_PATH)
    validate_interval(input_dict['group_type'])

    db_wrapper = dbWrapper(MONGO_URL, DB_NAME)
    collection = db_wrapper.db[COLLECTION_NAME]

    pipeline, date_format, date_range = prepare_pipeline(
        db_wrapper, input_dict['dt_from'], input_dict['dt_upto'], input_dict['group_type'])

    result = collection.aggregate(pipeline=pipeline)

    generate_response(result, input_dict['group_type'], date_format, date_range)
