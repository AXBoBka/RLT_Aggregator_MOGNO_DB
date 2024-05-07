import os
from pandas import DataFrame, date_range
from datetime import datetime
from database import dbWrapper
from dotenv import load_dotenv
from parse_input_file import parse_input_file


INTERVAL_TYPES = {
                    'hour': "h",
                    'day': "D",
                    'month': "MS",
                    'year': "YS"
                }

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
    if interval not in INTERVAL_TYPES.keys():
        err = f'Неизвестный интервал: {interval}'
        print(err)
        raise Exception(err)


def create_timestamps_list(start_dt: str, end_dt: str, freq: str):
    datetime_range = date_range(
        start=start_dt,
        end=end_dt,
        freq=freq
    )
    return [str(timestamp) for timestamp in datetime_range]


def create_result_dict(
        timestamps_list: list[str],
        collection: list,
    ) -> dict[str, list[str | int]]:
    dataframe = DataFrame(data=list(collection))
    result_dict = {}
    for ts in timestamps_list:
        dt_item = datetime.fromisoformat(ts)
        if dt_item not in dataframe.to_numpy():
            result_dict[dt_item.isoformat()] = 0
        else:
            result_dict[dt_item.isoformat()] = dataframe.loc[dataframe["_id"] == dt_item, "total"].iloc[0]
    return {
        "dataset": list(result_dict.values()),
        "labels": list(result_dict.keys()),
    }


if __name__ == "__main__":
    input_dict = parse_input_file(INPUT_DATA_PATH)

    dt_from = input_dict['dt_from']
    dt_upto = input_dict['dt_upto']
    group_type = input_dict['group_type']

    validate_interval(group_type)

    db_wrapper = dbWrapper(MONGO_URL, DB_NAME)
    collection = db_wrapper.db[COLLECTION_NAME]

    pipeline = db_wrapper.create_aggregation_pipeline(dt_from, dt_upto, group_type)

    aggregation_res = collection.aggregate(pipeline=pipeline)

    timestamps = create_timestamps_list(dt_from, dt_upto, INTERVAL_TYPES[group_type])

    result = create_result_dict(timestamps, aggregation_res)

    print(str(result).replace("'", '"'))
