import pymongo


class dbWrapper:
    def __init__(self, mongo_url, db_name):
        self.client = pymongo.MongoClient(mongo_url)
        self.db = self.client[db_name]

    def get_aggregation_pipeline(self, dt_from, dt_upto, interval):
        date_format = None
        group_by = {
            "year": {"$year": "$dt"},
            "month": {"$month": "$dt"}
        }

        if interval == "hour":
            group_by["day"] = {"$dayOfMonth": "$dt"}
            group_by["hour"] = {"$hour": "$dt"}
            date_format = "%Y-%m-%d %H"
        elif interval == "day":
            group_by["day"] = {"$dayOfMonth": "$dt"}
            date_format = "%Y-%m-%d"
        elif interval == "month":
            date_format = "%Y-%m"
        else:
            raise Exception(f'Неизвестный интервал: {interval}')

        pipeline = [
            {
                "$match": {
                    "dt": {
                        "$gte": dt_from,
                        "$lte": dt_upto
                    }
                }
            },
            {
                "$group": {
                    "_id": group_by,
                    "total_value": {"$sum": "$value"}
                }
            },
            {
                "$sort": {
                    "_id": 1
                }
            }
        ]

        return pipeline, date_format
