from datetime import datetime


def get_utc_datetime(utc_datetime_str: str) -> datetime:
    from beam_job.settings import DATETIME_FORMAT

    utc_datetime = datetime.strptime(utc_datetime_str, DATETIME_FORMAT)
    return utc_datetime
