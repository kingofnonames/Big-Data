from datetime import datetime


def transform_str_date(sub_time_str: str) -> datetime:
    return datetime.strptime(sub_time_str.strip(), "%H:%M | %d/%m/%Y")


def clean_text(text: str) -> str:
    return text.strip().replace("\n", "").replace("\r", "")
