import requests
import csv
from datetime import datetime
from http import HTTPStatus


class DownloadError(Exception):
    pass


class NotFoundError(DownloadError):
    pass


class AccessDeniedError(DownloadError):
    pass


def download_file(url, file_name="latest.txt"):
    response = requests.get(url, timeout=30)

    if response.status_code == HTTPStatus.NOT_FOUND:
        raise NotFoundError(f"File not found at {url}")
    elif response.status_code == HTTPStatus.FORBIDDEN:
        raise AccessDeniedError(f"Access denied to {url}")
    response.raise_for_status()

    with open(file_name, "wb") as file:
        file.write(response.content)


def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


def log_time(func):
    def wrapper(*args, **kwargs):
        start = datetime.now()
        print(f"Start: {start}")
        try:
            return func(*args, **kwargs)
        finally:
            end = datetime.now()
            print(f"End: {end}")
            print(f"Execution time: {end - start}")

    return wrapper


class ETL:
    def __init__(self, file_name):
        self.file_name = file_name        

    def extracts(self):
        with open(self.file_name, "r", encoding="utf-8") as file:
            for line in file:
                yield line.strip().split(",")

    def transform(self, row):
        missing_cols_data = []
        sum_avg_data = [row[0],0,0]
        row_values = [float(values) for values in row[1:] if is_number(values)]
        missing_values = [i for i, value in enumerate(row) if not is_number(value)]
        if row_values:
            total = sum(row_values)
            average = total / len(row_values) if row_values else 0
            sum_avg_data = [row[0], total, average]

        if missing_values:
            missing_cols_data = [row[0], "|".join(map(str,missing_values))]
        return {'sum_avg_data': sum_avg_data, 'missing_cols_data': missing_cols_data}

    @log_time
    def process(self):
        with open("values.csv", "w", newline="") as values_file, \
                open("missing_values.csv", "w", newline="") as missing_file:

            values_writer = csv.writer(values_file)
            missing_writer = csv.writer(missing_file)

            values_writer.writerow(["index", "sum", "avg"])
            missing_writer.writerow(["index", "missing_columns"])

            for row in self.extracts():
                data = self.transform(row)

                values_writer.writerow(data["sum_avg_data"])

                if data["missing_cols_data"]:
                    missing_writer.writerow(data["missing_cols_data"])


def main():
    try:
        download_file("https://oleksandr-fedoruk.com/sggw/podyplomowe/file", "data.csv")
        etl = ETL("data.csv")
        etl.process()

        #download_file("https://oleksandr-fedoruk.com/sggw/podyplomowe/404", "latest.csv")
        download_file("https://oleksandr-fedoruk.com/sggw/podyplomowe/403", "latest.csv")
    except DownloadError as e:
        print(f"Error downloading file: {e}")


if __name__ == "__main__":
    main()
