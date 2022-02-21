import dateparser
import datetime
import csv
import logging

logger = logging.getLogger(__name__)

class DasCSVLoader(object):

    REQ_COLS = ["source_provider", "manufacturer_id", "recorded_at", "lat", "lon"]
    OPTIONAL_COLS = ["subject_name", "subject_type", "subject_subtype", "model_name", "source_type"]

    def __init__(self, dasclient):
        self.dasclient = dasclient
        return

    def parse_observation_csv(self, filename):
        f = open(filename, encoding="utf-8-sig")
        reader = csv.DictReader(f, delimiter=',', quotechar='"')

        is_error = False
        for col in self.REQ_COLS:
            if(col not in reader.fieldnames):
                logger.error(f"Missing column name: {col}")
                is_error = True

        if(is_error):
            raise DataFormatException('Invalid columns.')

        for row in reader:
            point = self._process_row(row)
            yield point

    def _process_row(self, row):

        recorded_at = dateparser.parse(row["recorded_at"])

        if(recorded_at.tzinfo == None):
            recorded_at = recorded_at.replace(tzinfo=datetime.timezone.utc)

        point = {
            "recorded_at": recorded_at,
            "location": {
                "lat": float(row['lat']),
                "lon": float(row['lon'])
            },
            "additional": {}
        }

        for col in self.OPTIONAL_COLS:
            if(col in row.keys()):
                point[col] = row[col]

        for col in row.keys():
            if((col not in self.REQ_COLS) and (col not in self.OPTIONAL_COLS)):
                point["additional"][col] = row[col]

        return point

class DataFormatException(Exception):
    pass
