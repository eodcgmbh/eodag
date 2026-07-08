import os
import requests


def get_cds_result(product_id=None, provider=None, collection=None, end=".nc"):
    import cdsapi
    import re

    if not product_id:
        product_id = os.environ["PRODUCT_ID"]
    if not provider:
        provider = os.environ["PROVIDER"]
    if not collection:
        collection = os.environ["COLLECTION"]

    if provider in ["cop_cds"]:
        url = "https://cds.climate.copernicus.eu/api"
    elif provider in ["cop_ads"]:
        url = "https://ads.atmosphere.copernicus.eu/api"
    os.environ["CDSAPI_URL"] = url

    client = cdsapi.Client()

    re_str = re.search(
        r"^([a-zA-Z0-9\-]+)_(\-?\d+\.\d+)_(\-?\d+\.\d+)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(.+)$",
        product_id
    )

    if re_str:
        dataset = re_str.group(1)
        x, y = float(re_str.group(2)), float(re_str.group(3))
        date = re_str.group(4) + "/" +  re_str.group(5)
        variable = re_str.group(6)
        if end in variable:
            variable = variable.replace(end, "")
        if end ==".nc":
            data_format = "netcdf"

        request = {
            "variable": [variable],
            "location": {"longitude": x, "latitude": y},
            "date": [date],
            "data_format": data_format
        }
    else:
        from datetime import datetime
        import requests

        re_str = re.search(
            r"^([a-zA-Z0-9\-]+)_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_(month_average|10_day_average|daily)?_?(v\d*)_(active|passive|combined)?_?([a-z]*)_(.+)$",
            product_id
        )
        if not re_str:
            return
        dataset = re_str.group(1)
        start, end = re_str.group(2), re_str.group(3)
        year = []
        month = []
        day = []
        time_aggregation = re_str.group(4)
        version = re_str.group(5)
        type_of_sensor = re_str.group(6)
        type_of_record = re_str.group(7)
        variable = re_str.group(8)
        if ".nc" in variable:
            variable = variable.replace(".nc", "")

        url = f"https://cds.climate.copernicus.eu/api/catalogue/v1/collections/{dataset}/constraints.json"
        token = os.environ.get("CDSAPI_KEY")
        resp = requests.get(url, headers={"PRIVATE-TOKEN": token}, timeout=60)
        constraints = resp.json()

        for entry in constraints:
            if variable in entry["variable"] and version in entry["version"] and type_of_record in entry["type_of_record"]:
                if time_aggregation and not time_aggregation in entry["time_aggregation"]:
                    continue
                if type_of_sensor and type_of_sensor in entry["type_of_sensor"]:
                    continue
                years = entry["year"]
                months = entry["month"]
                days = entry["day"]
                break

        start = datetime.fromisoformat(start)
        end = datetime.fromisoformat(end)
        year = [y for y in years if int(start.year) <= int(y) and int(y) <= int(end.year)]

        if len(year) > 2:
            month = months
        elif len(year) == 2:
            month = [m for m in months if int(start.month) <= int(m) or int(m) <= int(end.month)]
        else:
            month = [m for m in months if int(start.month) <= int(m) and int(m) <= int(end.month)]

        if len(year) > 2 or len(month) > 2:
            day = days
        elif len(month) == 2:
            day = [d for d in days if int(start.day) <= int(d) or int(d) <= int(end.day)]
        else:
            day = [d for d in days if int(start.day) <= int(d) and int(d) <= int(end.day)]

        request = {
            "variable": [variable],
            "year": year,
            "month": month,
            "day": day,
            "type_of_record": [type_of_record],
            "version": [version]
        }
        if type_of_sensor:
            request["type_of_sensor"] = [type_of_sensor]
        if time_aggregation:
            request["time_aggregation"] = [time_aggregation]

    print(request)
    req = client.retrieve(dataset, request)
    return req.location

def stream_cds_s3(s3, url, S3_BUCKET="eodag"):
    provider = os.environ["PROVIDER"]
    collection = os.environ["COLLECTION"]
    filename = os.environ["PRODUCT_ID"]
    s3_target = f"{provider}/{collection}/{filename}"

    r = requests.get(url, stream=True)
    s3.upload_fileobj(
        Fileobj=r.raw,
        Bucket=S3_BUCKET,
        Key=s3_target
    )
    print(f"Uploaded to s3://{S3_BUCKET}/{s3_target}")