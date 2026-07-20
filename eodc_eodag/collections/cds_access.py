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

    if provider in ["cop_cds"]:
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
            start = datetime.fromisoformat(start)
            end = datetime.fromisoformat(end)
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

            for e in constraints:
                if variable and variable not in e.get('variable', []):
                    continue
                if version and version not in e.get('version', []):
                    continue
                if type_of_sensor and type_of_sensor not in e.get('type_of_sensor', []):
                    continue
                if time_aggregation and time_aggregation not in e.get('time_aggregation', []):
                    continue
                if type_of_record and type_of_record not in e.get('type_of_record', []):
                    continue
                year = e.get('year', None)
                if year:
                    years = [y for y in year if int(start.year) <= int(y) and int(y) <= int(end.year)]
                    if len(years) == 0:
                        continue

                    month = e.get('month', '')
                    print(month)
                    if len(years) > 2:
                        months = month
                    elif len(years) == 2:
                        months = [m for m in month if int(start.month) <= int(m) or int(m) <= int(end.month)]
                    else:
                        months = [m for m in month if int(start.month) <= int(m) and int(m) <= int(end.month)]
                    if len(months) == 0:
                        continue

                    day = e.get('day', '')
                    if len(years) > 2 or len(months) > 2:
                        days = day
                    elif len(months) == 2:
                        days = [d for d in day if int(start.day) <= int(d) or int(d) <= int(end.day)]
                    else:
                        days = [d for d in day if int(start.day) <= int(d) and int(d) <= int(end.day)]

                    if len(days) == 0:
                        continue
                    else:
                        entry = e
                        break

            print(entry)

            request = {
                "variable": [variable],
                "year": years,
                "month": months,
                "day": days,
                "type_of_record": [type_of_record],
                "version": [version]
            }
            if type_of_sensor:
                request["type_of_sensor"] = [type_of_sensor]
            if time_aggregation:
                request["time_aggregation"] = [time_aggregation]

    elif provider in ["cop_ads"]:
        re_str = re.search(
            r"^(cams.eaq.vra.ENSa).(pm2p5|o3|no2|no).(l\d+).(\d{4})-(\d{2})(.+)$",
            product_id
        )

        datasets = {"cams.eaq.vra.ENSa": "cams-europe-air-quality-reanalyses"}
        variables = {
            "pm2p5": "particulate_matter_2.5um",
            "o3": "ozone",
            "no": "nitrogen_monoxide",
            "no2": "nitrogen_dioxide",
            }
        if re_str:
            data = re_str.group(1)
            variable = re_str.group(2)
            level = re_str.group(3)
            year, month = re_str.group(4), re_str.group(5)

            dataset = datasets[data]
            request = {
                "variable": [variables[variable]],
                "model": ["ensemble"],
                "level": [str(level[1:])],
                "type": ["validated_reanalysis"],
                "year": [year],
                "month": [month]
            }

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