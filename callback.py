import json
import requests
import pandas as pd
from pathlib import Path
import datetime
import logging
import time
from alert_utils import (
    secrets_dict, setup_logger
)

_log = setup_logger("core_log", "/data/log/redial.log", level=logging.DEBUG)


def redial():
    redial_file = Path('/data/log/redial.csv')
    if redial_file.exists():
        df_redial = pd.read_csv(redial_file)
    else:
        df_redial = pd.DataFrame()

    response = requests.get(
        "https://api.46elks.com/a1/calls",
        auth=(secrets_dict["elks_username"], secrets_dict["elks_password"]),
    )
    calls_dict = json.loads(response.text)
    df = pd.DataFrame(calls_dict['data'])
    df = df[[
         'to',
         'created',
         'state',
        'id'
         ]]
    df['created'] = pd.to_datetime(df['created'])
    df = df[df['created'] > datetime.datetime.now() - datetime.timedelta(days=1)]
    failed = df[df.state=='failed']
    if failed.empty:
        return

    for i, row in failed.iterrows():
        time.sleep(5)
        if not df_redial.empty:
            if row.id in df_redial.original_id.values:
                continue
        response = requests.post(
            "https://api.46elks.com/a1/calls",
            auth=(secrets_dict["elks_username"], secrets_dict["elks_password"]),
            data={
                "from": secrets_dict["elks_phone"],
                "to": row['to'],
                "voice_start": '{"play":"https://callumrollo.com/files/frederik_short.mp3"}',
                "timeout": 60,
            },
        )
        response_dict = json.loads(response.text)
        response_dict['original_id'] = row.id
        _log.warn(f"REDIAL {str(response_dict)}")
        df_redial = pd.concat((df_redial, pd.DataFrame(data=response_dict, index=[(len(df_redial)+1)])))
        df_redial.to_csv(redial_file, index=False)

if __name__ == '__main__':
    time.sleep(30)
    _log.info("START")
    redial()
    _log.info("END")
