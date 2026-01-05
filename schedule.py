import pandas as pd
from alert_utils import mailer, mail_recipient, parse_schedule, contacts

if __name__ == "__main__":
    try:
        parse_schedule()
    except:
        mailer(
            "schedule",
            "parsing the schedule failed! Using the last good one",
            recipient=mail_recipient,
        )
    schedule = pd.read_csv(
        "/data/log/schedule.csv", parse_dates=True, index_col=0, sep=";", dtype=str
    )
    for name, number in contacts.items():
        schedule.replace(name, number, inplace=True)
    schedule_pilot_numbers = set("".join(schedule.pilot.unique()))
    valid_chars = {'+', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9'}
    if not schedule_pilot_numbers.issubset(valid_chars):
        mailer(
            "schedule",
            "Invalid characters in converted schedule! Using the last good one",
        )

