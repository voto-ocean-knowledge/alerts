import json
import pandas as pd
from pathlib import Path
import requests
import logging
import datetime
import email
import imaplib
import sys
import re
import subprocess
import numpy as np
import pytz

_log = logging.getLogger(name="core_log")

script_dir = Path(__file__).parent.absolute()

def mailer(subject, message, recipient="callum.rollo@voiceoftheocean.org"):
    if "callum" in str(script_dir):
        _log.error(f"Mock mail {subject}: {message} to {recipient}")
        return
    _log.warning(f"email: {subject}, {message}, {recipient}")
    subject = subject.replace(" ", "-")
    send_script = script_dir / "mailer.sh"
    subprocess.check_call(["/usr/bin/bash", send_script, message, subject, recipient])


format_basic = logging.Formatter(
    "%(asctime)s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
format_alarm = logging.Formatter("%(asctime)s,%(message)s", datefmt="%Y-%m-%d %H:%M:%S")

mail_alarms_json = Path("/data/log/mail_alarms.json")
with open(script_dir / "alarm_secrets.json", "r") as secrets_file:
    secrets_dict = json.load(secrets_file)
with open(script_dir / "contacts_secrets.json", "r") as secrets_file:
    contacts = json.load(secrets_file)
mail_recipient = secrets_dict["schedule_mail"]
slack_mail = secrets_dict["slack_mail"]

schedule = pd.read_csv(
    "/data/log/schedule.csv", parse_dates=True, index_col=0, sep=";", dtype=str
)
for name, number in contacts.items():
    schedule.replace(name, number, inplace=True)
now = datetime.datetime.now()
row = schedule[schedule.index < now].iloc[-1]
pilot_phone = row["pilot"]
supervisor_phone = row["supervisor"]
if type(supervisor_phone) is float:
    supervisor_phone = None
if type(pilot_phone) is str:
    pilot_phone = pilot_phone.replace(" ", "")
if type(supervisor_phone) is str:
    supervisor_phone = supervisor_phone.replace(" ", "")

def extra_alarm_recipients():
    votoweb_dir = secrets_dict["votoweb_dir"]
    sys.path.append(votoweb_dir)
    from voto.data.db_classes import User  # noqa
    from voto.bin.add_profiles import init_db  # noqa

    init_db()
    users_to_alarm = User.objects(alarm=True)
    users_to_alarm_surface = User.objects(alarm_surface=True)
    numbers = []
    numbers_surface = []
    for user in users_to_alarm:
        if user.name not in contacts.keys():
            _log.error(f"Did not find user {user.name} in contacts")
            mailer("Missing number", f"Did not find user {user.name} in contacts")
            continue
        number = contacts[user.name]
        if number == pilot_phone:
            continue
        numbers.append(number)
    for user in users_to_alarm_surface:
        if user.name not in contacts.keys():
            _log.error(f"Did not find user {user.name} in contacts")
            mailer("Missing number", f"Did not find user {user.name} in contacts")
            continue
        number = contacts[user.name]
        numbers_surface.append(number)
    return numbers, numbers_surface


extra_alarm_numbers = []
extra_alarm_numbers_surface = []

try:
    extra_alarm_numbers, extra_alarm_numbers_surface = extra_alarm_recipients()
except:
    mailer("failed extra numbers", "Could not extract extra numbers")


def setup_logger(name, log_file, level=logging.INFO, formatter=format_basic):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def find_previous_action(df, ddict):
    df = df[~df.alarm_source.str.contains("surf")]
    if df.empty:
        return df
    df = df[(df.mission == ddict["mission"]) & (df.cycle == ddict["cycle"])  & (df.security_level == ddict["security_level"])]
    if df.empty:
        return pd.DataFrame()
    df = df.sort_values("datetime")
    return df


def parse_mrs(comm_log_file):
    df_in = pd.read_csv(
        comm_log_file,
        names=["everything"],
        sep="Neverin100years",
        engine="python",
        on_bad_lines="skip",
        encoding="latin1",
    )
    if "trmId" in df_in.everything[0]:
        _log.warning(f"old logfile type in {comm_log_file}. skipping")
        return pd.DataFrame()
    df_mrs = df_in[df_in["everything"].str.contains("SEAMRS")].copy()
    # catch short, possibly malformed MRS strings
    df_mrs = df_mrs[df_mrs.everything.astype(str).str.len() > 90]
    parts = df_mrs.everything.str.split(";", expand=True)
    df_mrs["datetime"] = pd.to_datetime(parts[0].str[1:-1], dayfirst=True)
    df_mrs["message"] = parts[5]
    msg_parts = df_mrs.message.str.split(",", expand=True)
    df_mrs = df_mrs[msg_parts[1].astype(str) != "None"]
    msg_parts = df_mrs.message.str.split(",", expand=True)
    df_mrs["glider"] = msg_parts[1].str.replace(r"\D+", "", regex=True).astype(int)
    df_mrs["mission"] = msg_parts[2].str.replace(r"\D+", "", regex=True).astype(int)
    df_mrs["cycle"] = msg_parts[3].str.replace(r"\D+", "", regex=True).astype(int)
    df_mrs["security_level"] = (
        msg_parts[4].str.replace(r"\D+", "", regex=True).fillna(0).astype(int)
    )
    df_mrs = df_mrs[["cycle", "datetime", "glider", "mission", "security_level"]]
    df_mrs["alarm"] = False
    df_mrs.loc[df_mrs.security_level > 0, "alarm"] = True
    df_alm = df_in[df_in["everything"].str.contains("SEAALR")].copy()
    if not df_alm.empty:
        last_alarm = df_alm.tail(1).everything.values[0]
        alarm_string = last_alarm.split("$SEAALR,")[1]
        alarm_parts = alarm_string.split(",")
        alarm_mask = int(alarm_parts[1].split("*")[0])
        if alarm_mask != 0:
            _log.warning(
                f"Masking alarm! Mask {alarm_mask} glider {df_mrs.glider.values[0]} mission {df_mrs.mission.values[0]} cycle {df_mrs.cycle.values[-1]}"
            )
            df_mrs.loc[df_mrs.security_level == alarm_mask, "alarm"] = False
    df_mrs = df_mrs.sort_values("datetime")
    return df_mrs


def elks_text(ddict, recipient=pilot_phone, user="pilot", fake=True):
    recipient = re.sub(r"[^0-9+]", "", recipient)
    alarm_log = logging.getLogger(name=ddict["platform_id"])
    if "SB" in ddict['platform_id']:
        message = f"Sailbuoy warning {ddict['platform_id']} M{ddict['mission']}. Source: {ddict['alarm_source']}"
    elif ddict["security_level"] == 0:
        message = f"SURFACING {ddict['platform_id']} M{ddict['mission']} cycle {ddict['cycle']}. Source: {ddict['alarm_source']}"
    else:
        message = f"ALARM {ddict['platform_id']} M{ddict['mission']} cycle {ddict['cycle']} alarm code {ddict['security_level']}. Source: {ddict['alarm_source']}"
    data = {
        "from": "VOTOalert",
        "to": recipient,
        "message": message,
    }
    if fake:
        data["dryrun"] = "yes"
    response = requests.post(
        "https://api.46elks.com/a1/sms",
        auth=(secrets_dict["elks_username"], secrets_dict["elks_password"]),
        data=data,
    )
    _log.warning(f"ELKS SEND: {response.text}")
    if response.status_code == 200:
        alarm_log.info(
            f"{ddict['glider']},{ddict['mission']},{ddict['cycle']},{ddict['security_level']},text_{user},{ddict['alarm_source']}"
        )
    else:
        _log.error(
            f"failed elks text {response.text}  {response.text} to {recipient}. {ddict['glider']},{ddict['mission']},{ddict['cycle']},{ddict['security_level']},call_{user},{ddict['alarm_source']}"
        )


def elks_call(
    ddict, recipient=pilot_phone, user="pilot", fake=True, timeout_seconds=60
):
    recipient = re.sub(r"[^0-9+]", "", recipient)
    alarm_log = logging.getLogger(name=ddict["platform_id"])
    if fake:
        response = requests.post(
            "https://api.46elks.com/a1/sms",
            auth=(secrets_dict["elks_username"], secrets_dict["elks_password"]),
            data={
                "from": "GliderAlert",
                "to": recipient,
                "message": "this is a fake call",
                "dryrun": "yes",
            },
        )
    else:
        response = requests.post(
            "https://api.46elks.com/a1/calls",
            auth=(secrets_dict["elks_username"], secrets_dict["elks_password"]),
            data={
                "from": secrets_dict["elks_phone"],
                "to": recipient,
                "voice_start": '{"play":"https://callumrollo.com/files/frederik_short.mp3"}',
                "timeout": timeout_seconds,
            },
        )
    _log.warning(f"ELKS CALL: {response.text}")
    if response.status_code == 200:
        alarm_log.info(
            f"{ddict['glider']},{ddict['mission']},{ddict['cycle']},{ddict['security_level']},call_{user},{ddict['alarm_source']}"
        )
    else:
        _log.error(
            f"failed elks call {response.text} to {recipient}. {ddict['glider']},{ddict['mission']},{ddict['cycle']},{ddict['security_level']},call_{user},{ddict['alarm_source']}"
        )
        
def phone_test(recipient, fake=True, message="Hi this is a test message from VOTO alert system"):
    data = {
        "from": "VOTOalert",
        "to": recipient,
        "message": message,
    }
    if fake:
        data["dryrun"] = "yes"
    response = requests.post(
        "https://api.46elks.com/a1/sms",
        auth=(secrets_dict["elks_username"], secrets_dict["elks_password"]),
        data=data,
    )
    print(response.text)
    if not fake:
        response = requests.post(
            "https://api.46elks.com/a1/calls",
            auth=(secrets_dict["elks_username"], secrets_dict["elks_password"]),
            data={
                "from": secrets_dict["elks_phone"],
                "to": recipient,
                "voice_start": '{"play":"https://callumrollo.com/files/frederik_short.mp3"}',
                "timeout": 60,
            },
        )
        print(response.text)


def contact_pilot(ddict, fake=True):
    _log.warning("PILOT")
    if "," in pilot_phone:
        for phone_number in pilot_phone.split(","):
            elks_text(ddict, recipient=phone_number, fake=fake)
            elks_call(ddict, recipient=phone_number, fake=fake)
    else:
        elks_text(ddict, fake=fake)
        elks_call(ddict, fake=fake)
    if extra_alarm_numbers:
        for extra_number in extra_alarm_numbers:
            elks_text(ddict, recipient=extra_number, fake=fake, user="self-volunteered")
            elks_call(ddict, recipient=extra_number, fake=fake, user="self-volunteered")


def contact_supervisor(ddict, fake=True):
    if not supervisor_phone:
        _log.warning("No supervisor on duty: no action")
        return
    _log.warning("ESCALATE")
    elks_text(ddict, recipient=supervisor_phone, user="supervisor", fake=fake)
    elks_call(ddict, recipient=supervisor_phone, user="supervisor", fake=fake)


with open(script_dir / "email_secrets.json") as json_file:
    secrets = json.load(json_file)


def check_if_new_mail():
    # Check gmail account for any new emails
    subject_file = Path("/data/log/last_mail_subject.txt")
    if subject_file.exists():
        subject = subject_file.read_text()
    else:
        with open(subject_file, 'w') as fout:
            fout.write(str("nothing"))
        return True
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(secrets["email_username"], secrets["email_password"])
    mail.select("inbox")
    result, data = mail.search(None, 'ALL')
    mail_id = data[0].split()[-1]
    __, data = mail.fetch(mail_id, "(RFC822)")
    email_subject = "willnevermatch"
    for response_part in data:
        if isinstance(response_part, tuple):
            msg = email.message_from_bytes(response_part[1])
            email_subject = msg["subject"]
    if subject == email_subject:
        return False
    with open(subject_file, 'w') as fout:
        _log.info(f"Most recent email: {email_subject}")
        fout.write(email_subject)
    return True


def parse_mail_alarms():
    # Check gmail account for emails
    start = datetime.datetime.now()
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(secrets["email_username"], secrets["email_password"])
    mail.select("inbox")
    result, data = mail.search(None, '(SUBJECT "ALARM")')
    mail_ids = data[0]

    id_list = mail_ids.split()

    # read in previous alarms record
    if mail_alarms_json.exists():
        with open(mail_alarms_json, "r") as f:
            glider_alerts = json.load(f)
    else:
        glider_alerts = {}

    # Check 3 newest emails
    for i in id_list[-3:]:
        result, data = mail.fetch(i, "(RFC822)")
        for response_part in data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                email_subject = msg["subject"]
                if "fw" in email_subject.lower():
                    email_subject = email_subject[4:]
                email_from = msg["from"]
                # If email is from alseamar and subject contains ALARM, make some noise
                if (
                    "administrateur@alseamar-cloud.com" in email_from
                    or "calglider" in email_from
                    and "ALARM" in email_subject
                ):
                    _log.debug(f"email alarm parsed {email_subject}")
                    parts = email_subject.split(" ")
                    glider = parts[0][1:-1]
                    mission = int(parts[1][1:])
                    cycle = int(parts[3][1:])
                    alarm = int(parts[4][6:-1])
                    glider_alerts[glider] = (mission, cycle, alarm)
    with open(mail_alarms_json, "w") as f:
        json.dump(glider_alerts, f, indent=4)
    elapsed = datetime.datetime.now() - start
    _log.info(f"Completed mail check in {elapsed.seconds} seconds")


def surfacing_alerts(fake=True):
    # check what time email was last checked
    timefile = Path("lastcheck_surface.txt")
    if timefile.exists():
        with open(timefile, "r") as variable_file:
            for line in variable_file.readlines():
                last_check = datetime.datetime.fromisoformat((line.strip()))
    else:
        last_check = datetime.datetime(1970, 1, 1)

    _log.info("Check for surfacing emails")
    # Check gmail account for emails
    mail = imaplib.IMAP4_SSL("imap.gmail.com")
    mail.login(secrets["email_username"], secrets["email_password"])
    mail.select("inbox")

    result, data = mail.search(None, "ALL")
    mail_ids = data[0]

    id_list = mail_ids.split()
    first_email_id = int(id_list[0])
    latest_email_id = int(id_list[-1])
    # Cut to last 3 emails
    if len(id_list) > 3:
        first_email_id = int(id_list[-3])

    # Check which emails have arrived since the last run of this script
    unread_emails = []
    for i in range(first_email_id, latest_email_id + 1):
        result, data = mail.fetch(str(i), "(RFC822)")

        for response_part in data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                date_tuple = email.utils.parsedate_tz(msg["Date"])
                if date_tuple:
                    local_date = datetime.datetime.fromtimestamp(
                        email.utils.mktime_tz(date_tuple),
                    )
                    if local_date > last_check:
                        unread_emails.append(i)
                        last_check = local_date

    # Write the time of the most recently received surfacing email
    with open(timefile, "w") as f:
        f.write(str(last_check))
    if not extra_alarm_numbers_surface:
        _log.info("no one signed up for surfacing alerts")
        return

    # Exit if no new emails
    if not unread_emails:
        _log.info("No new mail")
        return
    _log.debug("New emails")

    # Check new emails
    for i in unread_emails:
        _log.debug(f"open mail {i}")
        result, data = mail.fetch(str(i), "(RFC822)")
        for response_part in data:
            if isinstance(response_part, tuple):
                msg = email.message_from_bytes(response_part[1])
                email_subject = msg["subject"]
                if email_subject.lower()[:2] == "fw":
                    email_subject = email_subject[4:]
                email_from = msg["from"]
                # If email is from alseamar and subject contains ALARM, make some noise
                if (
                    "administrateur@alseamar-cloud.com" in email_from
                    and "ALARM" not in email_subject
                ):
                    _log.warning(f"Surface {email_subject}")
                    parts = email_subject.split(" ")
                    glider = parts[0][1:-1]
                    mission = int(parts[1][1:])
                    cycle = int(parts[3][1:])
                    ddict = {
                        "glider": int(glider[3:]),
                        "platform_id": glider,
                        "mission": mission,
                        "cycle": cycle,
                        "security_level": 0,
                        "alarm_source": "surfacing email",
                    }
                    for surface_number in extra_alarm_numbers_surface:
                        elks_text(ddict, recipient=surface_number, fake=fake)
                        elks_call(ddict, recipient=surface_number, fake=fake)


def sailbuoy_alert(ds, dispatch, t_step=15):
    platform_serial = ds.attrs["platform_serial"]
    mission = ds.attrs["deployment_id"]
    if np.datetime64("now") - ds.time.values.max() > np.timedelta64(12, "h"):
        _log.info(f"old news from SB{platform_serial} M{mission}. No warnings")
        return
    _log.info(f"process alerts for {platform_serial} M{mission}")
    df_alarm = dispatch.df_alarm.copy()
    df_alarm = df_alarm[df_alarm['glider'] == platform_serial]
    df_alarm = df_alarm[df_alarm['mission'] == mission]
    ddict = {'glider': platform_serial, 'mission': mission, 'cycle': 0, 'security_level': 1, 'alarm_source': 'sailbuoy nav', 'platform_id': platform_serial}
    for var in ["Leak", "BigLeak", "SailRotation"]:
        if var not in list(ds):
            continue
        ds[var] = ds[var].fillna(0)
        if ds[var][-t_step:].any():
            ddict['alarm_source'] = var
            df = df_alarm[df_alarm['alarm_source'] == var]
            if df.empty:
                contact_pilot(ddict, fake=dispatch.dummy_calls)
                contact_supervisor(ddict, fake=dispatch.dummy_calls)
            else:
                _log.info(f"Already logged Sailbuoy warning {ddict['platform_id']} M{ddict['mission']}. Source: {ddict['alarm_source']}")

    # Only alarm on warnings / off track after mission has run for 24 hours
    if (ds.time.values.max() - ds.time.values.min()) / np.timedelta64(1, "h") < 24:
        _log.info(f"SB{platform_serial} M{mission} has just been deployed. Only leak emails")
        return
    var = "Warning"
    ds[var] = ds[var].fillna(0)
    if ds[var][-t_step:].any():
        if not len(np.unique(ds[var][-t_step:])) == 1:
            ddict['alarm_source'] = var
            df = df_alarm[df_alarm['alarm_source'] == var]
            if df.empty:
                contact_pilot(ddict, fake=dispatch.dummy_calls)
            else:
                _log.info(f"Already logged Sailbuoy warning {ddict['platform_id']} M{ddict['mission']}. Source: {ddict['alarm_source']}")
    return  # no track radius checks for now
    var = "WithinTrackRadius"
    ds[var] = ds[var].fillna(1)
    if not ds.WithinTrackRadius[-3].any():
        ddict['alarm_source'] = var
        df = df_alarm[df_alarm['alarm_source'] == var]
        df = df[df.datetime > datetime.datetime.now() - datetime.timedelta(hours=3)]
        if df.empty:
            mailer("Sailbuoy-off-track", f"Sailbuoy {ddict['platform_id']} off track", recipient=dispatch.slack_mail)
            contact_pilot(ddict, fake=True) # Just to log this event! Never sends a call/text
        else:
            _log.info(
                f"Already warned for off track in last 3 hours {ddict['platform_id']} M{ddict['mission']}. Source: {ddict['alarm_source']}")


def parse_schedule():
    schedule = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/"
        + secrets_dict["google_sheet_id"]
        + "/export?gid=722590891&format=csv",
        index_col=0,
    ).rename(
        {
            "handover-am (UTC)": "handover-am-raw",
            "handover-pm (UTC)": "handover-pm-raw",
        },
        axis=1,
    )
    schedule.dropna(subset="pilot-day", inplace=True)
    schedule.index = pd.to_datetime(schedule.index)
    for shift in ["am", "pm"]:
        schedule[f"handover-{shift}"] = schedule[f"handover-{shift}-raw"]
        if pd.api.types.is_object_dtype(schedule[f"handover-{shift}-raw"]):
            time_parts = schedule[f"handover-{shift}-raw"].str.split(":", expand=True)
            if time_parts.shape[1] == 2:
                time_parts[1] = time_parts[1].replace({None: 0})
                schedule[f"handover-{shift}"] = (
                    time_parts[0].astype(float) + time_parts[1].astype(float) / 60
                )
        schedule.drop(f"handover-{shift}-raw", axis=1, inplace=True)

        schedule.loc[schedule[f"handover-{shift}"] > 24, f"handover-{shift}"] = np.nan
        schedule.loc[schedule[f"handover-{shift}"] < 0, f"handover-{shift}"] = np.nan
    local_now = datetime.datetime.now().astimezone(pytz.timezone("Europe/Stockholm"))
    offset_dt = local_now.utcoffset()
    offset = int(offset_dt.seconds / 3600)

    schedule["handover-am"] = schedule["handover-am"].fillna(9 - offset)
    schedule["handover-pm"] = schedule["handover-pm"].fillna(17 - offset)

    df = pd.DataFrame({"pilot": ["Callum"]}, index=[pd.to_datetime("1970-01-01")])
    for i, row in schedule.iterrows():
        day_start = i + np.timedelta64(int(60 * row["handover-am"]), "m")
        day_row = pd.DataFrame(
            {
                "pilot": [row["pilot-day"]],
                "supervisor": [row["on-call"]],
                #'surface-text': [row['surface-text-day']],
            },
            index=[day_start],
        )
        df = pd.concat([df, day_row])

        night_start = i + np.timedelta64(int(60 * row["handover-pm"]), "m")
        night_row = pd.DataFrame(
            {
                "pilot": [row["pilot-night"]],
                "supervisor": [row["on-call"]],
                #'surface-text': [row['surface-text-night']],
            },
            index=[night_start],
        )
        df = pd.concat([df, night_row])

    strings = list(pd.unique(df[df.columns].values.ravel("K")))
    names = []
    for name_str in strings:
        if type(name_str) is not str:
            continue
        name_str = name_str.replace(" ", "")
        if "," in name_str:
            parts = name_str.split(",")
            for part in parts:
                names.append(part)
        else:
            names.append(name_str)

    bad_names = []
    for name in set(names):
        if name not in contacts.keys():
            df.replace(name, "", inplace=True, regex=True)
            bad_names.append(name)
    if len(bad_names) > 0:
        mailer(
            "bad names in schedule",
            f"The following names have been ignored: {bad_names}. Using the last good schedule",
            recipient=mail_recipient,
        )
    df.to_csv("/data/log/schedule.csv", sep=";")
    raw_date = datetime.datetime.now()
    date_string = raw_date.isoformat().replace(":", "").split('.')[0]
    fn = f"schedule_{date_string}.csv"
    df.to_csv(f"/data/log/old_schedules/{fn}", sep=";")

