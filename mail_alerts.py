from pathlib import Path
import logging
from alert_utils import (
    setup_logger,
    secrets_dict,
    parse_mail_alarms,
    surfacing_alerts,
    mail_recipient,
    mailer, check_if_new_mail,
)

_log = setup_logger("core_log", "/data/log/mail_alarms.log", level=logging.DEBUG)

def main():
    fail_file = Path("/data/log/mail_alarm_fails.txt")
    if fail_file.exists():
        with open(fail_file) as fin:
            fail_count = int(fail_file.read_text())
    else:
        fail_count = 0
        with open(fail_file, 'w') as fout:
            fout.write(str(fail_count))
    fail_count += 1
    if fail_count == 10:
        mailer("failed-alerts", "automated mail alerts system has failed. Switch over to backup system e.g. IFTTT", mail_recipient)
    fail = False
    with open(fail_file, 'w') as fout:
        fout.write(str(fail_count))
    try:
        new_mail = check_if_new_mail()
    except:
        new_mail = True
    if not new_mail:
        _log.info("No new mail. stop processing")
        with open(fail_file, 'w') as fout:
            fout.write(str(0))
        return
    try:
        parse_mail_alarms()
    except:
        _log.error("failed to process mail alarms")
        fail = True
        mailer("failed alerts", "Failed to execute mail alarms")
    base_dir = Path(secrets_dict["base_data_dir"])
    all_glider_dirs = list(base_dir.glob("SEA*")) +  list(base_dir.glob("SHW*"))
    all_glider_dirs.sort()
    fake = False
    if secrets_dict["dummy_calls"] == "True":
        fake = True
    try:
        surfacing_alerts(fake=fake)
    except:
        _log.error("failed to process surfacing alarms")
        mailer("failed alerts", "Failed to execute surfacing alerts")
        fail = True

    if not fail:
        fail_count = 0
    with open(fail_file, 'w') as fout:
        fout.write(str(fail_count))


if __name__ == "__main__":
    _log.info("******** START CHECK **********")
    main()
    _log.info("******** COMPLETE CHECK *********")
