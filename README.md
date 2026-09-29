# Alert system for autonomous platforms

The alert system monitors the health of deployed VOTO platforms (SeaExplorer gliders and Sailbuoys) and alerts the on-duty pilot by SMS and phone call when something goes wrong. If the alarm is not dealt with, it escalates to the on-call supervisor. Texts and calls are sent through the [46elks](https://46elks.com) API.

The system is a set of scripts run at regular intervals by cron:

| Script | Purpose |
|---|---|
| `schedule.py` | Download the piloting schedule and convert it to a table of phone numbers by time |
| `mail_alerts.py` | Check the alerts inbox for glider alarm and surfacing emails |
| `alert_dispatch.py` | Check every platform for alarm conditions and contact pilots (currently every minute) |
| `callback.py` | Redial any 46elks calls from the last 24 hours that failed |
| `alert_utils.py` | Shared functions: parsing, 46elks texts and calls, contacts, logging, email |
| `mailer.sh` | Send notification emails from the server |

### Schedule

`schedule.py` does the following:

1. Download the piloting schedule from a Google Sheet. It lists a day pilot, a night pilot and an on-call supervisor for each day, with am/pm handover times in UTC. If no handover time is given, the defaults are 09:00 and 17:00 Stockholm time.
2. Match pilot names to phone numbers from `contacts_secrets.json`. Unknown names are dropped and reported by email.
3. Write the schedule to `/data/log/schedule.csv` and archive a copy in `/data/log/old_schedules/`.

If parsing fails, or the converted schedule contains anything other than phone numbers, a notification email is sent and the last good schedule stays in use.

### Mail alerts

`mail_alerts.py` checks the alerts Gmail inbox and exits early if no new mail has arrived since the last check. Otherwise it:

1. Parses the newest `ALARM` emails from Alseamar and stores the latest mission, cycle and alarm code for each glider in `/data/log/mail_alarms.json`. `alert_dispatch.py` reads this file.
2. Sends a text and a call for each new surfacing (non-alarm) email to users who signed up for surfacing alerts.

### Alert dispatch

`alert_dispatch.py` loops over every glider data directory (`SEA*`, `SHW*`) under `base_data_dir`, then over every Sailbuoy NetCDF file in `/data/sailbuoy/nrt_proc`.

For gliders, an alarm is raised if:

1. The latest comm log (`G-Logs/*com.raw.log`) contains an uncleared alarm in the most recent `SEAMRS` message. Alarms masked by a `SEAALR` message are ignored, and so are logs older than 6 hours.
2. The glider has been at the surface for more than 45 minutes in its current cycle.
3. An Alseamar alarm email arrives for a mission/cycle newer than the latest comm log data.

For Sailbuoys, with data from the last 12 hours, an alarm is raised if:

1. `Leak`, `BigLeak` or `SailRotation` is flagged in recent data. The pilot and supervisor are contacted at the same time.
2. `Warning` changes in recent data, once the mission has been running for more than 24 hours. Only the pilot is contacted.

### Sending an alert and escalating

1. At first, the on-duty pilot gets a text with the alarm details followed by a phone call. Users who volunteered for alarms in the VOTO web database are contacted too.
2. If the same alarm (same mission, cycle and alarm code) is still active 30 minutes later, the on-call supervisor gets a text and a call.

Every action is logged per platform to `/data/log/alarm_<platform>.log`. This log is used to decide whether an alarm is new, has already been handled, or should be escalated.

### Safeguards

- `callback.py` redials any 46elks call from the last 24 hours that ended in `failed`, once per original call.
- `alert_dispatch.py` and `mail_alerts.py` keep a count of consecutive failed runs. After 10 failures in a row, an email asks the team to switch to the backup system (e.g. IFTTT).
- If processing a single platform fails, a notification email is sent and the other platforms are still checked.

### Configuration

These secrets files sit next to the scripts and are not tracked by git:

- `alarm_secrets.json`: 46elks credentials and phone number, `base_data_dir`, `google_sheet_id`, `votoweb_dir`, notification email addresses (`schedule_mail`, `slack_mail`), and `dummy_calls`. Set `dummy_calls` to `"True"` to send 46elks dry runs instead of real texts and calls.
- `contacts_secrets.json`: a mapping of pilot names to phone numbers.
- `email_secrets.json`: login for the alerts Gmail inbox.

Logs are written to `/data/log/`.
