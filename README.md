# Alert system for autonomous platforms

The alert system monitors the health of deployed platforms and sends an alert to pilots when certain conditions are met, with an optional escalation chain. The alerts system has two primary components: the schedule and the alert dispatcher.

### Schedule

At regular intervals, the schedule script performs the following steps:

1. Download the spreadsheet containing the piloting schedule
2. Match pilot names to contact phone numbers
3. Write the schedule out to a table of phone numbers and times

If these steps fail, a notification is sent to the piloting slack channel

### Alert dispatch

At regular intervals (currently every minute) the alert dispatch script runs. The dispatcher is individually configured for each platform for what conditions will trigger and alarm to be sent out.

For gliders, an alert is raised if:

1. An alarm email is received
2. The comm log of the glider contains an uncleared, unmasked alarm
3. The glider has been on the surface for > 30 minutes

### Send an alert

In the instance on an alarm, an SMS message of alarm information and a phone call are sent to the on-duty pilot using the 46 elks service.

1. In the first instance the pilot on duty will receive an alarm
2. If the source of the alarm is not dealt with within 30 minutes, the on duty supervisor will receive an alarm
