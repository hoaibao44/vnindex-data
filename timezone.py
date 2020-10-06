from datetime import date, datetime, timedelta,timezone

start_time = '2020-09-01 09:15'
tz ='+0700'
myFMT = '%Y-%m-%d %H:%M%z'

dt = datetime.strptime(start_time+tz, myFMT)
print(dt.timestamp())
