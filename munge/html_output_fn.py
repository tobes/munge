from datetime import datetime


TIME_PERIODS = [
    ('year', 60*60*24*365),
    ('month', 60*60*24*30),
    ('day', 60*60*24),
    ('hour', 60*60),
    ('minute', 60),
    ('second', 1)
]


def date_since(arg, parts=1):
    if not arg:
        return arg
    time_diff = datetime.now() - arg
    seconds = int(time_diff.total_seconds())
    strings = []
    for period_name,period_seconds in TIME_PERIODS:
        if seconds > period_seconds:
            period_value , seconds = divmod(seconds,period_seconds)
            if period_value == 1:
                strings.append("%s %s" % (period_value, period_name))
            else:
                strings.append("%s %ss" % (period_value, period_name))
            if len(strings) == parts:
                break
    return ", ".join(strings) + ' ago'
