"""Local wall-clock calendar; transition days are deliberately unavailable."""
from datetime import datetime, timedelta
from importlib.resources import files
from zoneinfo import ZoneInfo


def month_bounds(month):
    start = datetime.strptime(month, '%Y-%m')
    return start, datetime(start.year + (start.month == 12), start.month % 12 + 1, 1)


def calendar_rows(months):
    # Use the pinned package on every OS, not an uncontrolled host timezone database.
    with files('tzdata.zoneinfo').joinpath('America', 'New_York').open('rb') as stream:
        zone = ZoneInfo.from_file(stream, key='America/New_York')
    supplied = set(months)
    start = month_bounds(min(supplied))[0]
    end = month_bounds(max(supplied))[1]
    day = start
    while day < end:
        tomorrow = day + timedelta(days=1)
        transition = day.replace(tzinfo=zone).utcoffset() != tomorrow.replace(tzinfo=zone).utcoffset()
        for hour in range(24):
            local = day + timedelta(hours=hour)
            yield local, local.strftime('%Y-%m') in supplied, transition
        day = tomorrow
