"""Module providing date utils"""


from datetime import datetime, timedelta

from pandas import Timestamp, offsets

now = datetime.now()
now_0 = Timestamp(now.replace(microsecond=0))
now_s = now.strftime('%Y-%m-%d %H:%M:%S')

base_date = now.date()

yesterday_afternoon_start = now.replace(hour=13, minute=0, second=0, microsecond=0) - offsets.Day(1)
yesterday_afternoon_start_shift = now.replace(hour=13, minute=1, second=0, microsecond=0) - offsets.Day(1)
yesterday_afternoon_end_shift = now.replace(hour=14, minute=59, second=0, microsecond=0) - offsets.Day(1)
yesterday_afternoon_end = now.replace(hour=15, minute=0, second=0, microsecond=0) - offsets.Day(1)

today_morning_start = now.replace(hour=9, minute=30, second=0, microsecond=0)
today_morning_start_shift = now.replace(hour=9, minute=31, second=0, microsecond=0)
today_morning_end_shift = now.replace(hour=11, minute=29, second=0, microsecond=0)
today_morning_end = now.replace(hour=11, minute=30, second=0, microsecond=0)

today_afternoon_start = now.replace(hour=13, minute=0, second=0, microsecond=0)
today_afternoon_start_shift = now.replace(hour=13, minute=1, second=0, microsecond=0)
today_afternoon_end_shift = now.replace(hour=14, minute=59, second=0, microsecond=0)
today_afternoon_end = now.replace(hour=15, minute=0, second=0, microsecond=0)

today = base_date
yesterday = Timestamp((base_date - timedelta(1)))

two_days_ago = today - offsets.Day(2)
three_days_ago = today - offsets.Day(3)
three_months_ago = today - offsets.Day(91)
six_months_ago = today - offsets.Day(183)
one_week_ago = today - offsets.Week(1)
one_year_ago = today - offsets.Day(365) 

this_week_begin = base_date - offsets.Day(base_date.weekday())
last_week_begin = this_week_begin - offsets.Week()
last_week_end = last_week_begin + offsets.Week() - offsets.Day(3)

this_month_begin = base_date.replace(day=1)
this_month_end = this_month_begin + offsets.MonthEnd(0)
last_month_begin = this_month_begin + offsets.MonthBegin(-1)
last_month_end = this_month_end + offsets.MonthEnd(-1)
next_month_begin = this_month_begin + offsets.MonthBegin(1)
next_month_end = this_month_end + offsets.MonthEnd(1)

this_year_begin = base_date.replace(month=1,day=1)
last_year_begin = this_year_begin - offsets.YearBegin(1)

last_2_month_begin = this_month_begin + offsets.MonthBegin(-2)
last_2_month_end = this_month_end + offsets.MonthEnd(-2)
last_3_month_begin = this_month_begin + offsets.MonthBegin(-3)
last_6_month_begin = this_month_begin + offsets.MonthBegin(-6)
last_1_year_begin = this_month_begin + offsets.MonthBegin(-12)
last_2_year_begin = this_month_begin + offsets.MonthBegin(-24)
last_3_year_begin = this_month_begin + offsets.MonthBegin(-36)
last_5_year_begin = this_month_begin + offsets.MonthBegin(-60)
last_10_year_begin = this_month_begin + offsets.MonthBegin(-120)

yesterday_afternoon_start_s = yesterday_afternoon_start.strftime('%Y-%m-%d %H:%M:%S')
yesterday_afternoon_start_shift_s = yesterday_afternoon_start_shift.strftime('%Y-%m-%d %H:%M:%S')
yesterday_afternoon_end_shift_s = yesterday_afternoon_end_shift.strftime('%Y-%m-%d %H:%M:%S')
yesterday_afternoon_end_s = yesterday_afternoon_end.strftime('%Y-%m-%d %H:%M:%S')

today_morning_start_s = today_morning_start.strftime('%Y-%m-%d %H:%M:%S')
today_morning_start_shift_s = today_morning_start_shift.strftime('%Y-%m-%d %H:%M:%S')
today_morning_end_s = today_morning_end.strftime('%Y-%m-%d %H:%M:%S')
today_morning_end_shift_s = today_morning_end_shift.strftime('%Y-%m-%d %H:%M:%S')

today_afternoon_start_s = today_afternoon_start.strftime('%Y-%m-%d %H:%M:%S')
today_afternoon_start_shift_s = today_afternoon_start_shift.strftime('%Y-%m-%d %H:%M:%S')
today_afternoon_end_s = today_afternoon_end.strftime('%Y-%m-%d %H:%M:%S')
today_afternoon_end_shift_s = today_afternoon_end_shift.strftime('%Y-%m-%d %H:%M:%S')

today_s = today.strftime('%Y-%m-%d')
yesterday_s = yesterday.strftime('%Y-%m-%d')
two_days_ago_s = two_days_ago.strftime('%Y-%m-%d')
three_days_ago_s = three_days_ago.strftime('%Y-%m-%d')
one_week_ago_s = one_week_ago.strftime('%Y-%m-%d')
last_month_begin_s = last_month_begin.strftime('%Y-%m-%d')
last_month_end_s = last_month_end.strftime('%Y-%m-%d')
this_month_begin_s = this_month_begin.strftime('%Y-%m-%d')
this_month_end_s = this_month_end.strftime('%Y-%m-%d')
next_month_begin_s = next_month_begin.strftime('%Y-%m-%d')
next_month_end_s = next_month_end.strftime('%Y-%m-%d')

last_2_month_begin_s = last_2_month_begin.strftime('%Y-%m-%d')
last_2_month_end_s = last_2_month_end.strftime('%Y-%m-%d')
last_3_month_begin_s = last_3_month_begin.strftime('%Y-%m-%d')
last_6_month_begin_s = last_6_month_begin.strftime('%Y-%m-%d')
last_1_year_begin_s = last_1_year_begin.strftime('%Y-%m-%d')
last_2_year_begin_s = last_2_year_begin.strftime('%Y-%m-%d')
last_3_year_begin_s = last_3_year_begin.strftime('%Y-%m-%d')
last_5_year_begin_s = last_5_year_begin.strftime('%Y-%m-%d')
last_10_year_begin_s = last_10_year_begin.strftime('%Y-%m-%d')

this_month_s = base_date.strftime('%Y-%m')
last_month_s = (base_date + offsets.MonthEnd(-1)).strftime('%Y-%m')
last_2_month_s = (base_date + offsets.MonthEnd(-2)).strftime('%Y-%m')

this_week_begin_s = this_week_begin.strftime('%Y-%m-%d')
last_week_begin_s = last_week_begin.strftime('%Y-%m-%d')
last_week_end_s = last_week_end.strftime('%Y-%m-%d')

thirty_days_ago = today - offsets.Day(30)
thirty_days_ago_s = thirty_days_ago.strftime('%Y-%m-%d')

three_months_ago_s = three_months_ago.strftime('%Y-%m-%d')  
six_months_ago_s = six_months_ago.strftime('%Y-%m-%d')  

this_year_begin_s = this_year_begin.strftime('%Y-%m-%d')
last_year_begin_s = last_year_begin.strftime('%Y-%m-%d')
