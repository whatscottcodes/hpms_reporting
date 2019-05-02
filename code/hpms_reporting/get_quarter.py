import pandas as pd
import calendar


def get_quarter_dates(q=None, yr=None, return_q=False):

    q_months = {1: (1, 3), 2: (4, 6), 3: (7, 9), 4: (10, 12)}

    if q is None:
        today = pd.datetime.today()
        yr = today.year
        q = ((today.month - 1) // 3 + 1) - 1  # hpms reporting is for the previous Q
        if q == 0:
            q = 4
            yr -= 1

    if return_q:
        return q, yr

    start_month, end_month = q_months[q]

    start_date = f"{yr}-{start_month}-01"
    end_date = f"{yr}-{end_month}-{calendar.monthrange(yr, end_month)[1]}"

    return str(pd.to_datetime(start_date)), str(pd.to_datetime(end_date))
