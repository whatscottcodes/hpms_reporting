"""Gets quarter and quarter dates for quartely reporting."""

from calendar import monthrange
import pandas as pd

def get_Q():
    """Gets reporting quarter and year from user.
    Args:
        None, input from user; reporting quarter and year.

    Returns:
        Quarter and year as type -> int.
    """
    q_year = input('What quarter are we reporting? (Enter as Qx yyyy)')
    return int(q_year[1]), int(q_year[3:])

def get_Q_dates(Q, year):
    """Returns start and end date for each given quarter.

    Args:
        Q: int of reporting quarter.
        year: int of reporting year

    Returns:
        Tuple of start and end date as strings for given quarter.
            Ex: ('08/01/2018', '08/31/2018')}
    """
    q_dict = {1:[1,3], 2:[4,6],
              3:[7,9], 4:[10,12]}
    if Q < 4:
        start_month = '0'+str(q_dict[Q][0])
        end_month = '0'+str(q_dict[Q][1])
    else:
        start_month = str(q_dict[Q][0])
        end_month = str(q_dict[Q][1])

    start_date = start_month + '/' + '01' + '/' + str(year)

    end_date = (end_month + '/' + str(monthrange(year, q_dict[Q][1])[1]) +
                '/' + str(year))

    return pd.to_datetime(start_date), pd.to_datetime(end_date)

if __name__ == "__main__":
    Q, year = get_Q()
    dates = get_Q_dates(Q, year)
