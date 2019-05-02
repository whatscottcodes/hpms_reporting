import pandas as pd
import numpy as np
from calendar import monthrange
import get_quarter_info as q_info

def load_filter_data(dates):
    """Loads medical incidents, removes training ppt and filters for quarter
    data only.

    More information about required csv files and naming conventions can be
    found in reports.txt.

    Args:
        dates: tuple of start and end dates of reporting month.

    Returns:
        filtered_enrolled: pandas Dataframes filtered for reporting quarter.
    """
    enrolled = pd.read_csv('data/enrollment_details.csv')

    training_index_enr = enroll_disenroll[enroll_disenroll.MemberID == 1003].index.tolist()
    enroll_disenroll.drop(training_index_enr, axis=0, inplace=True)

    enrolled['DisenrollmentDate'] = pd.to_datetime(enrolled.DisenrollmentDate)
    enrolled['EnrollmentDate'] = pd.to_datetime(enrolled.EnrollmentDate)

    disenrolled_during_quarter = (enrolled.DisenrollmentDate >= dates[0]) & (enrolled.DisenrollmentDate <= dates[1])
    currently_enrolled = enrolled.DisenrollmentDate.isnull()
    enrolled_before_end_of_quarter = (enrolled.EnrollmentDate <= dates[1])

    filtered_enrolled = enrolled[(disenrolled_during_quarter | currently_enrolled) & enrolled_before_end_of_quarter]
    return filtered_enrolled

def census(enrolled):
    """Calculates census data for reporting quarter.

    Args:
        enrolled: pandas DataFrame of all ppts enrolled in PACE during quarter.

    Returns:
        pandas Dataframe with census data for each PACE locations.
    """
    pvd = enrolled[enrolled.Center == 'Providence'].shape[0]
    won = enrolled[enrolled.Center == 'Woonsocket'].shape[0]
    wes = enrolled[enrolled.Center == 'Westerly'].shape[0]
    total = enrolled.shape[0]
    if (pvd + won + wes) != total:
        print('Census error!')

    df = {'Total':[total], 'Providence':[pvd],
          'Westerly':[wes], 'Woonsocket':[won]}
    return pd.DataFrame(df)

def enroll_disenroll_data(enrolled, dates, enroll=True):
    """Returns dataframe of new enrollment with payment types.

    Args:
        enrolled: pandas DataFrame of all ppts enrolled in PACE during quarter.
        dates: tuple of start and end dates of reporting quarter.
        enroll: boolen, if True returns enrollment data, if False
                returns disenrollment data.
    Returns:
        pandas Dataframe with enrollment data for each PACE locations.
    """
    if enroll:
        enrolled_in_mask = (enrolled.EnrollmentDate >= dates[0]) & (enrolled.EnrollmentDate <= dates[1])
        in_q = enrolled[enrolled_in_mask]
    else:
        disenrolled_in_mask = (enrolled.DisenrollmentDate >= dates[0]) & (enrolled.DisenrollmentDate <= dates[1])
        in_q = enrolled[disenrolled_in_mask]

    total = in_q.shape[0]
    pvd = in_q[in_q.Center == 'Providence'].shape[0]
    won = in_q[in_q.Center == 'Woonsocket'].shape[0]
    wes = in_q[in_q.Center == 'Westerly'].shape[0]
    if (pvd + won + wes) != total:
        print('Enrollment error!')

    dual_eligible = in_q[(in_q.Medicare.notnull()) & (in_q.Medicaid.notnull())]
    medicare_only = in_q[in_q.Medicaid.isnull() & in_q.Medicare.notnull()]
    medicaid_only = in_q[in_q.Medicaid.notnull() & in_q.Medicare.isnull()]
    private_pay = in_q[in_q.Medicaid.isnull() & in_q.Medicare.isnull()]

    if dual_eligible.shape[0] != 0:
        total_dual = dual_eligible.shape[0]
        pvd_dual = dual_eligible[dual_eligible.Center == 'Providence'].shape[0]
        won_dual = dual_eligible[dual_eligible.Center == 'Woonsocket'].shape[0]
        wes_dual = dual_eligible[dual_eligible.Center == 'Westerly'].shape[0]
    else:
        total_dual=pvd_dual=won_dual=wes_dual= 0

    if medicare_only.shape[0] != 0:
        total_care = medicare_only.shape[0]
        pvd_care = medicare_only[medicare_only.Center == 'Providence'].shape[0]
        won_care = medicare_only[medicare_only.Center == 'Woonsocket'].shape[0]
        wes_care = medicare_only[medicare_only.Center == 'Westerly'].shape[0]
    else:
        total_care=pvd_care=won_care=wes_care= 0

    if medicaid_only.shape[0] != 0:
        total_caid = medicaid_only.shape[0]
        pvd_caid = medicaid_only[medicaid_only.Center == 'Providence'].shape[0]
        won_caid = medicaid_only[medicaid_only.Center == 'Woonsocket'].shape[0]
        wes_caid = medicaid_only[medicaid_only.Center == 'Westerly'].shape[0]
    else:
        total_caid=pvd_caid=won_caid=wes_caid= 0

    if private_pay.shape[0] != 0:
        total_pay = private_pay.shape[0]
        pvd_pay = private_pay[private_pay.Center == 'Providence'].shape[0]
        won_pay = private_pay[private_pay.Center == 'Woonsocket'].shape[0]
        wes_pay = private_pay[private_pay.Center == 'Westerly'].shape[0]
    else:
        total_pay=pvd_pay=won_pay=wes_pay=0

    df = {'Total':[total, total_care, total_dual, total_caid, total_pay], 'Providence':[pvd, pvd_care, pvd_dual, pvd_caid, pvd_pay],
          'Westerly':[wes, wes_care, wes_dual, wes_caid, wes_pay], 'Woonsocket':[won, won_care, won_dual, won_caid, won_pay]}

    return pd.DataFrame(df)

def deaths_in_quarter(enrolled, dates):
    """Returns dataframe of deaths in quarter.

    Args:
        enrolled: pandas DataFrame of all ppts enrolled in PACE during quarter.
        dates: tuple of start and end dates of reporting quarter.

    Returns:
        pandas Dataframe with death data for each PACE locations.
    """
    deaths_in_q = enrolled[enrolled.DeathDate.notnull()]
    total = deaths_in_q.shape[0]
    pvd = deaths_in_q[deaths_in_q.Center == 'Providence'].shape[0]
    won = deaths_in_q[deaths_in_q.Center == 'Woonsocket'].shape[0]
    wes = deaths_in_q[deaths_in_q.Center == 'Westerly'].shape[0]

    df = {'Total':[total], 'Providence':[pvd],
          'Westerly':[wes], 'Woonsocket':[won]}

    return pd.DataFrame(df)

if __name__ == "__main__":
    Q, year = q_info.get_Q()
    dates = q_info.get_Q_dates(Q, year)
    enrolled_list = load_filter_data(dates)
    census_df = census(enrolled_list)
    enrolled_df = enroll_disenroll_data(enrolled_list, dates, enroll=True)
    disenrolled_df = enroll_disenroll_data(enrolled_list, dates, enroll=False)
    deaths_df = deaths_in_quarter(enrolled_list, dates)
    row_names = pd.Series(['Census', 'Enrollments', 'Medicare', 'Dual Eligible', 'Medicaid', 'Private Pay',
             'Disenrollments', 'Medicare', 'Dual Eligible', 'Medicaid', 'Private Pay', 'Deaths'])
    final_df = pd.concat([census_df, enrolled_df, disenrolled_df, deaths_df])
    final_df.set_index(row_names, inplace=True)
    final_df.to_csv('hpms_enrollment.csv', index=True)
