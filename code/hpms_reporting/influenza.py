import sqlite3
import pandas as pd
import csv
from hpms_reporting.get_quarter import get_quarter_dates
from hpms_reporting.filepath import filepath, create_dir_if_needed

import argparse

### Influenza
# Calculated for Q4 and Q1 together (need to check that this is how HPMS wants it)
# What counts as "prior"? You need the vaccination every season,
# so does September count as prior and everything before that count as missed if the person didn't end up having a flu shot in the time period? (Need to ask Pauline)


def flu_season_dates(yr=None):
    if yr is None:
        yr = pd.datetime.today().year

    start_date = f"{yr-1}-10-01"
    end_date = f"{yr}-03-31"

    return start_date, end_date


def create_influ_query_dict():
    """
    Creates dictionary of SQL queries for the various vaccination statuses
    """

    during_q = """
    SELECT distinct(v.member_id)
    FROM influ v
    LEFT JOIN enrollment e on v.member_id=e.member_id
    WHERE e.enrollment_date <= ?
    AND (e.disenrollment_date >= ?
    OR e.disenrollment_date IS NULL)
    AND e.center = ?
    AND v.date_administered BETWEEN ? AND ?
    AND v.immunization_status = 1
    """

    prior_vacc_q = """
    SELECT distinct(v.member_id)
    FROM influ v
    LEFT JOIN enrollment e on v.member_id=e.member_id
    WHERE e.enrollment_date <= ?
    AND (e.disenrollment_date >= ?
    OR e.disenrollment_date IS NULL)
    AND e.center = ?
    AND v.date_administered BETWEEN ? and ?
    AND v.immunization_status = 1
    """

    refused_q = """
    SELECT distinct(v.member_id)
    FROM influ v
    LEFT JOIN enrollment e on v.member_id=e.member_id
    WHERE e.enrollment_date <= ?
    AND (e.disenrollment_date >= ?
    OR e.disenrollment_date IS NULL)
    AND e.center = ?
    AND v.date_administered BETWEEN ? AND ?
    AND v.immunization_status = 0
    """

    eligible_q = """
    SELECT distinct(e.member_id)
    FROM enrollment e
    WHERE e.enrollment_date <= ?
    AND (e.disenrollment_date >= ?
    OR e.disenrollment_date IS NULL)
    AND e.center = ?
    """

    contra_q = """
    SELECT distinct(v.member_id)
    FROM influ v
    LEFT JOIN enrollment e on v.member_id=e.member_id
    WHERE e.enrollment_date <= ?
    AND (e.disenrollment_date >= ?
    OR e.disenrollment_date IS NULL)
    AND e.center = ?
    AND v.immunization_status = 99
    """

    return {
        "during": during_q,
        "prior": prior_vacc_q,
        "refused": refused_q,
        "eligible": eligible_q,
        "contra": contra_q,
    }


def center_influ_data(center, q, yr):
    """
    Calculates number of ppts in each vaccination status
        + Starts by calculating the number of ppts enrolled in the time period and older than 65
        + Finds all ppts who have been vaccinated in the quarter
        + Finds all ppts who are alergic to the vaccination
        + Finds all ppts with a vaccination prior to the time period
            - Removes any that had a vaccination during the time period
        + Finds all ppts who have refused a vaccination
            - Removes any who have had a vaccination (they are refusing because they already had the vaccination)
        + Calculates the number of ppts that were missed
    Returns a list for the center of the number of ppts in each vaccination status
    """
    if q is not None:
        start_date, end_date = get_quarter_dates(q, yr)
    else:
        start_date, end_date = flu_season_dates(yr=None)

    conn = sqlite3.connect("V:\\Databases\\reporting.db")
    c = conn.cursor()
    queries = create_influ_query_dict()
    eligble = [
        val[0]
        for val in c.execute(
            queries["eligible"], (end_date, start_date, center)
        ).fetchall()
    ]
    eligble_for_vacc = len(eligble)

    during = [
        val[0]
        for val in c.execute(
            queries["during"], (end_date, start_date, center, start_date, end_date)
        ).fetchall()
    ]

    vacc_during = len(during)

    contra = [
        val[0]
        for val in c.execute(
            queries["contra"], (end_date, start_date, center)
        ).fetchall()
    ]
    vacc_contra = len(contra)

    prior = [
        val[0]
        for val in c.execute(
            queries["prior"],
            (
                end_date,
                start_date,
                center,
                f"{start_date.split('-')[0]}-08-01",
                start_date,
            ),
        ).fetchall()
        if val[0] not in during + contra
    ]
    vacc_prior = len(prior)

    refused = [
        val[0]
        for val in c.execute(
            queries["refused"],
            (
                end_date,
                start_date,
                center,
                f"{start_date.split('-')[0]}-08-01",
                end_date,
            ),
        ).fetchall()
        if val[0] not in during + prior + contra
    ]

    refused_vacc = len(refused)

    missed_list = [
        val for val in eligble if val not in during + prior + contra + refused
    ]

    missed_query = f"""
    SELECT p.member_id, p.first, p.last, e.enrollment_date, e.disenrollment_date
    FROM ppts p
    JOIN enrollment e on p.member_id=e.member_id
    WHERE p.member_id IN ({','.join('?' for i in missed_list)});
    """

    quarter, year = get_quarter_dates(q, yr, return_q=True)
    pd.read_sql(missed_query, conn, params=missed_list).to_csv(
        f"{filepath}\\{year}Q{quarter}\\missed_vacc\\missed_influ_{pd.datetime.today().date()}_{center}.csv",
        index=False,
    )

    missed = eligble_for_vacc - (vacc_during + vacc_prior + refused_vacc + vacc_contra)
    conn.close()

    return [
        eligble_for_vacc,
        vacc_contra,
        vacc_during,
        vacc_prior,
        refused_vacc,
        missed,
    ]


def influ_vacc(q=None, yr=None, return_df=False):
    """
    Gets flu season or quarter dates, calculates number of ppts in each vaccination status
    for each center during the quarter.
    
    Returns dataframe where each row is a vaccination status and each column
    is a center.    
    """

    immunization_dict = {}
    for center in ["Providence", "Woonsocket", "Westerly"]:
        immunization_dict[center] = center_influ_data(center, q, yr)

    df_index = ["eligble", "contra", "vacc_during", "vacc_prior", "refused", "missed"]

    df = pd.DataFrame.from_dict(immunization_dict)
    df.index = df_index

    quarter, year = get_quarter_dates(q, yr, return_q=True)
    df.to_csv(f"{filepath}\\{year}Q{quarter}\\hpms_influ_Q{quarter}_{year}.csv")

    if return_df:
        return df

    return "Influenza Complete!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--q", default=None, help="Number of quarter")
    parser.add_argument("--yr", default=None, help="Year of quarter")
    parser.add_argument(
        "--return_df", default=False, help="Should this return a Pandas DF?"
    )
    arguments = parser.parse_args()

    create_dir_if_needed(**vars(arguments)[:2])
    influ_vacc(**vars(arguments))
