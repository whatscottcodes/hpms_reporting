import sqlite3
import pandas as pd
from hpms_reporting.get_quarter import get_quarter_dates
from hpms_reporting.filepath import filepath, create_dir_if_needed
import argparse

### Pneumococcal
# If a ppt is vaccinated *during* the quarter, all prior vaccination records for that ppt are ignored.
# If ppt has a prior vaccination indicated, then all refusals from the ppt are ignored


def create_pneumo_query_dict():
    """
    Creates dictionary of SQL queries for the various vaccination statuses
    """

    during_q = """
    SELECT distinct(v.member_id),
    ROUND((ifnull(julianday(e.disenrollment_date), julianday(?)) - julianday(d.dob)) / 365.25) as age
    FROM pneumo v
    LEFT JOIN enrollment e on v.member_id=e.member_id
    LEFT JOIN demographics d on e.member_id=d.member_id
    WHERE e.enrollment_date <= ?
    AND (e.disenrollment_date >= ?
    OR e.disenrollment_date IS NULL)
    AND age >= 65
    AND e.center = ?
    AND v.date_administered BETWEEN ? AND ?
    AND v.immunization_status = 1
    """

    prior_vacc_q = """
    SELECT distinct(v.member_id),
    ROUND((ifnull(julianday(e.disenrollment_date), julianday(?)) - julianday(d.dob)) / 365.25) as age
    FROM pneumo v
    LEFT JOIN enrollment e on v.member_id=e.member_id
    LEFT JOIN demographics d on e.member_id=d.member_id
    WHERE e.enrollment_date <= ?
    AND (e.disenrollment_date >= ?
    OR e.disenrollment_date IS NULL)
    AND age >= 65
    AND e.center = ?
    AND v.immunization_status = 1
    """

    refused_q = """
    SELECT distinct(v.member_id),
    ROUND((ifnull(julianday(e.disenrollment_date), julianday(?)) - julianday(d.dob)) / 365.25) as age
    FROM pneumo v
    LEFT JOIN enrollment e on v.member_id=e.member_id
    LEFT JOIN demographics d on e.member_id=d.member_id
    WHERE e.enrollment_date <= ?
    AND (e.disenrollment_date >= ?
    OR e.disenrollment_date IS NULL)
    AND age >= 65
    AND e.center = ?
    """

    eligible_q = """
    SELECT count(distinct(e.member_id)),
    ROUND((ifnull(julianday(e.disenrollment_date), julianday(?)) - julianday(d.dob)) / 365.25) as age
    FROM enrollment e
    JOIN demographics d on e.member_id=d.member_id
    WHERE e.enrollment_date <= ?
    AND (e.disenrollment_date >= ?
    OR e.disenrollment_date IS NULL)
    AND age >= 65
    AND e.center = ?
    """

    contra_q = """
    SELECT distinct(v.member_id),
    ROUND((ifnull(julianday(e.disenrollment_date), julianday(?)) - julianday(d.dob)) / 365.25) as age
    FROM pneumo v
    LEFT JOIN enrollment e on v.member_id=e.member_id
    LEFT JOIN demographics d on e.member_id=d.member_id
    WHERE e.enrollment_date <= ?
    AND (e.disenrollment_date >= ?
    OR e.disenrollment_date IS NULL)
    AND age >= 65
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


def center_pneumo_data(center, start_date, end_date):
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
    conn = sqlite3.connect("V:\\Databases\\reporting.db")
    c = conn.cursor()

    queries = create_pneumo_query_dict()
    eligble = c.execute(
        queries["eligible"], (end_date, end_date, start_date, center)
    ).fetchone()[0]

    during = [
        val[0]
        for val in c.execute(
            queries["during"],
            (end_date, end_date, start_date, center, start_date, end_date),
        ).fetchall()
    ]

    vacc_during = len(during)

    contra = [
        val[0]
        for val in c.execute(
            queries["contra"], (end_date, end_date, start_date, center)
        ).fetchall()
    ]
    vacc_contra = len(contra)

    prior = [
        val[0]
        for val in c.execute(
            queries["prior"], (end_date, end_date, start_date, center)
        ).fetchall()
        if val[0] not in during + contra
    ]
    vacc_prior = len(prior)

    refused = len(
        [
            val[0]
            for val in c.execute(
                queries["refused"], (end_date, end_date, start_date, center)
            ).fetchall()
            if val[0] not in during + prior + contra
        ]
    )

    missed = eligble - (vacc_during + vacc_prior + refused + vacc_contra)

    conn.close()
    return [eligble, vacc_contra, vacc_during, vacc_prior, refused, missed]


def pneumo_vacc(q=None, yr=None, return_df=False):
    """
    Gets quarter dates, calculates number of ppts in each vaccination status
    for each center during the quarter.
    
    Returns dataframe where each row is a vaccination status and each column
    is a center.    
    """
    start_date, end_date = get_quarter_dates(q, yr)
    immunization_dict = {}
    for center in ["Providence", "Woonsocket", "Westerly"]:
        immunization_dict[center] = center_pneumo_data(center, start_date, end_date)

    df_index = ["eligble", "contra", "vacc_during", "vacc_prior", "refused", "missed"]

    df = pd.DataFrame.from_dict(immunization_dict)
    df.index = df_index

    quarter, year = get_quarter_dates(q, yr, return_q=True)

    df.to_csv(f"{filepath}\\{year}Q{quarter}\\hpms_pneumo_Q{quarter}_{year}.csv")

    if return_df:
        return df

    return "Pneumococcal Complete!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--q", default=None, help="Number of quarter")
    parser.add_argument("--yr", default=None, help="Year of quarter")
    parser.add_argument(
        "--return_df", default=False, help="Should this return a Pandas DF?"
    )
    arguments = parser.parse_args()

    create_dir_if_needed(**vars(arguments)[:2])
    pneumo_vacc(**vars(arguments))
