import sqlite3
import pandas as pd
from collections import defaultdict
from hpms_reporting.get_quarter import get_quarter_dates
from hpms_reporting.filepath import filepath, create_dir_if_needed
import argparse


def enrollment_data(q=None, yr=None):

    centers = ["Providence", "Woonsocket", "Westerly"]

    eligbilities = {
        "all": "",
        "dual": "AND medicare = 1 AND medicaid = 1",
        "medicare_only": "AND medicare = 1 AND medicaid = 0",
        "medicaid_only": "AND medicare = 0 AND medicaid = 1",
        "private_pay": "AND medicare = 0 AND medicaid = 0",
    }

    start_date, end_date = get_quarter_dates(q, yr)

    enrollment_dict = defaultdict(list)

    conn = sqlite3.connect("V:\\Databases\\reporting.db")
    c = conn.cursor()

    for center in centers:
        q = f"""
            SELECT COUNT(member_id) FROM enrollment
            WHERE enrollment_date <= ?
            AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
            AND center = ?;
            """

        # census
        enrollment_dict[center].append(
            c.execute(q, (end_date, start_date, center)).fetchone()[0]
        )
        for eligibility in eligbilities.keys():
            q = f"""
                SELECT COUNT(member_id) FROM enrollment
                WHERE enrollment_date BETWEEN ? AND ?
                AND CENTER = ?
                {eligbilities[eligibility]};
                """

            enrollment_dict[center].append(
                c.execute(q, (start_date, end_date, center)).fetchone()[0]
            )
        for eligibility in eligbilities.keys():
            q = f"""
                SELECT COUNT(member_id) FROM enrollment
                WHERE disenrollment_date BETWEEN ? AND ?
                AND CENTER = ?
                {eligbilities[eligibility]};
                """

            enrollment_dict[center].append(
                c.execute(q, (start_date, end_date, center)).fetchone()[0]
            )
        q = f"""
            SELECT COUNT(member_id) FROM enrollment
            WHERE disenrollment_date BETWEEN ? AND ?
            AND disenroll_type = 'Deceased'
            AND center = ?;
            """

        # census
        enrollment_dict[center].append(
            c.execute(q, (start_date, end_date, center)).fetchone()[0]
        )

    conn.close()

    rows = [
        "Census",
        "Enrolled",
        "Dual",
        "Medicare",
        "Medicaid",
        "Private Pay",
        "Disnrolled",
        "Dual",
        "Medicare",
        "Medicaid",
        "Private Pay",
        "Deaths",
    ]

    return pd.DataFrame.from_dict(enrollment_dict).set_index(pd.Index(rows))


def double_check(df, q, yr):

    conn = sqlite3.connect("V:\\Databases\\reporting.db")
    c = conn.cursor()

    start_date, end_date = get_quarter_dates(q, yr)
    census_q = f"""
            SELECT COUNT(member_id) FROM enrollment
            WHERE enrollment_date <= ?
            AND (disenrollment_date >= ?
            OR disenrollment_date IS NULL)
            """

    enrolled_q = f"""
                SELECT COUNT(member_id) FROM enrollment
                WHERE enrollment_date BETWEEN ? AND ?
                """

    disenrolled_q = f"""
                SELECT COUNT(member_id) FROM enrollment
                WHERE disenrollment_date BETWEEN ? AND ?
                """

    deaths_q = f"""
                SELECT COUNT(member_id) FROM enrollment
                WHERE disenrollment_date BETWEEN ? AND ?
                AND disenroll_type = 'Deceased'
                """

    totals = df.sum(axis=1)

    if totals["Census"] != c.execute(census_q, (end_date, start_date)).fetchone()[0]:
        raise ValueError("Census does not match")

    if (
        totals["Enrolled"]
        != c.execute(enrolled_q, (start_date, end_date)).fetchone()[0]
    ):
        raise ValueError("Enrolled does not match")

    if (
        totals["Disnrolled"]
        != c.execute(disenrolled_q, (start_date, end_date)).fetchone()[0]
    ):
        raise ValueError("Disnrolled does not match")

    if totals["Deaths"] != c.execute(deaths_q, (start_date, end_date)).fetchone()[0]:
        raise ValueError("Disnrolled does not match")

    conn.close()

    return "Double check complete"


def enrollment(q=None, yr=None, return_df=False):
    enrollment_df = enrollment_data(q, yr)
    double_check(enrollment_df, q, yr)

    quarter, year = get_quarter_dates(q, yr, return_q=True)
    enrollment_df.to_csv(
        f"{filepath}\\{year}Q{quarter}\\hpms_enrollment_Q{quarter}_{year}.csv"
    )

    if return_df:
        return enrollment_df

    return "Enrollment Complete!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--q", default=None, help="Number of quarter")
    parser.add_argument("--yr", default=None, help="Year of quarter")
    parser.add_argument(
        "--return_df", default=False, help="Should this return a Pandas DF?"
    )

    arguments = parser.parse_args()

    create_dir_if_needed(**vars(arguments)[:2])
    enrollment(**vars(arguments))
