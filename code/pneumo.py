import argparse
import csv
import os
import pandas as pd
from filepath import filepath, create_dir_if_needed, db_filepath
from paceutils import Helpers


helpers = Helpers(db_filepath)


def eligible(params, center):
    params = [params[1]] + list(params) + [center]
    query = """
    SELECT DISTINCT(e.member_id),
    ((julianday(?) - julianday(d.dob)) / 365.25) as age
    FROM enrollment e
    LEFT JOIN demographics d ON e.member_id=d.member_id
    WHERE age >=65
    AND (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND e.center = ?;
    """

    return helpers.fetchall_query(query, params)


def during(params, center):
    params = [params[1]] + list(params) + list(params) + [center]

    query = """
    SELECT DISTINCT(e.member_id),
    ((julianday(?) - julianday(d.dob)) / 365.25) as age
    FROM enrollment e
    LEFT JOIN pneumo v on e.member_id = v.member_id
    LEFT JOIN demographics d ON v.member_id=d.member_id
    WHERE age >=65
    AND (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND dose_status = 1
    AND date_administered BETWEEN ? AND date(?, '+1 day')
    AND e.center = ?;
    """

    return helpers.fetchall_query(query, params)


def prior(params, center):

    params = [params[1]] + list(params) + [params[0]] + [center]

    query = """
    SELECT DISTINCT(e.member_id),
    ((julianday(?) - julianday(d.dob)) / 365.25) as age
    FROM enrollment e
    LEFT JOIN pneumo v on e.member_id = v.member_id
    LEFT JOIN demographics d ON v.member_id=d.member_id
    WHERE age >=65
    AND (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND dose_status = 1
    AND date_administered < ?
    AND e.center = ?
    """

    return helpers.fetchall_query(query, params)


def refused_during(params, center):

    params = [params[1]] + list(params) + list(params) + [center]

    query = """
    SELECT DISTINCT(e.member_id),
    ((julianday(?) - julianday(d.dob)) / 365.25) as age
    FROM enrollment e
    LEFT JOIN pneumo v on e.member_id = v.member_id
    LEFT JOIN demographics d ON v.member_id=d.member_id
    WHERE age >=65
    AND (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND dose_status = 0
    AND date_administered BETWEEN ? AND date(?, '+1 day')
    AND e.center = ?;
    """

    return helpers.fetchall_query(query, params)


def refused_prior(params, center):

    params = [params[1]] + list(params) + [params[0]] + [center]

    query = """
    SELECT DISTINCT(e.member_id),
    ((julianday(?) - julianday(d.dob)) / 365.25) as age
    FROM enrollment e
    LEFT JOIN pneumo v on e.member_id = v.member_id
    LEFT JOIN demographics d ON v.member_id=d.member_id
    WHERE age >=65
    AND (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND dose_status = 0
    AND date_administered < ?
    AND e.center = ?
    """

    return helpers.fetchall_query(query, params)


def contra(params, center):

    params = [params[1]] + list(params) + [center]

    query = """
    SELECT DISTINCT(e.member_id),
    ((julianday(?) - julianday(d.dob)) / 365.25) as age
    FROM enrollment e
    LEFT JOIN pneumo v on e.member_id = v.member_id
    LEFT JOIN demographics d ON v.member_id=d.member_id
    WHERE age >=65
    AND (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND dose_status = 99
    AND e.center = ?
    """

    return helpers.fetchall_query(query, params)


def center_pneumo_data(center, params, quarter, year):

    eligible_ppts = [val[0] for val in eligible(params, center)]
    during_ppts = [val[0] for val in during(params, center)]

    prior_vaccs = [val[0] for val in prior(params, center)]
    prior_ppts = [mem_id for mem_id in prior_vaccs if mem_id not in during_ppts]

    contra_ppts = [val[0] for val in contra(params, center)]

    all_recieved_or_alergic = prior_ppts + during_ppts + contra_ppts

    refused_vacc = [val[0] for val in refused_during(params, center)]
    refused_ppts = [
        mem_id for mem_id in refused_vacc if mem_id not in all_recieved_or_alergic
    ]

    all_in_pneumo = all_recieved_or_alergic + refused_ppts

    missed = [mem_id for mem_id in eligible_ppts if mem_id not in all_in_pneumo]

    csvfile = f"{filepath}\\{year}Q{quarter}\\missed_vacc\\missed_pneumo.csv"

    # Assuming res is a flat list
    with open(csvfile, "a") as output:
        writer = csv.writer(output, lineterminator="\n")
        for val in missed:
            writer.writerow([val])

    if len(missed) != (len(eligible_ppts) - len(all_in_pneumo)):
        raise ValueError("Missed does not match eligible - all recieved or refused")

    return [
        len(eligible_ppts),
        len(during_ppts),
        len(prior_ppts),
        len(refused_ppts),
        len(contra_ppts),
        len(missed),
    ]


def missed_list_for_nursing(quarter, year):
    df = pd.read_csv(
        f"{filepath}\\{year}Q{quarter}\\missed_vacc\\missed_pneumo.csv",
        header=None,
        names=["member_id"],
    )
    missed = df["member_id"].to_list()
    member_list = ",".join(["?"] * len(missed))

    query = f"""SELECT e.member_id, p.first, p.last, e.center, p.team, e.enrollment_date, e.disenrollment_date
    FROM enrollment e
    JOIN ppts p on e.member_id=p.member_id
    WHERE e.member_id IN ({member_list});"""

    os.remove(f"{filepath}\\{year}Q{quarter}\\missed_vacc\\missed_pneumo.csv")

    helpers.dataframe_query(query, missed).to_csv(
        f"{filepath}\\{year}Q{quarter}\\missed_vacc\\missed_pneumo_hpms.csv",
        index=False,
    )


def pneumo_vacc(quarter=None, year=None):
    """
    Gets quarter dates, calculates number of ppts in each vaccination status
    for each center during the quarter.
    
    Returns dataframe where each row is a vaccination status and each column
    is a center.    
    """
    if quarter is None:
        params = helpers.last_quarter()
        quarter, year = helpers.last_quarter(return_q=True)
    else:
        params = helpers.get_quarter_dates(quarter, year)

    immunization_dict = {}
    for center in ["Providence", "Woonsocket", "Westerly"]:
        immunization_dict[center] = center_pneumo_data(center, params, quarter, year)

    df_index = ["eligble", "during", "prior", "refused", "contra", "missed"]

    df = pd.DataFrame.from_dict(immunization_dict)
    df.index = df_index

    df.to_csv(f"{filepath}\\{year}Q{quarter}\\hpms_pneumo_Q{quarter}_{year}.csv")

    missed_list_for_nursing(quarter, year)
    return "Pneumococcal Complete!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--quarter", default=None, help="Number of quarter")
    parser.add_argument("--year", default=None, help="Year of quarter")

    arguments = parser.parse_args()

    create_dir_if_needed(**vars(arguments))
    pneumo_vacc(**vars(arguments))
