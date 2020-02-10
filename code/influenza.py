import csv
import pandas as pd
import datetime
from filepath import filepath, create_dir_if_needed, db_filepath
from paceutils import Helpers
import os
import argparse

helpers = Helpers(db_filepath)


def flu_season_dates(year=None):
    if year is None:
        year = pd.datetime.today().year

    start_date = f"{year-1}-10-01"
    end_date = f"{year}-03-31"

    return start_date, end_date


def eligible(params, center):
    params = list(params) + [center]
    query = """
    SELECT DISTINCT(e.member_id)
    FROM enrollment e
    WHERE (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND e.center = ?
    """

    return helpers.fetchall_query(query, params)


def during(params, center):
    params = list(params) + list(params) + [center]

    query = """
    SELECT DISTINCT(e.member_id)
    FROM enrollment e
    LEFT JOIN influ v on e.member_id = v.member_id
    WHERE (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND dose_status = 1
    AND date_administered BETWEEN ? AND date(?, '+1 day')
    AND e.center = ?
    """

    return helpers.fetchall_query(query, params)


def prior(params, center):

    params = list(params) + [params[0], params[0]] + [center]

    query = """
    SELECT DISTINCT(e.member_id)
    FROM enrollment e
    LEFT JOIN influ v on e.member_id = v.member_id
    WHERE (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND dose_status = 1
    AND date_administered BETWEEN date(?, '-2 months') AND ?
    AND e.center = ?
    """

    return helpers.fetchall_query(query, params)


def refused_during(params, center):

    params = list(params) + list(params) + [center]

    query = """
    SELECT DISTINCT(e.member_id)
    FROM enrollment e
    LEFT JOIN influ v on e.member_id = v.member_id
    WHERE (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND dose_status = 0
    AND date_administered BETWEEN ? AND date(?, '+1 day')
    AND e.center = ?
    """

    return helpers.fetchall_query(query, params)


def refused_prior(params, center):

    params = list(params) + [params[0], params[0]] + [center]

    query = """
    SELECT DISTINCT(e.member_id)
    FROM enrollment e
    LEFT JOIN influ v on e.member_id = v.member_id
    WHERE (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND dose_status = 0
    AND date_administered date(?, '-2 months') AND ?
    AND e.center = ?
    """

    return helpers.fetchall_query(query, params)


def contra(params, center):

    params = list(params) + [center]

    query = """
    SELECT DISTINCT(e.member_id)
    FROM enrollment e
    LEFT JOIN influ v on e.member_id = v.member_id
    WHERE (disenrollment_date >=?
    OR disenrollment_date IS NULL)
    AND enrollment_date <= ?
    AND dose_status = 99
    AND e.center = ?
    """

    return helpers.fetchall_query(query, params)


def center_influ_data(center, params, quarter, year):

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

    all_in_influ = all_recieved_or_alergic + refused_ppts

    missed = [mem_id for mem_id in eligible_ppts if mem_id not in all_in_influ]

    csvfile = f"{filepath}\\{year}Q{quarter}\\missed_vacc\\missed_influ.csv"

    # Assuming res is a flat list
    with open(csvfile, "a") as output:
        writer = csv.writer(output, lineterminator="\n")
        for val in missed:
            writer.writerow([val])

    if len(missed) != (len(eligible_ppts) - len(all_in_influ)):
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
        f"{filepath}\\{year}Q{quarter}\\missed_vacc\\missed_influ.csv",
        header=None,
        names=["member_id"],
    )
    missed = df["member_id"].to_list()
    member_list = ",".join(["?"] * len(missed))

    q = f"""SELECT e.member_id, p.first, p.last, e.enrollment_date, e.disenrollment_date
    FROM enrollment e
    JOIN ppts p on e.member_id=p.member_id
    WHERE e.member_id IN ({member_list});"""

    os.remove(f"{filepath}\\{year}Q{quarter}\\missed_vacc\\missed_influ.csv")

    helpers.dataframe_query(q, missed).to_csv(
        f"{filepath}\\{year}Q{quarter}\\missed_vacc\\missed_influ_hpms.csv", index=False
    )


def influ_vacc(quarter=None, year=None):
    """
    Gets flu season or quarter dates, calculates number of ppts in each vaccination status
    for each center during the quarter.
    
    Returns dataframe where each row is a vaccination status and each column
    is a center.    
    """

    if quarter is not None:
        params = helpers.get_quarter_dates(quarter, year)
    else:
        params = flu_season_dates(year=None)
        quarter = 1
        year = datetime.datetime.now().year

    immunization_dict = {}
    for center in ["Providence", "Woonsocket", "Westerly"]:
        immunization_dict[center] = center_influ_data(center, params, quarter, year)

    df_index = ["eligible", "vacc_during", "vacc_prior", "refused", "contra", "missed"]

    df = pd.DataFrame.from_dict(immunization_dict)
    df.index = df_index

    df.to_csv(f"{filepath}\\{year}Q{quarter}\\hpms_influ_Q{quarter}_{year}.csv")

    missed_list_for_nursing(quarter, year)

    return "Influenza Complete!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--quarter", default=None, help="Number of quarter")
    parser.add_argument("--year", default=None, help="Year of quarter")

    arguments = parser.parse_args()

    create_dir_if_needed(**vars(arguments))
    influ_vacc(**vars(arguments))
