import argparse
from collections import defaultdict
import pandas as pd
from filepath import filepath, create_dir_if_needed, db_filepath
from paceutils import CenterEnrollment, Enrollment


def enrollment_data(params):

    centers = ["Providence", "Woonsocket", "Westerly"]

    center_enrollment = CenterEnrollment(db_filepath)

    enrollment_dict = defaultdict(list)

    for center in centers:
        enrollment_dict[center].append(
            center_enrollment.census_during_period(params, center)
        )

        enrollment_dict[center].append(center_enrollment.enrolled(params, center))
        enrollment_dict[center].append(center_enrollment.dual_enrolled(params, center))
        enrollment_dict[center].append(
            center_enrollment.medicare_only_enrolled(params, center)
        )
        enrollment_dict[center].append(
            center_enrollment.medicaid_only_enrolled(params, center)
        )
        enrollment_dict[center].append(
            center_enrollment.private_pay_enrolled(params, center)
        )

        enrollment_dict[center].append(center_enrollment.disenrolled(params, center))
        enrollment_dict[center].append(
            center_enrollment.dual_disenrolled(params, center)
        )
        enrollment_dict[center].append(
            center_enrollment.medicare_only_disenrolled(params, center)
        )
        enrollment_dict[center].append(
            center_enrollment.medicaid_only_disenrolled(params, center)
        )
        enrollment_dict[center].append(
            center_enrollment.private_pay_disenrolled(params, center)
        )

        enrollment_dict[center].append(center_enrollment.deaths(params, center))

    rows = [
        "Census",
        "Enrolled",
        "Dual",
        "Medicare",
        "Medicaid",
        "Private Pay",
        "Disenrolled",
        "Dual",
        "Medicare",
        "Medicaid",
        "Private Pay",
        "Deaths",
    ]

    return pd.DataFrame.from_dict(enrollment_dict).set_index(pd.Index(rows))


def double_check(df, params):
    enroll = Enrollment(db_filepath)
    totals = df.sum(axis=1)

    if totals["Census"] != enroll.census_during_period(params):
        raise ValueError("Census does not match")

    if totals["Enrolled"] != enroll.enrolled(params):
        raise ValueError("Enrolled does not match")

    if totals["Disenrolled"] != enroll.disenrolled(params):
        raise ValueError("Disenrolled does not match")

    if totals["Deaths"] != enroll.deaths(params):
        raise ValueError("Deaths do not match")

    return "Double check complete"


def hpms_enrollment(quarter=None, year=None):
    enroll = Enrollment(db_filepath)

    if quarter is None:
        params = enroll.last_quarter()
        quarter, year = enroll.last_quarter(return_q=True)
    else:
        params = enroll.get_quarter_dates(quarter, year)

    enrollment_df = enrollment_data(params)

    double_check(enrollment_df, params)

    enrollment_df.to_csv(
        f"{filepath}\\{year}Q{quarter}\\hpms_enrollment_Q{quarter}_{year}.csv"
    )

    return "Enrollment Complete!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--quarter", default=None, help="Number of quarter")
    parser.add_argument("--year", default=None, help="Year of quarter")

    arguments = parser.parse_args()

    create_dir_if_needed(**vars(arguments))
    hpms_enrollment(**vars(arguments))
