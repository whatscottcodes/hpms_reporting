import argparse
from enrollment import hpms_enrollment
from med_errors import med_errors
from pneumo import pneumo_vacc
from influenza import influ_vacc
from paceutils import Helpers
from filepath import create_dir_if_needed, db_filepath


def hpms_reporting_wrapper(q=None, yr=None):
    if q is None:
        helpers = Helpers(db_filepath)
        q, yr = helpers.last_quarter(return_q=True)

    create_dir_if_needed(q, yr)
    hpms_enrollment(q, yr)
    med_errors(q, yr)
    pneumo_vacc(q, yr)

    if (q == 4) | (q == 1):
        influ_vacc(q, yr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--q", default=None, help="Number of quarter")
    parser.add_argument("--yr", default=None, help="Year of quarter")

    arguments = parser.parse_args()

    hpms_reporting_wrapper(**vars(arguments))

    print("Complete!")
