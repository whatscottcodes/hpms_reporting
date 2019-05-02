import argparse
from hpms_reporting.enrollment import enrollment
from hpms_reporting.med_errors import med_errors
from hpms_reporting.pneumo import pneumo_vacc
from hpms_reporting.influenza import influ_vacc
from hpms_reporting.filepath import create_dir_if_needed


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--q", default=None, help="Number of quarter")
    parser.add_argument("--yr", default=None, help="Year of quarter")
    parser.add_argument("--return_df", default=False, help="Year of quarter")
    arguments = parser.parse_args()

    create_dir_if_needed(vars(arguments)["q"], vars(arguments)["yr"])

    enrollment(**vars(arguments))
    med_errors(**vars(arguments))
    pneumo_vacc(**vars(arguments))
    influ_vacc(**vars(arguments))

    print("Complete!")
