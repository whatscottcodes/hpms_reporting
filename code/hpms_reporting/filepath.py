import os
from hpms_reporting.get_quarter import get_quarter_dates

filepath = "C:\\Users\\snelson\\work\\hpms_reporting\\report_files"


def create_dir_if_needed(q=None, yr=None):
    quarter, year = get_quarter_dates(q, yr, return_q=True)

    if not os.path.exists(f"{filepath}\\{year}Q{quarter}"):
        os.makedirs(f"{filepath}\\{year}Q{quarter}")

    if not os.path.exists(f"{filepath}\\{year}Q{quarter}\\missed_vacc"):
        os.makedirs(f"{filepath}\\{year}Q{quarter}\\missed_vacc")
