import os
from paceutils import Helpers

filepath = "C:\\Users\\snelson\\repos\\hpms_reporting\\output"
db_filepath = "V:\\Databases\\PaceDashboard.db"


def create_dir_if_needed(quarter=None, year=None):
    if quarter is None:
        helpers = Helpers(db_filepath)
        quarter, year = helpers.last_quarter(return_q=True)

    if not os.path.exists(f"{filepath}\\{year}Q{quarter}"):
        os.makedirs(f"{filepath}\\{year}Q{quarter}")

    if not os.path.exists(f"{filepath}\\{year}Q{quarter}\\missed_vacc"):
        os.makedirs(f"{filepath}\\{year}Q{quarter}\\missed_vacc")
