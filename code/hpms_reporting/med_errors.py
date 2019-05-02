import sqlite3
import pandas as pd
from hpms_reporting.get_quarter import get_quarter_dates
from hpms_reporting.filepath import filepath, create_dir_if_needed

import argparse


def rename_columns(quarter_incidents, return_maps=False):
    """Rename columns to match accepted values as indiated by HPMS.

    Args:
        quarter_incidents: pandas DataFrame of medical incidents
        filtered for quarter.
        return_maps: if True function only returns the dictionaries of
        column mapping data

    Returns:
        quarter_incidents with renamned column names.
        contributing_map: maps columns of contributing factors to HPMS factors.
        med_error_map: maps columns of med error factors to HPMS factors.
        measures_map: maps columns of measures taken to HPMS reasons.
    """
    contributing_map = {
        "change_in_delivery_method": "Change in Method of Delivery",
        "change_in_pharmacy": "Change in Pharmacy Provider",
        "communication_with_inpatient_hospice": "Communication between PACE Inpatient Hospice",
        "communication_with_acs": "Communication between PACE Organization and ACS",
        "communication_with_alf": "Communication between PACE Organization and Assisted Living Facility",
        "communication_with_hospital": "Communication between PACE Organization and Hospital",
        "communication_with_nursing_facility": "Communication between PACE Organization and Nursing Facility",
        "communication_with_pharmacy": "Communication between PACE Organization and Pharmacy",
        "administered_by_unauthorized_staff": "Medication Administered by staff not Permitted to Administer Medication",
        "new_staff_member": "New Staff Member",
        "order_transcription_error": "Order Transcription Error",
        "participant_id_error": "Participant ID Error",
        "pharmacy_error": "Pharmacy Error",
        "physician_prescription_error": "Physician Prescription Error",
        "similar_name": "Similar Name",
        "staff_error": "Staff Error",
        "other_contributing_factor": "Other - Provide Additional Details",
    }

    med_error_map = {
        "wrong_dose": "Medication Administered - Incorrect Dose",
        "wrong_dose_not_administered": "Medication not Administered - Incorrect Dose Dispensed to Participant",
        "wrong_med_tx": "Medication Administered - Incorrect Medication",
        "wrong_med_tx_not_administered": "Medication not Administered - Incorrect Medication Dispensed to Participant",
        "wrong_ppt": "N/A",
        "wrong_ppt_not_administered": "Medication not Administered - Dispensed to wrong Participant",
        "wrong_route": "Medication Administered - Incorrect Route",
        "wrong_label_not_administered": "Medication not Administered - Medication Incorrectly Labeled",
        "wrong_time_day": "Medication Administered - Incorrect Time",
        "dose_omitted_not_administered": "Medication not Administered - Dose Omitted",
    }

    measures_map = {
        "implemented_new_policy": "Implemented a New Policy",
        "increase_home_care": "Increase Home Care",
        "change_to_medication_administration_process": "Change to Medication Administration Process",
        "changes_to_medication_transcription_process": "Changes to Medication Transcription Process",
        "initiated_contractor_oversight": "Implemented Additional Contractor Oversight",
        "revised_existing_policy": "Amended Current Policy",
        "increased_center_attendance": "Increased Center Attendance",
        "staff_education": "Staff Education",
        "change_to_participant_identification_process": "Change to Participant Identification Process",
        "pcp_assessment": "PCP Assessment",
        "requested_a_corrective_action_plan_from_contracted_provider": "Requested a Corrective Action Plan from Contracted Provider",
        "changes_to_medication_prescription_process": "Changes to Medication Prescription Process",
        "implemented_a_new_medication_delivery_system": "Implemented a New Medication Delivery System",
        "initiated_quality_improvement_activities": "Implemented Quality Improvement Activities",
        "change_in_contracted_provider": "Change in Contracted Provider",
        "rn_assessment": "RN Assessment",
    }

    if return_maps:
        return contributing_map, med_error_map, measures_map
    rename_cols = {**contributing_map, **med_error_map, **measures_map}

    return quarter_incidents.rename(columns=rename_cols)


def create_tag_cols(quarter_incidents):
    """Collects factors/measures into binned columns.

    Args:
        quarter_incidents: pandas DataFrame of medical incidents
        filtered for quarter.
    Returns:
        quarter_incidents with columns containing the tags
        for errors, measures, and contributing factors.
    """

    contributing_map, med_error_map, measures_map = rename_columns(
        quarter_incidents, return_maps=True
    )
    quarter_incidents["med_error_tags"] = ""

    for col_name in list(med_error_map.values()):
        indicies = quarter_incidents.loc[
            quarter_incidents[col_name] == 1
        ].index.tolist()
        for i in indicies:
            quarter_incidents.at[i, "med_error_tags"] += col_name

    quarter_incidents["measures_tags"] = ""

    for col_name in list(measures_map.values()):
        indicies = quarter_incidents.loc[
            quarter_incidents[col_name] == 1
        ].index.tolist()
        for i in indicies:
            if quarter_incidents.at[i, "measures_tags"] == "":
                quarter_incidents.at[i, "measures_tags"] += col_name
            else:
                addional_tags = ", " + col_name
                quarter_incidents.at[i, "measures_tags"] += addional_tags

    quarter_incidents["contributing_tags"] = ""

    for col_name in list(contributing_map.values()):
        indicies = quarter_incidents.loc[
            quarter_incidents[col_name] == 1
        ].index.tolist()
        for i in indicies:
            if quarter_incidents.at[i, "contributing_tags"] == "":
                quarter_incidents.at[i, "contributing_tags"] += col_name
            else:
                addional_tags = ", " + col_name
                quarter_incidents.at[i, "contributing_tags"] += addional_tags

    return quarter_incidents


def map_location_and_center(quarter_incidents):
    """Maps location and center coloumns to HPMS language.

    Args:
        quarter_incidents: pandas DataFrame of medical incidents
        filtered for quarter.

    Returns:
        quarter_incidents with renamned column names.
    """

    location_map = {
        "ADHC/Clinic": "PACE Center",
        "Alternative Care Setting": "Alternative Care Setting",
        "ALF/PCBH": "Assisted Living Facility",
        "Home/Independent Living": "Participant Home",
        "Hospital": "Hospital",
        "Inpatient Hospice": "Inpatient Hospice",
        "Nursing Home": "Nursing Facility",
    }

    quarter_incidents["location"] = quarter_incidents["location"].map(
        lambda x: location_map[x]
    )

    center_map = {
        "Providence": "PACE Rhode Island - Providence",
        "Woonsocket": "PACE Rhode Island - Woonsocket",
        "Westerly": "PACE Rhode Island - Westerly",
    }

    quarter_incidents["center"] = quarter_incidents["center"].map(
        lambda x: center_map[x]
    )

    return quarter_incidents


def create_csv(quarter_incidents):
    """Createst csv of med_error incidents for HPMS.

    Args:
        quarter_incidents: pandas DataFrame of medical incidents
        filtered for quarter.
        Q: quarter number as int

    Returns:
        None.
    """
    final_cols_map = {
        "member_id": "Member ID",
        "center": "Site Name",
        "location": "Location of Incident (select from dropdown)",
        "med_error_tags": "Type of Medication Error (select from dropdown)",
        "contributing_tags": "Contributing Factors (use values from separate worksheet)",
        "Other - Provide Additional Details": "Other Contributing Factor",
        "measures_tags": "Actions Taken (use values from separate worksheet)",
        "comments": "Other Action",
    }

    quarter_incidents.rename(columns=final_cols_map, inplace=True)

    final_cols = [
        "Member ID",
        "Site Name",
        "Location of Incident (select from dropdown)",
        "Type of Medication Error (select from dropdown)",
        "Contributing Factors (use values from separate worksheet)",
        "Other Contributing Factor",
        "Actions Taken (use values from separate worksheet)",
        "Other Action",
    ]

    final_med_incidents = quarter_incidents[final_cols].copy()

    return final_med_incidents


def med_errors(q=None, yr=None, return_df=False):
    conn = sqlite3.connect("V:\\Databases\\reporting.db")

    start_date, end_date = get_quarter_dates(q, yr)

    query = """
        SELECT med_errors.*, e.center FROM med_errors
        JOIN enrollment e on med_errors.member_id=e.member_id
        WHERE date_discovered BETWEEN ? and ?
        """

    quarter_incidents = rename_columns(
        pd.read_sql(query, conn, params=(start_date, end_date))
    )
    quarter_incidents = create_tag_cols(quarter_incidents)
    quarter_incidents = map_location_and_center(quarter_incidents)

    conn.close()

    final_df = create_csv(quarter_incidents)

    quarter, year = get_quarter_dates(q, yr, return_q=True)
    final_df.to_csv(
        f"{filepath}\\{year}Q{quarter}\\hpms_med_errors_Q{quarter}_{year}.csv"
    )

    if return_df:
        return final_df

    return "Med Errors Complete!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--q", default=None, help="Number of quarter")
    parser.add_argument("--yr", default=None, help="Year of quarter")
    parser.add_argument(
        "--return_df", default=False, help="Should this return a Pandas DF?"
    )
    arguments = parser.parse_args()

    create_dir_if_needed(**vars(arguments)[:2])
    med_errors(**vars(arguments))
