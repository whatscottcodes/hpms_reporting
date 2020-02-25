from paceutils import Helpers
from filepath import filepath, create_dir_if_needed, db_filepath
import pandas as pd
import numpy as np
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
    """Maps location and centers coloumns to HPMS language.

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
    quarter_incidents["Indicator Type - v1"] = "IB"
    quarter_incidents["Contract Number"] = "H4105"

    # fillna cols correctly

    cf_null = (
        quarter_incidents["Contributing Factors (use values from separate worksheet)"]
        == ""
    )
    details_null = (
        quarter_incidents["Actions Taken (use values from separate worksheet)"] == ""
    )

    quarter_incidents[
        "Contributing Factors (use values from separate worksheet)"
    ] = np.where(
        cf_null,
        "Other - Provide Additional Details",
        quarter_incidents["Contributing Factors (use values from separate worksheet)"],
    )

    quarter_incidents[
        "Actions Taken (use values from separate worksheet)"
    ] = np.where(
        details_null,
        "Other - Provide Additional Details",
        quarter_incidents["Actions Taken (use values from separate worksheet)"],
    )

    final_cols = [
        "Member ID",
        "Indicator Type - v1",
        "Contract Number",
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


def med_errors(quarter=None, year=None):
    helpers = Helpers(db_filepath)

    if quarter is None:
        params = helpers.last_quarter()
        quarter, year = helpers.last_quarter(return_q=True)
    else:
        params = helpers.get_quarter_dates(quarter, year)

    query = """
        SELECT med_errors.*, c.center FROM med_errors
        JOIN enrollment e on med_errors.member_id=e.member_id
        JOIN centers c on e.member_id = c.member_id
        WHERE date_discovered BETWEEN ? and date(?, '+1 day')
        AND (order_written_correctly = 1
        OR order_written_correctly='Unknown')
        """

    quarter_incidents = rename_columns(helpers.dataframe_query(query, params))
    quarter_incidents = create_tag_cols(quarter_incidents)
    quarter_incidents = map_location_and_center(quarter_incidents)

    final_df = create_csv(quarter_incidents)

    other_contr_filter = (
        final_df["Contributing Factors (use values from separate worksheet)"]
        != "Other - Provide Additional Details"
    )
    final_df["Other Contributing Factor"] = np.where(
        other_contr_filter, "", final_df["Other Contributing Factor"]
    )

    other_action_filter = (
        final_df["Actions Taken (use values from separate worksheet)"]
        != "Other - Provide Additional Details"
    )
    final_df["Other Action"] = np.where(
        other_action_filter, "", final_df["Other Action"]
    )

    final_df.to_csv(
        f"{filepath}\\{year}Q{quarter}\\hpms_med_errors_Q{quarter}_{year}.csv",
        index=False,
    )
    final_df.drop(["Member ID"], axis=1, inplace=True)

    final_df = final_df[
        final_df["Type of Medication Error (select from dropdown)"] != ""
    ]

    final_pvd = final_df[
        final_df["Site Name"] == "PACE Rhode Island - Providence"
    ].copy()
    final_woo = final_df[
        final_df["Site Name"] == "PACE Rhode Island - Woonsocket"
    ].copy()
    final_wes = final_df[final_df["Site Name"] == "PACE Rhode Island - Westerly"].copy()

    tab_cols = ["IB v2.0"] + [""] * 8

    first_row = pd.DataFrame(
        data=[final_df.columns], index=[0], columns=final_df.columns
    )

    final_pvd = first_row.append(final_pvd, sort=False)
    final_woo = first_row.append(final_woo, sort=False)
    final_wes = first_row.append(final_wes, sort=False)

    final_pvd.columns = tab_cols
    final_woo.columns = tab_cols
    final_wes.columns = tab_cols

    final_pvd.to_csv(
        f"{filepath}\\{year}Q{quarter}\\hpms_med_errors_Q{quarter}_{year}_pvd.txt",
        index=False,
        sep="\t",
    )
    final_woo.to_csv(
        f"{filepath}\\{year}Q{quarter}\\hpms_med_errors_Q{quarter}_{year}_woon.txt",
        index=False,
        sep="\t",
    )
    final_wes.to_csv(
        f"{filepath}\\{year}Q{quarter}\\hpms_med_errors_Q{quarter}_{year}_wes.txt",
        index=False,
        sep="\t",
    )

    return "Med Errors Complete!"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--quarter", default=None, help="Number of quarter")
    parser.add_argument("--year", default=None, help="Year of quarter")

    arguments = parser.parse_args()

    create_dir_if_needed(**vars(arguments))
    med_errors(**vars(arguments))
