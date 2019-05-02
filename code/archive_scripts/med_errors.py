import pandas as pd
import numpy as np
from calendar import monthrange
import get_quarter_info as q_info

def load_and_filter(dates):
    """Loads medical incidents, removes training ppt and filters for quarter
    data only.

    Args:
        dates: tuple of start and end dates of reporting month.

    Returns:
        pandas DataFrame of medical incidents
    """
    med_incidents = pd.read_csv('data/incident_med_errors.csv')

    alvin_training_index = med_incidents[med_incidents['Member ID'] == 1003].index.tolist()
    med_incidents.drop(alvin_training_index, axis=0, inplace=True)
    med_incidents.reset_index(inplace=True)

    med_incidents['Date Discovered'] = pd.to_datetime(med_incidents['Date Discovered'])

    quarter_mask = ((med_incidents['Date Discovered'] >= dates[0]) &
                    (med_incidents['Date Discovered'] <= dates[1]))
    return med_incidents[quarter_mask]

def map_binary(quarter_incidents):
    """Maps columns that have Yes/No reponse to binary.

    Args:
        quarter_incidents: pandas DataFrame of medical incidents
        filtered for quarter.

    Returns: quarter_incidents with yes/no columns mapped to binary.
    """

    binary_cols = ['Responsibility - Pharmacy',
                   'Responsibility - Clinic',
                   'Responsibility - Home Care',
                   'Responsibility - Facility',
                   'Responsible Facility Name',
                   'Wrong Dose',
                   'Wrong Dose (not administered)',
                   'Wrong Med/Tx',
                   'Wrong Med/Tx (not administered)',
                   'Wrong PPT',
                   'Wrong PPT (not administered)',
                   'Wrong Route',
                   'Wrong Label (not administered)',
                   'Wrong Time/Day',
                   'Dose Omitted (not administered)',
                   'Expired Order',
                   'Transcription',
                   'Not Ordered',
                   'Med given despite hold order (VS)',
                   'Med/Tx given with known allergy',
                   'Med/Tx given beyond stop date',
                   'Change in Delivery Method',
                   'Change in Pharmacy',
                   'Communication with Inpatient Hospice',
                   'Communication with ACS',
                   'Communication with ALF',
                   'Communication with Hospital',
                   'Communication with Nursing Facility',
                   'Communication with Pharmacy',
                   'Administered by Unauthorized Staff',
                   'New Staff Member',
                   'Order Transcription Error',
                   'Participant ID Error',
                   'Pharmacy Error',
                   'Physician Prescription Error',
                   'Similar Name',
                   'Staff Error',
                   'Family Education',
                   'Home Care Assessment',
                   'Implemented Falls Prevention Program',
                   'Implemented New Policy',
                   'Increased OT/PT',
                   'Initiated Contractor Oversight',
                   'Initiated Quality Improvement Activities',
                   'Medication Evaluation/Change',
                   'Modified ALF Environment',
                   'Modified Hospital Environment',
                   'Modified NF Environment',
                   'Modified PACE Center Environment',
                   'Modified PPT Home Environment',
                   'OT Assessment',
                   'PCP Assessment',
                   'PPT Education',
                   'PT Assessment',
                   'Revised Existing Policy',
                   'RN Assessment',
                   'Staff Education',
                   'Increase Home Care',
                   'Increased Center Attendance',
                   'Change in Contracted Provider',
                   'Change to Medication Administration Process',
                   'Change to Participant Identification Process',
                   'Changes to Medication Prescription Process',
                   'Changes to Medication Transcription Process',
                   'Implemented a New Medication Delivery System',
                   'Requested a Corrective Action Plan from Contracted Provider']

    binary_map = {'Yes' : 1, 'No' : 0, np.nan : np.nan}

    return pd.concat([quarter_incidents[quarter_incidents.columns[~quarter_incidents.columns.isin(binary_cols)]],
                        quarter_incidents[binary_cols].applymap(lambda x: binary_map[x])],
                        axis=1)


def rename_columns(quarter_incidents, return_maps = False):
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
    contributing_map = {'Change in Delivery Method' : 'Change in Method of Delivery',
       'Change in Pharmacy' : 'Change in Pharmacy Provider',
       'Communication with Inpatient Hospice' : 'Communication between PACE Inpatient Hospice',
       'Communication with ACS' : 'Communication between PACE Organization and ACS',
       'Communication with ALF' : 'Communication between PACE Organization and Assisted Living Facility',
       'Communication with Hospital' : 'Communication between PACE Organization and Hospital',
       'Communication with Nursing Facility' : 'Communication between PACE Organization and Nursing Facility',
       'Communication with Pharmacy' : 'Communication between PACE Organization and Pharmacy',
       'Administered by Unauthorized Staff' : 'Medication Administered by staff not Permitted to Administer Medication',
       'New Staff Member' : 'New Staff Member',
       'Order Transcription Error' : 'Order Transcription Error',
       'Participant ID Error' : 'Participant ID Error',
       'Pharmacy Error' : 'Pharmacy Error',
       'Physician Prescription Error' : 'Physician Prescription Error',
       'Similar Name' : 'Similar Name',
       'Staff Error' : 'Staff Error',
       'Other Contributing Factor' : 'Other - Provide Additional Details'}

    med_error_map = {'Wrong Dose' : 'Medication Administered - Incorrect Dose',
       'Wrong Dose (not administered)' : 'Medication not Administered - Incorrect Dose Dispensed to Participant',
       'Wrong Med/Tx' : 'Medication Administered - Incorrect Medication',
       'Wrong Med/Tx (not administered)' : 'Medication not Administered - Incorrect Medication Dispensed to Participant',
       'Wrong PPT' : 'N/A',
       'Wrong PPT (not administered)' : 'Medication not Administered - Dispensed to wrong Participant',
       'Wrong Route' : 'Medication Administered - Incorrect Route',
       'Wrong Label (not administered)' : 'Medication not Administered - Medication Incorrectly Labeled',
       'Wrong Time/Day' : 'Medication Administered - Incorrect Time',
       'Dose Omitted (not administered)' : 'Medication not Administered - Dose Omitted'}

    measures_map = {'Implemented New Policy' : 'Implemented a New Policy',
       'Increase Home Care' : 'Increase Home Care',
       'Change to Medication Administration Process' : 'Change to Medication Administration Process',
       'Changes to Medication Transcription Process' : 'Changes to Medication Transcription Process',
       'Initiated Contractor Oversight' : 'Implemented Additional Contractor Oversight',
       'Revised Existing Policy' : 'Amended Current Policy',
       'Increased Center Attendance' : 'Increased Center Attendance',
       'Staff Education' : 'Staff Education',
       'Change to Participant Identification Process' : 'Change to Participant Identification Process',
       'PCP Assessment' : 'PCP Assessment',
       'Requested a Corrective Action Plan from Contracted Provider' : 'Requested a Corrective Action Plan from Contracted Provider',
       'Changes to Medication Prescription Process' : 'Changes to Medication Prescription Process',
       'Implemented a New Medication Delivery System' : 'Implemented a New Medication Delivery System',
       'Initiated Quality Improvement Activities' : 'Implemented Quality Improvement Activities',
       'Change in Contracted Provider' : 'Change in Contracted Provider',
       'RN Assessment' : 'RN Assessment'}

    if return_maps:
        return contributing_map, med_error_map, measures_map
    rename_cols = {**contributing_map, **med_error_map, **measures_map}

    return quarter_incidents.rename(columns=rename_cols)

def map_and_rename(quarter_incidents):
    """Wrapper function to map binary columns and rename columns
    to HPMS approved indicators.

    Args:
        quarter_incidents: pandas DataFrame of medical incidents
        filtered for quarter.

    Returns: quarter_incidents with yes/no columns mapped to binary
    and factor/measures columns renamed to match HPMS language.
    """
    return rename_columns(map_binary(quarter_incidents))

def create_tag_cols(quarter_incidents):
    """Collects factors/measures into binned columns.

    Args:
        quarter_incidents: pandas DataFrame of medical incidents
        filtered for quarter.
    Returns:
        quarter_incidents with columns containing the tags
        for errors, measures, and contributing factors.
    """

    contributing_map, med_error_map, measures_map = rename_columns(quarter_incidents,
                                                        return_maps = True)
    quarter_incidents['med_error_tags'] = ''

    for col_name in list(med_error_map.values()):
        indicies = quarter_incidents.loc[quarter_incidents[col_name]==1].index.tolist()
        for i in indicies:
            quarter_incidents.at[i, 'med_error_tags'] += col_name

    quarter_incidents['measures_tags'] = ''

    for col_name in list(measures_map.values()):
        indicies = quarter_incidents.loc[quarter_incidents[col_name]==1].index.tolist()
        for i in indicies:
            if quarter_incidents.at[i, 'measures_tags'] == '':
                quarter_incidents.at[i, 'measures_tags'] += col_name
            else:
                addional_tags = ', ' + col_name
                quarter_incidents.at[i, 'measures_tags'] += addional_tags

    quarter_incidents['contributing_tags'] = ''

    for col_name in list(contributing_map.values()):
        indicies = quarter_incidents.loc[quarter_incidents[col_name]==1].index.tolist()
        for i in indicies:
            if quarter_incidents.at[i, 'contributing_tags'] == '':
                quarter_incidents.at[i, 'contributing_tags'] += col_name
            else:
                addional_tags = ', ' + col_name
                quarter_incidents.at[i, 'contributing_tags'] += addional_tags

    return quarter_incidents


def map_location_and_center(quarter_incidents):
    """Maps location and center coloumns to HPMS language.

    Args:
        quarter_incidents: pandas DataFrame of medical incidents
        filtered for quarter.

    Returns:
        quarter_incidents with renamned column names.
    """

    location_map = {'ADHC/Clinic':'PACE Center',
        'Alternative Care Setting':'Alternative Care Setting',
        'Assisted Living Facility/PCBH':'Assisted Living Facility',
        'Home/Independent Living':'Participant Home',
        'Hospital':'Hospital',
        'Inpatient Hospice':'Inpatient Hospice',
        'Nursing Home':'Nursing Facility'}

    quarter_incidents['Location'] = quarter_incidents['Location'].map(lambda x: location_map[x])

    center_map = {'Providence' : 'PACE Rhode Island - Providence',
              'Woonsocket' : 'PACE Rhode Island - Woonsocket',
              'Westerly' : 'PACE Rhode Island - Westerly'}

    quarter_incidents['Center'] = quarter_incidents['Center'].map(lambda x: center_map[x])

    return quarter_incidents

def create_csv(quarter_incidents, Q):
    """Createst csv of med_error incidents for HPMS.

    Args:
        quarter_incidents: pandas DataFrame of medical incidents
        filtered for quarter.
        Q: quarter number as int

    Returns:
        None.
    """
    final_cols_map = {'Center' : 'Site Name',
                  'Location' : 'Location of Incident (select from dropdown)',
                  'med_error_tags' : 'Type of Medication Error (select from dropdown)',
                  'contributing_tags' : 'Contributing Factors (use values from separate worksheet)',
                  'Other - Provide Additional Details' : 'Other Contributing Factor',
                  'measures_tags' : 'Actions Taken (use values from separate worksheet)',
                  'Comments' : 'Other Action'}

    quarter_incidents.rename(columns=final_cols_map, inplace=True)

    final_cols = ['Member ID', 'Site Name',
                'Location of Incident (select from dropdown)',
                'Type of Medication Error (select from dropdown)',
                'Contributing Factors (use values from separate worksheet)',
                'Other Contributing Factor',
                'Actions Taken (use values from separate worksheet)',
                'Other Action']

    final_med_incidents = quarter_incidents[final_cols].copy()
    file_name = 'med_error_Q' + str(Q)+'.csv'
    final_med_incidents.to_csv(file_name, index=False)

if __name__ == "__main__":
    Q, year = q_info.get_Q()
    dates = q_info.get_Q_dates(Q, year)
    quarter_incidents = load_and_filter(dates)
    quarter_incidents = map_and_rename(quarter_incidents)
    quarter_incidents = create_tag_cols(quarter_incidents)
    quarter_incidents = map_location_and_center(quarter_incidents)
    create_csv(quarter_incidents, Q)
