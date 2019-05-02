import pandas as pd
import numpy as np
from calendar import monthrange
import get_quarter_info as q_info

def up_to_date_question():
    '''Need user to indiated if this is for HPMS reporting or for up
    to date tracking.

    Args:
        None

    Returns: True is up to date report is needed, false if for HPMS.

    '''

    up_to_date = input('Do we need up to date numbers and the list of still needed ppts? (Y or N): ')
    if up_to_date.lower() == 'y':
        return True
    else:
        return False

def load_data(up_to_date=False):
    """Loads csv files and removes Alan Training (fake ppt) from data.

    More information about required csv files and naming conventions can be
    found in reports.txt.

    Args:
        up_to_date: boolean, set to true if report is not for HPMS,
        but for internal up to date data.

    Returns:
        5 or 6 pandas Dataframes depending on boolean arg.
    """
    try:
        pneumo = pd.read_csv('data/pneumo.csv')
        pneumo_contra = pd.read_csv('data/pneumo_contra.csv')

        alvin_training_pneumo = pneumo[pneumo['Patient: Patient ID'] == 1003].index.tolist()
        pneumo.drop(alvin_training_pneumo, axis=0, inplace=True)
        pneumo.reset_index(inplace=True)

    except FileNotFoundError:
        pneumo ='_'
        pneumo_contra ='_'
    try:
        influenza =  pd.read_csv('data/influenza.csv')
        influenza_contra = pd.read_csv('data/influenza_contra.csv')


        alvin_training_influ = influenza[influenza['Patient: Patient ID'] == 1003].index.tolist()
        influenza.drop(alvin_training_influ, axis=0, inplace=True)
        influenza.reset_index(inplace=True)

    except FileNotFoundError:
        influenza = '_'
        influenza_contra = '_'

    roster = pd.read_csv('data/enrollment_details.csv')
    alvin_training_roster = roster[roster.MemberID == 1003].index.tolist()
    roster.drop(alvin_training_roster, axis=0, inplace=True)
    roster.reset_index(inplace=True)

    if up_to_date:
        enrolled = pd.read_csv('data/current_roster.csv')

        alvin_training_enroll = enrolled[enrolled.ParticipantName.str.contains('Training')].index.tolist()
        enrolled.drop(alvin_training_enroll, axis=0, inplace=True)
        enrolled.reset_index(inplace=True)

        return (pneumo, pneumo_contra, influenza, influenza_contra,
                roster, enrolled)

    return pneumo, pneumo_contra, influenza, influenza_contra, roster

def filter_roster(roster, dates):
    """Converts date columns to pd.datetime objects and filters
    the roster to contain only ppts enrolled during the quarter.

    Args:
        roster: enrollment roster for all ppts over all time.
        dates: start and end date of the quarter as strings.

    Returns:
        filtered_roster: pandas DataFrame filtered for ppts
        enrolled during the reporting quarter.
    """

    #convert dates to datetime objects
    roster['DisenrollmentDate'] = pd.to_datetime(roster.DisenrollmentDate)
    roster['EnrollmentDate'] = pd.to_datetime(roster.EnrollmentDate)

    #filter enrollment list for participants enrolled during the quarter
    #this encludes anyone with an enrollment date before the end of the
    #quarter and with a disenroll date during the quarter
    disenrolled_during_quarter = (roster.DisenrollmentDate >= dates[0]) &  (roster.DisenrollmentDate <= dates[1])
    
    currently_enrolled = roster.DisenrollmentDate.isnull()
    enrolled_before_end_of_quarter = (roster.EnrollmentDate <= dates[1])

    #filter enrollment list
    filtered_roster = roster[(disenrolled_during_quarter |
                                currently_enrolled) &
                                enrolled_before_end_of_quarter]
    return filtered_roster

def filter_roster_utd(roster, enrolled):
    """Filters roster using current enrollment roster for up to date reporting.

    Args:
        roster: enrollment roster for all ppts over all time.
        enrolled: roster as of today.

    Returns:
        filtered_roster: pandas DataFrame filtered for current ppts.

    """

    filtered_roster = enrolled.merge(roster, on='ParticipantName', how='left',
                                    suffixes=('_x', ''))
    duplicates = [x for x in filtered_roster.columns if '_x' in x]
    filtered_roster.drop(duplicates, axis=1, inplace=True)

    return filtered_roster

def merge_vacc_data(vacc, filtered_roster):
    """Combines enrollment data and vaccination data into one dataframe.

    Args:
        vacc: pandas Dataframe containing vaccination information.
        filtered_roster: pandas DataFrame filtered for current ppts.

    Returns:
        master_immune: pandas Dataframe ppts information and
        immunization outcomes.
    """

    #to merge vaccination data needs to have ID column changed to match roster
    vacc.rename(columns={'Patient: Patient ID': 'MemberID'}, inplace=True)

    #update date of immunization to datetime object
    vacc['Immunization: Date Administered'] = pd.to_datetime(vacc['Immunization: Date Administered'])

    #build master list of immunization record for ppts
    master_immune = filtered_roster.merge(vacc, on='MemberID', how='left')

    return master_immune


def filter_vacc(master_immune):
    """For the Pneumococcal immunization only, filters out ppts < 65 year old.

    Args:
        master_immune: pandas Dataframe ppts information and
        immunization outcomes.

    Returns:
        master_immune: pandas Dataframe ppts information and
        immunization outcomes filtered to only show ppts older than 65.
    """
    if master_immune['age'].isnull().sum() > 0:
        raise ValueError('Missing Age!')
    master_immune = master_immune[master_immune['age'] >= 65]

    return master_immune

def filter_vacc_utd(master_immune):
    """For the Pneumococcal immunization only, filters out ppts < 65 year old.
    This is for the up to date report as it uses a different age columns name.

    #could be updated to work with filter_vacc, undecided on worth.

    Args:
        master_immune: pandas Dataframe ppts information and
        immunization outcomes.

    Returns:
        master_immune: pandas Dataframe ppts information and
        immunization outcomes filtered to only show ppts older than 65.
    """
    master_immune = master_immune[master_immune['age'] >= 65]
    return master_immune


def immunization_status(master_immune, dates, contra):
    """Creates a column that indicates the ppts immunization status;
    1:Administered//0:Not Administered//-1:Missed//99:allergic

    Will also remvoe any ppts who have a refusal of the immunization after
    already having it administered.

    Args:
        master_immune: pandas Dataframe ppts information and
        immunization outcomes.
        contra: pandas Dataframe of ppts allergic to immunization.

    Returns:
    master_immune: pandas Dataframe ppts information and
    immunization outcomes with immunization_status column.
    """

    #create column that lets us know if the interaction took place during the quarter
    #1: yes//0: no
    master_immune['during'] = np.where(master_immune['Immunization: Date Administered'] >= dates[0], 1, 0)

    #encode our dose status column as 1:Administered//0:Not Administered//-1:Missed
    master_immune['immunization_status'] = np.where(master_immune['Immunization: Dose Status'] ==  'Not Administered', 0, 1)
    master_immune['immunization_status'] = np.where(master_immune['Immunization: Dose Status'] ==  'Administered', 1, 0)
    master_immune['immunization_status'] = np.where(master_immune['Immunization: Dose Status'].isnull(), -1, master_immune['immunization_status'] )



    #need to check if any ppts who come in as not administered are that way because they have had the vaccine administered previously
    #this would mean they count as prior admin, not as not administered

    #list of ppts who have had the vaccine
    admin_members = master_immune[master_immune['immunization_status']==1].MemberID.unique()

    #list of ppts who have had a not administered interaction
    not_admin_members = master_immune[master_immune['immunization_status']==0].MemberID

    #check for any ppts who have had both a not administered & administered interaction
    not_admin_now_admin = not_admin_members[not_admin_members.isin(admin_members)].values

    #if there are any, this will remove their not administered interaction from our dataset
    mask = ((master_immune.MemberID.isin(not_admin_now_admin)) &
            (master_immune['immunization_status']==0))

    if len(not_admin_now_admin) != 0:
            master_immune = master_immune.drop(master_immune[mask].index)

    #change does status of ppts alergic to the vaccine
    #print(contra['Patient: Patient ID'].values)

    master_immune['immunization_status'] = np.where(master_immune['MemberID'].isin(contra['Patient: Patient ID']), -0.5, master_immune['immunization_status'])
    
    return master_immune

def create_hpms_file(master_immune, file_name):
    """Creates pandas dataframe and csv file with HPMS reportable numbers.

    Args:
        master_immune: pandas Dataframe ppts information and
        immunization outcomes.

    Returns:
        hpms_df: pandas dataframe formated for HPMS input & saves csv of df.
    """
    #use a group by to sum deal with the multiple interactions per ppt
    group_by = master_immune.groupby(['Center', 'MemberID']).sum()[['immunization_status', 'during']]

    #create final result dataframe
    df = {}

    for center in master_immune.Center.unique():
        df[center] = {}

        group_loc = group_by.loc[center]

        eligible = group_loc.shape[0]
        df[center]['eligible'] = eligible

        received_during = group_loc[(group_loc['immunization_status'] > 0) &
                                    (group_loc['during'] > 0)].shape[0]
        df[center]['received_immunization'] = received_during

        received_prior = group_loc[(group_loc['immunization_status'] > 0) &
                            (group_loc['during'] == 0)].shape[0]
        df[center]['prior_immunization'] = received_prior

        refused = group_loc[(group_loc['immunization_status'] == 0)].shape[0]
        df[center]['refused'] = refused

        contraindicated = group_loc[(group_loc['immunization_status'] == -0.5)].shape[0]
        df[center]['medically_contraindicated'] = contraindicated

        missed = group_loc[(group_loc['immunization_status'] == -1)].shape[0]
        df[center]['missed_opportunity'] = missed

        success_rate = ((received_during + received_prior) / eligible) * 100
        df[center]['success_rate'] = success_rate

    hpms_df = pd.DataFrame.from_dict(df)
    hpms_df['Total'] = hpms_df.sum(axis=1)

    total_success_rate = ((hpms_df.at['prior_immunization', 'Total'] +
                            hpms_df.at['received_immunization', 'Total']) /
                            hpms_df.at['eligible', 'Total'])
    hpms_df.at['success_rate', 'Total'] = total_success_rate

    hpms_df.to_csv(file_name, index=True)

    return hpms_df

def create_still_needed(master_immune, filename):
    """Create pandas dataframe of up to date ppts who still need immunization.

    Args:
        master_immune: pandas Dataframe ppts information and
        immunization outcomes.

    Returns:
        still_need_immune: pandas dataframe of up to date ppts who
        still need immunization.
    """

    still_need_immune = master_immune[(master_immune.immunization_status == 0)|
                            (master_immune.immunization_status == -1)].copy()
    not_needed_cols = ['MedicaidEligibilty', 'MedicareEligibility',
                       'ZipCode', 'AgeGroup', 'SSN', 'DeathDate',
                       'DisenrollmentDate', 'Other', 'Patient: Patient Name',
                       'Patient: Date Of Birth', 'Patient: Age',
                        'during']

    still_need_immune.drop(not_needed_cols, axis=1, inplace=True)
    still_need_immune.drop_duplicates(subset='MemberID', inplace=True)
    still_need_immune.to_csv(filename, index=False)

    return still_need_immune

if __name__ == "__main__":
    if up_to_date_question():
        Q, year = q_info.get_Q()
        dates = q_info.get_Q_dates(Q, year)
        pneumo, pneumo_contra, influenza, influenza_contra, roster, enrolled =  load_data(up_to_date=True)
        filtered_roster = filter_roster_utd(roster, enrolled)

        if type(pneumo) != str:
            master_immune = merge_vacc_data(pneumo, filtered_roster)
            master_immune = filter_vacc_utd(master_immune)
            master_immune = immunization_status(master_immune, dates, pneumo_contra)
            create_hpms_file(master_immune, 'pneumo_today.csv')
            create_still_needed(master_immune, 'pneumo_needed.csv')

        if type(influenza) != str:
            master_immune = merge_vacc_data(influenza, filtered_roster)
            master_immune = immunization_status(master_immune, dates, influenza_contra)
            create_hpms_file(master_immune, 'influ_today.csv')
            create_still_needed(master_immune, 'influ_needed.csv')

        print('Done')

    else:
        Q, year = q_info.get_Q()
        dates = q_info.get_Q_dates(Q, year)
        pneumo, pneumo_contra, influenza, influenza_contra, roster = load_data()
        filtered_roster = filter_roster(roster, dates)

        if type(pneumo) != str:
            master_immune = merge_vacc_data(pneumo, filtered_roster)
            master_immune = filter_vacc(master_immune)
            master_immune = immunization_status(master_immune, pneumo_contra)
            create_hpms_file(master_immune, file_name = 'hpms_pneumo.csv')

        if type(influenza) != str:
            master_immune= merge_vacc_data(influenza, filtered_roster)
            master_immune = immunization_status(master_immune, influenza_contra)
            create_hpms_file(master_immune, file_name = 'hpms_influenza.csv')

        print('Done')
