"""Creates csv file of quarterly HPMS reporting.

See report.txt for information on required csv files from Cognify and
naming conventions for script to run correctly.

"""

import enrollment as en
import immunization as im
import med_errors as md
import add_age as ad
import get_quarter_info as q_info
import pandas as pd

if __name__ == "__main__":
    introduction = '''This will output 4 csv files for HPMS reporting.
                    It covers;
                    HPMS Enrollment
                    HPMS Influenza
                    HPMS Pneumococcal
                    PMS Med Errors
                    Please ensure all files are in the folder, saved as csv,
                    and named as follows;
                    pneumo or influenza
                    pneumo_contra or influenza_contra
                    enrollment_details
                    incident_med_errors
                    If you need information on where to find the reports,
                    open reports.txt.
                   '''
    print(introduction)
    ad.add_age()
    Q, year = q_info.get_Q()
    print('Here we go...')
    dates = q_info.get_Q_dates(Q, year)

    pneumo, pneumo_contra, influenza, influenza_contra, roster = im.load_data()

    filtered_roster = im.filter_roster(roster, dates)
    census_df = en.census(filtered_roster)
    enrolled_df = en.enroll_disenroll_data(filtered_roster, dates,
                                            enroll=True)
    disenrolled_df = en.enroll_disenroll_data(filtered_roster, dates,
                                            enroll=False)
    deaths_df = en.deaths_in_quarter(filtered_roster, dates)

    row_names = pd.Series(['Census', 'Enrollments', 'Medicare',
                            'Dual Eligible', 'Medicaid', 'Private Pay',
                            'Disenrollments', 'Medicare', 'Dual Eligible',
                            'Medicaid', 'Private Pay', 'Deaths'])

    final_enrollment_df = (pd.concat([census_df, enrolled_df,
                            disenrolled_df, deaths_df]))
    final_enrollment_df.set_index(row_names, inplace=True)
    final_enrollment_df.to_csv('hpms_enrollment.csv', index=True)

    if type(pneumo) != str:
        master_immune = im.merge_vacc_data(pneumo, filtered_roster)
        master_immune = im.filter_vacc(master_immune)
        master_immune = im.immunization_status(master_immune, dates, pneumo_contra)
        master_immune.to_csv('immunization_pneumo.csv')
        im.create_hpms_file(master_immune, file_name = 'hpms_pneumo.csv')

    if type(influenza) != str:
        master_immune= im.merge_vacc_data(influenza, filtered_roster)
        master_immune = im.immunization_status(master_immune, dates, influenza_contra)
        master_immune.to_csv('immunization_influ.csv')
        im.create_hpms_file(master_immune, file_name = 'hpms_influenza.csv')


    quarter_incidents = md.load_and_filter(dates)
    quarter_incidents = md.map_and_rename(quarter_incidents)
    quarter_incidents = md.create_tag_cols(quarter_incidents)
    quarter_incidents = md.map_location_and_center(quarter_incidents)
    md.create_csv(quarter_incidents, Q)


    print("...and we're done")
