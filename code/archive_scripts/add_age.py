import pandas as pd

def add_age():
    enrolled = pd.read_csv('data/enrolled_ppts.csv')

    disenrolled = pd.read_csv('data/disenrolled_ppts.csv')

    enrollment_details = pd.read_csv('data/enrollment_details.csv')

    birth_info = enrolled.append(disenrolled)

    birth_info['BirthDate'] = pd.to_datetime(birth_info.BirthDate)
    birth_info = birth_info[['MemberID', 'BirthDate']]

    birth_info['age'] = (pd.datetime.today() - birth_info['BirthDate']).astype('<m8[Y]')
    birth_info.drop('BirthDate', axis=1, inplace=True)

    enrollment_details = enrollment_details.merge(birth_info, on='MemberID', how='left')

    enrollment_details.to_csv('data/enrollment_details.csv', index=False)
    print('Age added to enrollment.')
    
if __name__ == "__main__":
    add_age()
