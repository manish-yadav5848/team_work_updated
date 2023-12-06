from wealthcentral.jobs.ssn.job_utils import (participant_core_balance_monthly,
                                              participant_balance_quarterly,
                                              part_fund_loan_quarterly,
                                              part_fund_loan_monthly,
                                              participant_balance_monthly)

class FileParameters:
    def __init__(self, entity_name, primary_keys, database_name):
        self.entity_name = entity_name.lower(),
        self.primary_keys = primary_keys.split(",")
        self.database_name = database_name.lower()



JobNameToJobMapper = {
    "participant_core_balance_monthly": participant_core_balance_monthly.transform,
    "participant_balance_monthly": participant_balance_monthly.transform,
    "participant_balance_quarterly": participant_balance_quarterly.transform,
    "part_fund_loan_monthly": part_fund_loan_monthly.transform,
    "part_fund_loan_quarterly": part_fund_loan_quarterly.transform
}