from wealthcentral.jobs.inflight_pcb.job_utils import (client_balance_monthly_12_31,
                                                       client_balance_quarterly_12_31,
                                                       client_balance_monthly_updated_12_31,
                                                       client_balance_quarterly_updated_12_31,
                                                       participant_balance_monthly_12_31,
                                                       participant_balance_quarterly_12_31,
                                                       participant_balance_monthly_updated_12_31,
                                                       participant_balance_quarterly_updated_12_31,
                                                       plan_balance_quarterly_12_31,
                                                       plan_balance_monthly_12_31,
                                                       plan_balance_monthly_updated_12_31,
                                                       plan_balance_quarterly_updated_12_31,
                                                       plan_fund_balance_monthly_12_31,
                                                       plan_fund_balance_monthly_updated_12_31,
                                                       plan_source_balance_monthly_12_31,
                                                       plan_source_balance_monthly_updated_12_31)

class ConsumptionParameters:
    def __init__(self, entity_name, entity_load, primary_keys):
        self.entity_name = entity_name.lower()
        self.entity_load = entity_load.lower()
        self.table_name = entity_name.replace('.', '_').lower()
        if primary_keys:
            self.primary_keys = primary_keys.split(",")
        else:
            self.primary_keys = None


JobNameToJobMapper = {
    "client_balance_quarterly_12_31": client_balance_quarterly_12_31.transform,
    "client_balance_monthly_12_31": client_balance_monthly_12_31.transform,
    "client_balance_monthly_updated_12_31": client_balance_monthly_updated_12_31.transform,
    "client_balance_quarterly_updated_12_31": client_balance_quarterly_updated_12_31.transform,
    "participant_balance_quarterly_12_31": participant_balance_quarterly_12_31.transform,
    "participant_balance_quarterly_updated_12_31": participant_balance_quarterly_updated_12_31.transform,
    "participant_balance_monthly_12_31": participant_balance_monthly_12_31.transform,
    "participant_balance_monthly_updated_12_31": participant_balance_monthly_updated_12_31.transform,
    "plan_balance_quarterly_12_31": plan_balance_quarterly_12_31.transform,
    "plan_balance_quarterly_updated_12_31": plan_balance_quarterly_updated_12_31.transform,
    "plan_balance_monthly_12_31": plan_balance_monthly_12_31.transform,
    "plan_balance_monthly_updated_12_31": plan_balance_monthly_updated_12_31.transform,
    "plan_fund_balance_monthly_12_31": plan_fund_balance_monthly_12_31.transform,
    "plan_fund_balance_monthly_updated_12_31": plan_fund_balance_monthly_updated_12_31.transform,
    "plan_source_balance_monthly_12_31": plan_source_balance_monthly_12_31.transform,
    "plan_source_balance_monthly_updated_12_31": plan_source_balance_monthly_updated_12_31.transform
}
