from wealthcentral.jobs.transform_to_consumption.job_utils import (
    part_source_balance_rollup,
    client_balance_monthly,
    client_balance_quarterly,
    client_balance,
    div_sub_balance,
    div_sub_fund_balance,
    div_sub_fund_balance_daily,
    div_sub_fund_source_balance,
    div_sub_source_balance,
    div_sub_source_balance_daily,
    nq_client_balance,
    nq_participant_balance,
    nq_participant_balance_monthly,
    nq_participant_balance_quarterly,
    nq_participant_source_balance,
    nq_plan_balance,
    nq_plan_balance_monthly,
    nq_plan_balance_quarterly,
    nq_plan_source_balance,
    participant_balance_monthly,
    participant_balance_quarterly,
    part_fund_loan_monthly,
    part_fund_loan_quarterly,
    part_fund_source_txn,
    part_gaa_fund_bal_monthly,
    part_gaa_fund_bal_quarterly,
    part_source_txn_summary,
    participant_balance,
    participant_fund_balance,
    participant_source_txn,
    participant_txn_summary,
    plan_balance,
    plan_balance_monthly,
    plan_balance_quarterly,
    plan_div_sub_txn_summary,
    plan_divsub_source_txn_summary,
    plan_fund_balance,
    plan_fund_source_balance,
    plan_fund_balance_monthly,
    plan_source_balance_monthly,
    plan_loan_rollup,
    plan_source_balance,
    plan_source_txn,
    plan_source_txn_summary,
    plan_source_type,
    plan_txn_summary,
    producer_participant_balance,
    producer_plan_balance,
    producer_plan_fund_balance,
    producer_plan_fund_source_balance,plan_fund_balance_daily,plan_source_balance_daily
)


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
    "part_source_balance_rollup": part_source_balance_rollup.transform,
    "client_balance_monthly": client_balance_monthly.transform,
    "client_balance_quarterly": client_balance_quarterly.transform,
    "client_balance": client_balance.transform,
    "div_sub_balance": div_sub_balance.transform,
    "div_sub_fund_balance": div_sub_fund_balance.transform,
    "div_sub_fund_balance_daily": div_sub_fund_balance_daily.transform,
    "div_sub_fund_source_balance": div_sub_fund_source_balance.transform,
    "div_sub_source_balance": div_sub_source_balance.transform,
    "div_sub_source_balance_daily": div_sub_source_balance_daily.transform,
    "nq_client_balance": nq_client_balance.transform,
    "nq_participant_balance": nq_participant_balance.transform,
    "nq_participant_balance_monthly": nq_participant_balance_monthly.transform,
    "nq_participant_balance_quarterly": nq_participant_balance_quarterly.transform,
    "nq_participant_source_balance": nq_participant_source_balance.transform,
    "nq_plan_balance": nq_plan_balance.transform,
    "nq_plan_balance_monthly": nq_plan_balance_monthly.transform,
    "nq_plan_balance_quarterly": nq_plan_balance_quarterly.transform,
    "nq_plan_source_balance": nq_plan_source_balance.transform,
    "participant_balance_monthly": participant_balance_monthly.transform,
    "participant_balance_quarterly": participant_balance_quarterly.transform,
    "part_fund_loan_monthly": part_fund_loan_monthly.transform,
    "part_fund_loan_quarterly": part_fund_loan_quarterly.transform,
    "part_fund_source_txn": part_fund_source_txn.transform,
    "part_gaa_fund_bal_monthly": part_gaa_fund_bal_monthly.transform,
    "part_gaa_fund_bal_quarterly": part_gaa_fund_bal_quarterly.transform,
    "part_source_txn_summary": part_source_txn_summary.transform,
    "participant_balance": participant_balance.transform,
    "participant_fund_balance": participant_fund_balance.transform,
    "participant_source_txn": participant_source_txn.transform,
    "participant_txn_summary": participant_txn_summary.transform,
    "plan_balance": plan_balance.transform,
    "plan_balance_monthly": plan_balance_monthly.transform,
    "plan_balance_quarterly": plan_balance_quarterly.transform,
    "plan_div_sub_txn_summary": plan_div_sub_txn_summary.transform,
    "plan_divsub_source_txn_summary": plan_divsub_source_txn_summary.transform,
    "plan_fund_balance": plan_fund_balance.transform,
    "plan_fund_balance_monthly": plan_fund_balance_monthly.transform,
    "plan_fund_source_balance": plan_fund_source_balance.transform,
    "plan_loan_rollup": plan_loan_rollup.transform,
    "plan_source_balance": plan_source_balance.transform,
    "plan_source_balance_monthly": plan_source_balance_monthly.transform,
    "plan_source_txn": plan_source_txn.transform,
    "plan_source_txn_summary": plan_source_txn_summary.transform,
    "plan_source_type": plan_source_type.transform,
    "plan_txn_summary": plan_txn_summary.transform,
    "producer_participant_balance": producer_participant_balance.transform,
    "producer_plan_balance": producer_plan_balance.transform,
    "producer_plan_fund_balance": producer_plan_fund_balance.transform,
    "producer_plan_fund_source_balance": producer_plan_fund_source_balance.transform,
    "plan_fund_balance_daily" :plan_fund_balance_daily.transform,
    "plan_source_balance_daily" :plan_source_balance_daily.transform,
}