schema = {
  "base_transaction_code": {
    "type": "string",
    "nullable": True,
    "start": 1,
    "end": 3
  },
  "client_id": {
    "type": "string",
    "nullable": True,
    "start": 4,
    "end": 5
  },
  "plan_number": {
    "type": "string",
    "nullable": True,
    "start": 6,
    "end": 11
  },
  "participant_id": {
    "type": "string",
    "nullable": True,
    "start": 12,
    "end": 20
  },
  "trade_date": {
    "type": "string",
    "nullable": True,
    "start": 21,
    "end": 28
  },
  "run_date": {
    "type": "string",
    "nullable": True,
    "start": 29,
    "end": 36
  },
  "run_time": {
    "type": "string",
    "nullable": True,
    "start": 37,
    "end": 40
  },
  "sequence_number": {
    "type": "string",
    "nullable": True,
    "start": 41,
    "end": 45
  },
  "distribution_type": {
    "type": "string",
    "nullable": True,
    "start": 46,
    "end": 47
  },
  "disbursement_reason": {
    "type": "string",
    "nullable": True,
    "start": 48,
    "end": 49
  },
  "distribution_amount": {
    "type": "string",
    "nullable": True,
    "start": 50,
    "end": 62
  },
  "disbursement_reversal_date": {
    "type": "string",
    "nullable": True,
    "start": 63,
    "end": 70
  },
  "alternate_payee_bank_account_number": {
    "type": "string",
    "nullable": True,
    "start": 71,
    "end": 87
  },
  "eligible_rollover_distribution_amount": {
    "type": "string",
    "nullable": True,
    "start": 88,
    "end": 100
  },
  "total_value_to_be_rolled": {
    "type": "string",
    "nullable": True,
    "start": 101,
    "end": 113
  },
  "cash_value_to_be_rolled": {
    "type": "string",
    "nullable": True,
    "start": 114,
    "end": 126
  },
  "number_of_shares_to_be_rolled": {
    "type": "string",
    "nullable": True,
    "start": 127,
    "end": 136
  },
  "market_value_of_shares_to_be_rolled": {
    "type": "string",
    "nullable": True,
    "start": 137,
    "end": 149
  },
  "cost_value_of_shares_to_be_rolled": {
    "type": "string",
    "nullable": True,
    "start": 150,
    "end": 162
  },
  "tran_code_1": {
    "type": "string",
    "nullable": True,
    "start": 163,
    "end": 165
  },
  "usage_code_group_level": {
    "type": "string",
    "nullable": True,
    "start": 166,
    "end": 175
  },
  "other_share": {
    "type": "string",
    "nullable": True,
    "start": 176,
    "end": 189
  },
  "other_percent": {
    "type": "string",
    "nullable": True,
    "start": 190,
    "end": 203
  },
  "other_numeric": {
    "type": "string",
    "nullable": True,
    "start": 204,
    "end": 216
  },
  "other_factor": {
    "type": "string",
    "nullable": True,
    "start": 217,
    "end": 233
  },
  "voucher_number": {
    "type": "string",
    "nullable": True,
    "start": 234,
    "end": 242
  },
  "reversal_reason": {
    "type": "string",
    "nullable": True,
    "start": 243,
    "end": 243
  },
  "trade_type": {
    "type": "string",
    "nullable": True,
    "start": 244,
    "end": 244
  },
  "use_plan_year_flag": {
    "type": "string",
    "nullable": True,
    "start": 245,
    "end": 245
  },
  "associated_activity": {
    "type": "string",
    "nullable": True,
    "start": 246,
    "end": 246
  },
  "sub_activity_code": {
    "type": "string",
    "nullable": True,
    "start": 247,
    "end": 258
  },
  "state_tax_withheld": {
    "type": "string",
    "nullable": True,
    "start": 259,
    "end": 271
  },
  "mandatory_withholding_amount": {
    "type": "string",
    "nullable": True,
    "start": 272,
    "end": 284
  },
  "federal_tax_withheld": {
    "type": "string",
    "nullable": True,
    "start": 285,
    "end": 297
  },
  "excess_distribution_type": {
    "type": "string",
    "nullable": True,
    "start": 298,
    "end": 298
  },
  "related_participant_id": {
    "type": "string",
    "nullable": True,
    "start": 299,
    "end": 315
  },
  "state_code": {
    "type": "string",
    "nullable": True,
    "start": 316,
    "end": 317
  },
  "five_year_participation_indicator": {
    "type": "string",
    "nullable": True,
    "start": 318,
    "end": 318
  },
  "percent_of_split": {
    "type": "string",
    "nullable": True,
    "start": 319,
    "end": 324
  },
  "lump_sum_distribution_indicator": {
    "type": "string",
    "nullable": True,
    "start": 325,
    "end": 325
  },
  "qvec_distribution_indicator": {
    "type": "string",
    "nullable": True,
    "start": 326,
    "end": 326
  },
  "federal_distribution_type": {
    "type": "string",
    "nullable": True,
    "start": 327,
    "end": 327
  },
  "federal_averaging_benefit_exclusion_code": {
    "type": "string",
    "nullable": True,
    "start": 328,
    "end": 328
  },
  "tran_reason_code": {
    "type": "string",
    "nullable": True,
    "start": 329,
    "end": 329
  },
  "check_delivery_method": {
    "type": "string",
    "nullable": True,
    "start": 330,
    "end": 330
  },
  "plan_code": {
    "type": "string",
    "nullable": True,
    "start": 331,
    "end": 331
  },
  "capital_gains_distribution_federal": {
    "type": "string",
    "nullable": True,
    "start": 332,
    "end": 343
  },
  "ordinary_income_distributed_federal": {
    "type": "string",
    "nullable": True,
    "start": 344,
    "end": 355
  },
  "nontaxable_employee_contributions_earnings_distributed_federal": {
    "type": "string",
    "nullable": True,
    "start": 356,
    "end": 367
  },
  "unrealized_gain_on_employer_securities_federal": {
    "type": "string",
    "nullable": True,
    "start": 368,
    "end": 379
  },
  "percent_of_federal_tax_withheld": {
    "type": "string",
    "nullable": True,
    "start": 380,
    "end": 385
  },
  "federal_withholding_override_flag": {
    "type": "string",
    "nullable": True,
    "start": 386,
    "end": 386
  },
  "federal_withholding_override_amount": {
    "type": "string",
    "nullable": True,
    "start": 387,
    "end": 398
  },
  "federal_tax_override_percent": {
    "type": "string",
    "nullable": True,
    "start": 399,
    "end": 404
  },
  "federal_marital_status_override": {
    "type": "string",
    "nullable": True,
    "start": 405,
    "end": 405
  },
  "federal_tax_exemptions_override": {
    "type": "string",
    "nullable": True,
    "start": 406,
    "end": 408
  },
  "state_marital_status_override": {
    "type": "string",
    "nullable": True,
    "start": 409,
    "end": 409
  },
  "electronic_payment_indicator": {
    "type": "string",
    "nullable": True,
    "start": 410,
    "end": 410
  },
  "state_withholding_override_flag": {
    "type": "string",
    "nullable": True,
    "start": 411,
    "end": 411
  },
  "state_withholding_override_amount": {
    "type": "string",
    "nullable": True,
    "start": 412,
    "end": 423
  },
  "state_withholding_override_percent": {
    "type": "string",
    "nullable": True,
    "start": 424,
    "end": 429
  },
  "percent_of_state_tax_withheld": {
    "type": "string",
    "nullable": True,
    "start": 430,
    "end": 435
  },
  "local_withholding_amount": {
    "type": "string",
    "nullable": True,
    "start": 436,
    "end": 447
  },
  "local_withholding_override_percent": {
    "type": "string",
    "nullable": True,
    "start": 448,
    "end": 453
  },
  "local_withholding_override_flag": {
    "type": "string",
    "nullable": True,
    "start": 454,
    "end": 454
  },
  "local_withholding_override_amount": {
    "type": "string",
    "nullable": True,
    "start": 455,
    "end": 466
  },
  "local_withholding_percent": {
    "type": "string",
    "nullable": True,
    "start": 467,
    "end": 472
  },
  "local_marital_status_override": {
    "type": "string",
    "nullable": True,
    "start": 473,
    "end": 473
  },
  "qvec_distribution_amount": {
    "type": "string",
    "nullable": True,
    "start": 474,
    "end": 485
  },
  "federal_tax_withheld_on_qvec_distribution": {
    "type": "string",
    "nullable": True,
    "start": 486,
    "end": 497
  },
  "state_tax_withheld_on_qvec_distribution": {
    "type": "string",
    "nullable": True,
    "start": 498,
    "end": 509
  },
  "dividend_amount_distrbuted": {
    "type": "string",
    "nullable": True,
    "start": 510,
    "end": 521
  },
  "check_number": {
    "type": "string",
    "nullable": True,
    "start": 522,
    "end": 530
  },
  "udf_code_3": {
    "type": "string",
    "nullable": True,
    "start": 531,
    "end": 534
  },
  "value_of_shares_sold": {
    "type": "string",
    "nullable": True,
    "start": 535,
    "end": 546
  },
  "cost_of_shares_sold": {
    "type": "string",
    "nullable": True,
    "start": 547,
    "end": 558
  },
  "value_of_shares_distributed": {
    "type": "string",
    "nullable": True,
    "start": 559,
    "end": 570
  },
  "cost_of_shares_distributed_from_contributions": {
    "type": "string",
    "nullable": True,
    "start": 571,
    "end": 582
  },
  "status_prior_to_disbursement": {
    "type": "string",
    "nullable": True,
    "start": 583,
    "end": 584
  },
  "check_address_line_1": {
    "type": "string",
    "nullable": True,
    "start": 585,
    "end": 624
  },
  "check_address_line_2": {
    "type": "string",
    "nullable": True,
    "start": 625,
    "end": 664
  },
  "check_address_line_3": {
    "type": "string",
    "nullable": True,
    "start": 665,
    "end": 704
  },
  "check_address_city": {
    "type": "string",
    "nullable": True,
    "start": 705,
    "end": 732
  },
  "check_address_state": {
    "type": "string",
    "nullable": True,
    "start": 733,
    "end": 735
  },
  "check_address_zip_code": {
    "type": "string",
    "nullable": True,
    "start": 736,
    "end": 744
  },
  "posting_process_counter": {
    "type": "string",
    "nullable": True,
    "start": 745,
    "end": 749
  },
  "pre_1987_after_tax_contributions_distributed": {
    "type": "string",
    "nullable": True,
    "start": 750,
    "end": 761
  },
  "post_1986_after_tax_contributions_distributed": {
    "type": "string",
    "nullable": True,
    "start": 762,
    "end": 773
  },
  "federal_tax_basis_cost": {
    "type": "string",
    "nullable": True,
    "start": 774,
    "end": 785
  },
  "loan_repaid_amount_non_distributable": {
    "type": "string",
    "nullable": True,
    "start": 786,
    "end": 797
  },
  "cost_of_shares_distributed_from_earnings": {
    "type": "string",
    "nullable": True,
    "start": 798,
    "end": 809
  },
  "state_tax_basis_cost": {
    "type": "string",
    "nullable": True,
    "start": 810,
    "end": 821
  },
  "termination_date": {
    "type": "string",
    "nullable": True,
    "start": 822,
    "end": 829
  },
  "capital_gains_distribution_state": {
    "type": "string",
    "nullable": True,
    "start": 830,
    "end": 841
  },
  "ordinary_income_distributed_state": {
    "type": "string",
    "nullable": True,
    "start": 842,
    "end": 853
  },
  "nontaxable_employee_contributions_earnings_distributed_state": {
    "type": "string",
    "nullable": True,
    "start": 854,
    "end": 865
  },
  "unrealized_gain_on_employer_securities_state": {
    "type": "string",
    "nullable": True,
    "start": 866,
    "end": 877
  },
  "tax_year": {
    "type": "string",
    "nullable": True,
    "start": 878,
    "end": 881
  },
  "minimum_distribution_amount": {
    "type": "string",
    "nullable": True,
    "start": 882,
    "end": 893
  },
  "loss_on_return_of_aftertax_contributions_federal": {
    "type": "string",
    "nullable": True,
    "start": 894,
    "end": 905
  },
  "loss_on_return_of_after_tax_contributions_state": {
    "type": "string",
    "nullable": True,
    "start": 906,
    "end": 917
  },
  "alternate_payee_address_line_1": {
    "type": "string",
    "nullable": True,
    "start": 918,
    "end": 957
  },
  "alternate_payee_address_line_2": {
    "type": "string",
    "nullable": True,
    "start": 958,
    "end": 997
  },
  "alternate_payee_address_line_3": {
    "type": "string",
    "nullable": True,
    "start": 998,
    "end": 1037
  },
  "alternate_payee_address_city": {
    "type": "string",
    "nullable": True,
    "start": 1038,
    "end": 1065
  },
  "alternate_payee_address_state": {
    "type": "string",
    "nullable": True,
    "start": 1066,
    "end": 1068
  },
  "alternate_payee_address_zip_code": {
    "type": "string",
    "nullable": True,
    "start": 1069,
    "end": 1077
  },
  "alternate_payee_address_alternate_name": {
    "type": "string",
    "nullable": True,
    "start": 1078,
    "end": 1109
  },
  "alternate_payee_bank_account_number_1": {
    "type": "string",
    "nullable": True,
    "start": 1110,
    "end": 1126
  },
  "alternate_payee_bank_number": {
    "type": "string",
    "nullable": True,
    "start": 1127,
    "end": 1135
  },
  "usage_codes": {
    "type": "string",
    "nullable": True,
    "start": 1136,
    "end": 1145
  },
  "mandatory_withhelding_amount": {
    "type": "string",
    "nullable": True,
    "start": 1146,
    "end": 1157
  },
  "supplied_mandatory_withholding_amount": {
    "type": "string",
    "nullable": True,
    "start": 1158,
    "end": 1169
  },
  "supplied_mandatory_withholding_percent": {
    "type": "string",
    "nullable": True,
    "start": 1170,
    "end": 1176
  },
  "supplied_mandatory_withholding_method": {
    "type": "string",
    "nullable": True,
    "start": 1177,
    "end": 1177
  }
}