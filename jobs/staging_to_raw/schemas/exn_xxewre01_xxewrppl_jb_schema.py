schema = {
  "client_id": {
    "type": "string",
    "nullable": True,
    "start": 1,
    "end": 2
  },
  "plan_number": {
    "type": "string",
    "nullable": True,
    "start": 3,
    "end": 8
  },
  "participant_id": {
    "type": "string",
    "nullable": True,
    "start": 9,
    "end": 17
  },
  "loan_number": {
    "type": "string",
    "nullable": True,
    "start": 18,
    "end": 20
  },
  "accumulated_inactive_interest": {
    "type": "string",
    "nullable": True,
    "start": 21,
    "end": 32
  },
  "ach_debit_flag": {
    "type": "string",
    "nullable": True,
    "start": 33,
    "end": 33
  },
  "ach_debit_flag_desc": {
    "type": "string",
    "nullable": True,
    "start": 34,
    "end": 34
  },
  "advance_payment_method": {
    "type": "string",
    "nullable": True,
    "start": 35,
    "end": 35
  },
  "annual_percentage_rate": {
    "type": "string",
    "nullable": True,
    "start": 36,
    "end": 43
  },
  "arrears_method": {
    "type": "string",
    "nullable": True,
    "start": 44,
    "end": 44
  },
  "compliance_default_notification_flag": {
    "type": "string",
    "nullable": True,
    "start": 45,
    "end": 45
  },
  "compliance_notification_flag": {
    "type": "string",
    "nullable": True,
    "start": 46,
    "end": 46
  },
  "current_loan_balance": {
    "type": "string",
    "nullable": True,
    "start": 47,
    "end": 58
  },
  "date_of_loan_issuance": {
    "type": "string",
    "nullable": True,
    "start": 59,
    "end": 66
  },
  "date_of_next_loan_payment": {
    "type": "string",
    "nullable": True,
    "start": 67,
    "end": 74
  },
  "date_of_previous_loan_payment": {
    "type": "string",
    "nullable": True,
    "start": 75,
    "end": 82
  },
  "deemed_interest_rate": {
    "type": "string",
    "nullable": True,
    "start": 83,
    "end": 90
  },
  "deemed_offset_date": {
    "type": "string",
    "nullable": True,
    "start": 91,
    "end": 98
  },
  "default_notification_flag": {
    "type": "string",
    "nullable": True,
    "start": 99,
    "end": 99
  },
  "default_terms": {
    "type": "string",
    "nullable": True,
    "start": 100,
    "end": 110
  },
  "delay_default_date": {
    "type": "string",
    "nullable": True,
    "start": 111,
    "end": 118
  },
  "discharge_default_frequency_interval": {
    "type": "string",
    "nullable": True,
    "start": 119,
    "end": 129
  },
  "discharge_default_method": {
    "type": "string",
    "nullable": True,
    "start": 130,
    "end": 130
  },
  "discharge_default_static_interest": {
    "type": "string",
    "nullable": True,
    "start": 131,
    "end": 142
  },
  "earnings_interest_paid": {
    "type": "string",
    "nullable": True,
    "start": 143,
    "end": 154
  },
  "earnings_rate": {
    "type": "string",
    "nullable": True,
    "start": 155,
    "end": 162
  },
  "first_delinquency_notice_date": {
    "type": "string",
    "nullable": True,
    "start": 163,
    "end": 170
  },
  "from_loan_number": {
    "type": "string",
    "nullable": True,
    "start": 171,
    "end": 173
  },
  "past_year_highest_loan_balance": {
    "type": "string",
    "nullable": True,
    "start": 174,
    "end": 183
  },
  "last_payment_calculation": {
    "type": "string",
    "nullable": True,
    "start": 184,
    "end": 184
  },
  "last_repayment_amount": {
    "type": "string",
    "nullable": True,
    "start": 185,
    "end": 196
  },
  "last_repayment_date": {
    "type": "string",
    "nullable": True,
    "start": 197,
    "end": 204
  },
  "loan_amortization_type_flag": {
    "type": "string",
    "nullable": True,
    "start": 205,
    "end": 205
  },
  "loan_discharge_schedule_at_termination": {
    "type": "string",
    "nullable": True,
    "start": 206,
    "end": 206
  },
  "loan_exclusion_flag": {
    "type": "string",
    "nullable": True,
    "start": 207,
    "end": 207
  },
  "loan_fee_override_amt": {
    "type": "string",
    "nullable": True,
    "start": 208,
    "end": 219
  },
  "loan_inactive_date": {
    "type": "string",
    "nullable": True,
    "start": 220,
    "end": 227
  },
  "loan_interest_applied_to_principal": {
    "type": "string",
    "nullable": True,
    "start": 228,
    "end": 239
  },
  "loan_interest_in_arrears": {
    "type": "string",
    "nullable": True,
    "start": 240,
    "end": 251
  },
  "loan_interest_rate": {
    "type": "string",
    "nullable": True,
    "start": 252,
    "end": 259
  },
  "loan_original_number_of_payments": {
    "type": "string",
    "nullable": True,
    "start": 260,
    "end": 263
  },
  "loan_originator": {
    "type": "string",
    "nullable": True,
    "start": 264,
    "end": 264
  },
  "loan_payment_amount": {
    "type": "string",
    "nullable": True,
    "start": 265,
    "end": 276
  },
  "loan_payment_frequency": {
    "type": "string",
    "nullable": True,
    "start": 277,
    "end": 277
  },
  "loan_payoff_date": {
    "type": "string",
    "nullable": True,
    "start": 278,
    "end": 285
  },
  "loan_payroll_code": {
    "type": "string",
    "nullable": True,
    "start": 286,
    "end": 293
  },
  "loan_principal_in_arrears": {
    "type": "string",
    "nullable": True,
    "start": 294,
    "end": 306
  },
  "loan_reamortization_date": {
    "type": "string",
    "nullable": True,
    "start": 307,
    "end": 314
  },
  "loan_refinance_date": {
    "type": "string",
    "nullable": True,
    "start": 315,
    "end": 322
  },
  "loan_repayment_option": {
    "type": "string",
    "nullable": True,
    "start": 323,
    "end": 323
  },
  "loan_status_code": {
    "type": "string",
    "nullable": True,
    "start": 324,
    "end": 324
  },
  "loan_status_flag": {
    "type": "string",
    "nullable": True,
    "start": 325,
    "end": 325
  },
  "loan_total_expected_interest": {
    "type": "string",
    "nullable": True,
    "start": 326,
    "end": 337
  },
  "loan_total_interest_paid": {
    "type": "string",
    "nullable": True,
    "start": 338,
    "end": 350
  },
  "loan_type_flag": {
    "type": "string",
    "nullable": True,
    "start": 351,
    "end": 351
  },
  "loan_type_id": {
    "type": "string",
    "nullable": True,
    "start": 352,
    "end": 352
  },
  "loan_use_indicator": {
    "type": "string",
    "nullable": True,
    "start": 353,
    "end": 353
  },
  "next_periodic_interest_date": {
    "type": "string",
    "nullable": True,
    "start": 354,
    "end": 361
  },
  "number_of_payments_in_arrears": {
    "type": "string",
    "nullable": True,
    "start": 362,
    "end": 370
  },
  "original_issue_date": {
    "type": "string",
    "nullable": True,
    "start": 371,
    "end": 378
  },
  "original_loan_amount": {
    "type": "string",
    "nullable": True,
    "start": 379,
    "end": 390
  },
  "original_takeover_amount": {
    "type": "string",
    "nullable": True,
    "start": 391,
    "end": 402
  },
  "original_total_interest_paid": {
    "type": "string",
    "nullable": True,
    "start": 403,
    "end": 414
  },
  "payment_default_loan": {
    "type": "string",
    "nullable": True,
    "start": 415,
    "end": 415
  },
  "prepaid_fees": {
    "type": "string",
    "nullable": True,
    "start": 416,
    "end": 425
  },
  "prior_frequency": {
    "type": "string",
    "nullable": True,
    "start": 426,
    "end": 426
  },
  "prior_loan_number": {
    "type": "string",
    "nullable": True,
    "start": 427,
    "end": 438
  },
  "process_fees": {
    "type": "string",
    "nullable": True,
    "start": 439,
    "end": 448
  },
  "residential_term": {
    "type": "string",
    "nullable": True,
    "start": 449,
    "end": 450
  },
  "second_delinquency_notice_date": {
    "type": "string",
    "nullable": True,
    "start": 451,
    "end": 458
  },
  "signed_date": {
    "type": "string",
    "nullable": True,
    "start": 459,
    "end": 466
  },
  "takeover_date": {
    "type": "string",
    "nullable": True,
    "start": 467,
    "end": 474
  },
  "third_delinquent_notice_date": {
    "type": "string",
    "nullable": True,
    "start": 475,
    "end": 482
  },
  "to_loan_number": {
    "type": "string",
    "nullable": True,
    "start": 483,
    "end": 485
  },
  "user_defined_field_amount_04": {
    "type": "string",
    "nullable": True,
    "start": 486,
    "end": 497
  },
  "user_defined_field_code_05": {
    "type": "string",
    "nullable": True,
    "start": 498,
    "end": 498
  },
  "user_defined_field_flag_04": {
    "type": "string",
    "nullable": True,
    "start": 499,
    "end": 499
  },
  "user_defined_field_text_02": {
    "type": "string",
    "nullable": True,
    "start": 500,
    "end": 509
  },
  "user_defined_field_text_03": {
    "type": "string",
    "nullable": True,
    "start": 510,
    "end": 519
  },
  "user_defined_field_text_04": {
    "type": "string",
    "nullable": True,
    "start": 520,
    "end": 529
  },
  "source_system": {
    "type": "string",
    "nullable": True,
    "start": 530,
    "end": 539
  },
  "source_cycle_date": {
    "type": "string",
    "nullable": True,
    "start": 540,
    "end": 547
  }
}