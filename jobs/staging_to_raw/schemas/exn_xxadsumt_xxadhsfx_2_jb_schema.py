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
  "source_cycle_date": {
    "type": "string",
    "nullable": True,
    "start": 9,
    "end": 16
  },
  "total_enrollments": {
    "type": "string",
    "nullable": True,
    "start": 17,
    "end": 26
  },
  "total_employee_before_tax_contributions_count": {
    "type": "string",
    "nullable": True,
    "start": 27,
    "end": 36
  },
  "total_employee_after_tax_contributions_count": {
    "type": "string",
    "nullable": True,
    "start": 37,
    "end": 46
  },
  "total_employer_contributions_count": {
    "type": "string",
    "nullable": True,
    "start": 47,
    "end": 56
  },
  "total_rollover_contributions_count": {
    "type": "string",
    "nullable": True,
    "start": 57,
    "end": 66
  },
  "total_termination_distributions_with_rollover_count": {
    "type": "string",
    "nullable": True,
    "start": 67,
    "end": 76
  },
  "total_termination_distributions_with_rollover_rolled_amount": {
    "type": "string",
    "nullable": True,
    "start": 77,
    "end": 92
  },
  "total_termination_distributions_with_rollover_not_rolled_amount": {
    "type": "string",
    "nullable": True,
    "start": 93,
    "end": 108
  },
  "total_termination_distributions_with_no_rollover_count": {
    "type": "string",
    "nullable": True,
    "start": 109,
    "end": 118
  },
  "total_termination_distributions_with_no_rollover_amount": {
    "type": "string",
    "nullable": True,
    "start": 119,
    "end": 134
  },
  "total_in_service_withdrawals_with_rollover_count": {
    "type": "string",
    "nullable": True,
    "start": 135,
    "end": 144
  },
  "total_in_service_withdrawals_with_rollover_rolled_amount": {
    "type": "string",
    "nullable": True,
    "start": 145,
    "end": 160
  },
  "total_in_service_withdrawals_with_rollover_not_rolled_amount": {
    "type": "string",
    "nullable": True,
    "start": 161,
    "end": 176
  },
  "total_in_service_withdrawals_with_no_rollover_count": {
    "type": "string",
    "nullable": True,
    "start": 177,
    "end": 186
  },
  "total_in_service_withdrawals_with_no_rollover_amount": {
    "type": "string",
    "nullable": True,
    "start": 187,
    "end": 202
  },
  "total_hardship_withdrawals_count": {
    "type": "string",
    "nullable": True,
    "start": 203,
    "end": 212
  },
  "total_hardship_withdrawals_amount": {
    "type": "string",
    "nullable": True,
    "start": 213,
    "end": 228
  },
  "total_installments_setup_count": {
    "type": "string",
    "nullable": True,
    "start": 229,
    "end": 238
  },
  "total_installment_distributions_by_check_count": {
    "type": "string",
    "nullable": True,
    "start": 239,
    "end": 248
  },
  "total_installment_distributions_by_check_amount": {
    "type": "string",
    "nullable": True,
    "start": 249,
    "end": 264
  },
  "total_installment_distributions_by_ach_count": {
    "type": "string",
    "nullable": True,
    "start": 265,
    "end": 274
  },
  "total_installment_distributions_by_ach_amount": {
    "type": "string",
    "nullable": True,
    "start": 275,
    "end": 290
  },
  "total_default_rollover_distributions_count": {
    "type": "string",
    "nullable": True,
    "start": 291,
    "end": 300
  },
  "total_default_rollover_distributions_amount": {
    "type": "string",
    "nullable": True,
    "start": 301,
    "end": 316
  },
  "total_deminimus_distributions_count": {
    "type": "string",
    "nullable": True,
    "start": 317,
    "end": 326
  },
  "total_deminimus_distributions_amount": {
    "type": "string",
    "nullable": True,
    "start": 327,
    "end": 342
  },
  "total_loan_issue_count": {
    "type": "string",
    "nullable": True,
    "start": 343,
    "end": 352
  },
  "total_loan_reamortized_count": {
    "type": "string",
    "nullable": True,
    "start": 353,
    "end": 362
  },
  "total_loan_defaults_deemed_distributions_count": {
    "type": "string",
    "nullable": True,
    "start": 363,
    "end": 372
  },
  "total_loan_repayments_count": {
    "type": "string",
    "nullable": True,
    "start": 373,
    "end": 382
  },
  "total_loan_payoff_count": {
    "type": "string",
    "nullable": True,
    "start": 383,
    "end": 392
  },
  "total_dividend_pass_thru_count": {
    "type": "string",
    "nullable": True,
    "start": 393,
    "end": 402
  },
  "total_dividend_pass_thru_amount": {
    "type": "string",
    "nullable": True,
    "start": 403,
    "end": 418
  },
  "total_dividend_reinvestments_count": {
    "type": "string",
    "nullable": True,
    "start": 419,
    "end": 428
  },
  "total_dividend_reinvestments_amount": {
    "type": "string",
    "nullable": True,
    "start": 429,
    "end": 444
  },
  "total_rebalance_transfers_count": {
    "type": "string",
    "nullable": True,
    "start": 445,
    "end": 454
  },
  "total_fee_deductions_count": {
    "type": "string",
    "nullable": True,
    "start": 455,
    "end": 464
  },
  "total_qdro_splits_count": {
    "type": "string",
    "nullable": True,
    "start": 465,
    "end": 474
  },
  "total_beneficiary_splits_count": {
    "type": "string",
    "nullable": True,
    "start": 475,
    "end": 484
  },
  "total_rmd_count": {
    "type": "string",
    "nullable": True,
    "start": 485,
    "end": 494
  },
  "total_rmd_amount": {
    "type": "string",
    "nullable": True,
    "start": 495,
    "end": 510
  },
  "div_sub_id": {
    "type": "string",
    "nullable": True,
    "start": 511,
    "end": 514
  },
  "total_cash_earnings_count": {
    "type": "string",
    "nullable": True,
    "start": 515,
    "end": 524
  },
  "total_cash_earnings_amount": {
    "type": "string",
    "nullable": True,
    "start": 525,
    "end": 540
  },
  "tot_unreal_earn_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 541,
    "end": 550
  },
  "tot_unreal_earn_amt2": {
    "type": "string",
    "nullable": True,
    "start": 551,
    "end": 566
  },
  "tot_div_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 567,
    "end": 576
  },
  "tot_div_amt2": {
    "type": "string",
    "nullable": True,
    "start": 577,
    "end": 592
  },
  "tot_earn_base_fct_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 593,
    "end": 602
  },
  "tot_earn_base_fct_amt2": {
    "type": "string",
    "nullable": True,
    "start": 603,
    "end": 618
  },
  "total_fee_count": {
    "type": "string",
    "nullable": True,
    "start": 619,
    "end": 628
  },
  "total_fee_amount": {
    "type": "string",
    "nullable": True,
    "start": 629,
    "end": 644
  },
  "tot_inter_ppt_to_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 645,
    "end": 654
  },
  "tot_inter_ppt_to_amt2": {
    "type": "string",
    "nullable": True,
    "start": 655,
    "end": 670
  },
  "tot_inter_ppt_frm_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 671,
    "end": 680
  },
  "tot_inter_ppt_frm_amt2": {
    "type": "string",
    "nullable": True,
    "start": 681,
    "end": 696
  },
  "tot_inter_ppt_rel_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 697,
    "end": 706
  },
  "tot_inter_ppt_rel_amt2": {
    "type": "string",
    "nullable": True,
    "start": 707,
    "end": 722
  },
  "tot_loan_reamrt_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 723,
    "end": 732
  },
  "tot_loan_reamrt_amt2": {
    "type": "string",
    "nullable": True,
    "start": 733,
    "end": 748
  },
  "tot_loan_ramrt_rt_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 749,
    "end": 758
  },
  "tot_loan_ramrt_rt_amt2": {
    "type": "string",
    "nullable": True,
    "start": 759,
    "end": 774
  },
  "tot_accord_int_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 775,
    "end": 784
  },
  "tot_accord_int_amt2": {
    "type": "string",
    "nullable": True,
    "start": 785,
    "end": 800
  },
  "tot_rtrn_excss_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 801,
    "end": 810
  },
  "tot_rtrn_excss_amt2": {
    "type": "string",
    "nullable": True,
    "start": 811,
    "end": 826
  },
  "tot_rtrn_excss_wd_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 827,
    "end": 836
  },
  "tot_rtrn_excss_wd_amt2": {
    "type": "string",
    "nullable": True,
    "start": 837,
    "end": 852
  },
  "tot_enrl_dem_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 853,
    "end": 862
  },
  "tot_enrl_elec_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 863,
    "end": 872
  },
  "tot_dem_updt_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 873,
    "end": 882
  },
  "tot_inv_elec_cnt2": {
    "type": "string",
    "nullable": True,
    "start": 883,
    "end": 892
  },
  "total_loan_issue_amount": {
    "type": "string",
    "nullable": True,
    "start": 893,
    "end": 908
  }
}