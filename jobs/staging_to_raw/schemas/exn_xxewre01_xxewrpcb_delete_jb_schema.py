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
  "fund_iv": {
    "type": "string",
    "nullable": True,
    "start": 18,
    "end": 19
  },
  "money_source": {
    "type": "string",
    "nullable": True,
    "start": 20,
    "end": 20
  },
  "unit_share_bal": {
    "type": "string",
    "nullable": True,
    "start": 21,
    "end": 34
  },
  "cash_value": {
    "type": "string",
    "nullable": True,
    "start": 35,
    "end": 51
  },
  "trade_date": {
    "type": "string",
    "nullable": True,
    "start": 52,
    "end": 59
  },
  "accrued_dividend_amount": {
    "type": "string",
    "nullable": True,
    "start": 60,
    "end": 76
  },
  "annual_dividend_amount": {
    "type": "string",
    "nullable": True,
    "start": 77,
    "end": 93
  },
  "brokerage_account_cash_value": {
    "type": "string",
    "nullable": True,
    "start": 94,
    "end": 110
  },
  "brokerage_account_trade_date": {
    "type": "string",
    "nullable": True,
    "start": 111,
    "end": 118
  },
  "contribution_allocation_percent": {
    "type": "string",
    "nullable": True,
    "start": 119,
    "end": 132
  },
  "contribution_current_cal_year": {
    "type": "string",
    "nullable": True,
    "start": 133,
    "end": 149
  },
  "contribution_current_fis_year": {
    "type": "string",
    "nullable": True,
    "start": 150,
    "end": 166
  },
  "contribution_gross": {
    "type": "string",
    "nullable": True,
    "start": 167,
    "end": 183
  },
  "conversions_in": {
    "type": "string",
    "nullable": True,
    "start": 184,
    "end": 200
  },
  "conversions_out": {
    "type": "string",
    "nullable": True,
    "start": 201,
    "end": 217
  },
  "earnings_cash": {
    "type": "string",
    "nullable": True,
    "start": 218,
    "end": 234
  },
  "earnings_dividend": {
    "type": "string",
    "nullable": True,
    "start": 235,
    "end": 251
  },
  "earnings_gain_loss_shares_unit": {
    "type": "string",
    "nullable": True,
    "start": 252,
    "end": 265
  },
  "fee_disbursements": {
    "type": "string",
    "nullable": True,
    "start": 266,
    "end": 282
  },
  "forfeitures_credited": {
    "type": "string",
    "nullable": True,
    "start": 283,
    "end": 299
  },
  "forfeitures_debited": {
    "type": "string",
    "nullable": True,
    "start": 300,
    "end": 316
  },
  "installment_disbursements": {
    "type": "string",
    "nullable": True,
    "start": 317,
    "end": 333
  },
  "insurance_premium_paid": {
    "type": "string",
    "nullable": True,
    "start": 334,
    "end": 350
  },
  "loan_issues": {
    "type": "string",
    "nullable": True,
    "start": 351,
    "end": 367
  },
  "loan_repayment_interest": {
    "type": "string",
    "nullable": True,
    "start": 368,
    "end": 384
  },
  "loan_repayment_principal": {
    "type": "string",
    "nullable": True,
    "start": 385,
    "end": 401
  },
  "miscellanous_debits": {
    "type": "string",
    "nullable": True,
    "start": 402,
    "end": 418
  },
  "miscellanous_receipts": {
    "type": "string",
    "nullable": True,
    "start": 419,
    "end": 435
  },
  "net_contribution_current": {
    "type": "string",
    "nullable": True,
    "start": 436,
    "end": 452
  },
  "net_dollars": {
    "type": "string",
    "nullable": True,
    "start": 453,
    "end": 465
  },
  "number_of_units": {
    "type": "string",
    "nullable": True,
    "start": 466,
    "end": 484
  },
  "other_debits": {
    "type": "string",
    "nullable": True,
    "start": 485,
    "end": 501
  },
  "payment_attributable_contributions": {
    "type": "string",
    "nullable": True,
    "start": 502,
    "end": 518
  },
  "pending_credit_cash": {
    "type": "string",
    "nullable": True,
    "start": 519,
    "end": 535
  },
  "pending_credit_shares": {
    "type": "string",
    "nullable": True,
    "start": 536,
    "end": 552
  },
  "pending_debit_cash": {
    "type": "string",
    "nullable": True,
    "start": 553,
    "end": 569
  },
  "pending_debit_shares": {
    "type": "string",
    "nullable": True,
    "start": 570,
    "end": 586
  },
  "shares_units_distributed": {
    "type": "string",
    "nullable": True,
    "start": 587,
    "end": 603
  },
  "shares_units_forfeited": {
    "type": "string",
    "nullable": True,
    "start": 604,
    "end": 620
  },
  "shares_units_purchased": {
    "type": "string",
    "nullable": True,
    "start": 621,
    "end": 635
  },
  "shares_units_receipted": {
    "type": "string",
    "nullable": True,
    "start": 636,
    "end": 650
  },
  "shares_units_sold": {
    "type": "string",
    "nullable": True,
    "start": 651,
    "end": 665
  },
  "termination_disbursements": {
    "type": "string",
    "nullable": True,
    "start": 666,
    "end": 682
  },
  "transfers_in": {
    "type": "string",
    "nullable": True,
    "start": 683,
    "end": 699
  },
  "transfers_out": {
    "type": "string",
    "nullable": True,
    "start": 700,
    "end": 716
  },
  "uninvested_balance": {
    "type": "string",
    "nullable": True,
    "start": 717,
    "end": 733
  },
  "unit_price": {
    "type": "string",
    "nullable": True,
    "start": 734,
    "end": 750
  },
  "vested_balance": {
    "type": "string",
    "nullable": True,
    "start": 751,
    "end": 767
  },
  "vested_percent": {
    "type": "string",
    "nullable": True,
    "start": 768,
    "end": 784
  },
  "withdrawal_disbursements": {
    "type": "string",
    "nullable": True,
    "start": 785,
    "end": 801
  },
  "source_system": {
    "type": "string",
    "nullable": True,
    "start": 802,
    "end": 811
  },
  "source_cycle_date": {
    "type": "string",
    "nullable": True,
    "start": 812,
    "end": 819
  }
}