

COMMENT ON COLUMN fds_nplus.drvd_intra_hour_quarter_hour_adds.date IS 'keeping last date 7 days data e represents date';


COMMENT ON COLUMN fds_nplus.drvd_intra_hour_quarter_hour_adds.hour IS 'represents hour for the adds';
COMMENT ON COLUMN fds_nplus.drvd_intra_hour_quarter_hour_adds.quarter_hour IS 'Each hour is divided into 4 quarters - 15 mins';
COMMENT ON COLUMN fds_nplus.drvd_intra_hour_quarter_hour_adds.paid_adds IS 'Paid adds';
COMMENT ON COLUMN fds_nplus.drvd_intra_hour_quarter_hour_adds.trial_adds IS 'Trial adds';
COMMENT ON COLUMN fds_nplus.drvd_intra_hour_quarter_hour_adds.payment_method IS 'Payment methods STRIPE,PAYPAL,...';
