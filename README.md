# testfile
SELECT
    A.SeqNo,
    A.TransferDate,
    A.Status,
    Detail.ClntCodeFrom,
    Detail.ClntCodeTo,
    Detail.AcctTypeFrom,
    Detail.AcctTypeTo,
    Detail.MarketFrom,
    Detail.MarketTo,
    Detail.CCYFrom,
    Detail.CCYTo,
    Detail.BankFrom,
    Detail.BankTo,
    Detail.AmountFrom,
    Detail.AmountTo,
    A.FXRate,
    Detail.RemarksFrom,
    Detail.RemarksTo,
    A.UserIDSave,
    A.UserIDCancel,
    A.BuyInXRate,
    A.SellOutXRate,
    Detail.CancelDateFrom,
    Detail.CancelDateTo,
    Detail.ApproverFrom,
    Detail.ApproverTo,
    Detail.ApprovalTimeFrom,
    Detail.ApprovalTimeTo
FROM
    CashTransfer A
    JOIN (
        SELECT
            SeqNo = COALESCE(CT1.SeqNo, CT2.SeqNo),
            MarketFrom = CT1.Market,
            CCYFrom = CT1.CCY,
            VoucherNoFrom = CT1.VoucherNo,
            ClntCodeFrom = CT1.ClntCode,
            AcctTypeFrom = CT1.AcctType,
            BankFrom = CT1.Bank,
            RemarksFrom = CT1.Remarks,
            StatusFrom = CT1.Status,
            AmountFrom = CV1.Amount,
            CancelDateFrom = CV1.CancelDate,
            ApproverFrom = CV1.Approver,
            ApprovalTimeFrom = CV1.ApprovalTime,
            
            MarketTo = CT2.Market,
            CCYTo = CT2.CCY,
            VoucherNoTo = CT2.VoucherNo,
            ClntCodeTo = CT2.ClntCode,
            AcctTypeTo = CT2.AcctType,
            BankTo = CT2.Bank,
            RemarksTo = CT2.Remarks,
            StatusTo = CT2.Status,
            AmountTo = CV2.Amount,
            CancelDateTo = CV2.CancelDate,
            ApproverTo = CV2.Approver,
            ApprovalTimeTo = CV2.ApprovalTime
        FROM
            CashTransferDetail CT1
            LEFT JOIN CashVoucher CV1 
                ON CT1.Market = CV1.Market 
                AND CT1.VoucherNo = CV1.VoucherNo
            LEFT JOIN CashTransferDetail CT2 
                ON CT2.SeqNo = CT1.SeqNo 
                AND CT2.ClntCode = CT1.ClntCode 
                AND CT2.AcctType = CT1.AcctType
                AND CT2.FromTo = '2'  -- 收入方
            LEFT JOIN CashVoucher CV2 
                ON CT2.Market = CV2.Market 
                AND CT2.VoucherNo = CV2.VoucherNo
        WHERE
            CT1.FromTo = '1'  -- 支出方
    ) Detail ON A.SeqNo = Detail.SeqNo
WHERE
    Detail.CCYFrom <> Detail.CCYTo
ORDER BY
    A.SeqNo,
    Detail.ClntCodeFrom;
