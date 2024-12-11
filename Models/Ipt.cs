using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Ipt
{
    public string An { get; set; } = null!;

    public string? Admdoctor { get; set; }

    public DateOnly? Dchdate { get; set; }

    public string? Dchstts { get; set; }

    public TimeOnly? Dchtime { get; set; }

    public string? Dchtype { get; set; }

    public string? Dthdiagdct { get; set; }

    public string? Hn { get; set; }

    public string? Ivstist { get; set; }

    public string? Ivstost { get; set; }

    public sbyte? Lockdx { get; set; }

    public string? Prediag { get; set; }

    public string? Pttype { get; set; }

    public DateOnly? Regdate { get; set; }

    public TimeOnly? Regtime { get; set; }

    public string? Rfrics { get; set; }

    public string? Rfrilct { get; set; }

    public string? Rfrocs { get; set; }

    public string? Rfrolct { get; set; }

    public string? Spclty { get; set; }

    public string? Vn { get; set; }

    public string? Ward { get; set; }

    public string? RcptDisease { get; set; }

    public string? DchDoctor { get; set; }

    public sbyte? IptType { get; set; }

    public string? IrefType { get; set; }

    public int? Ipacc { get; set; }

    public double? ActMoneyLimit { get; set; }

    public string? Drg { get; set; }

    public string? Mdc { get; set; }

    public double? Rw { get; set; }

    public double? Wtlos { get; set; }

    public int? Ot { get; set; }

    public string? Result { get; set; }

    public sbyte? Gravidity { get; set; }

    public sbyte? Parity { get; set; }

    public sbyte? LivingChildren { get; set; }

    public string? Rxdoctor { get; set; }

    public string? Staff { get; set; }

    public int? Bw { get; set; }

    public string? FirstWard { get; set; }

    public string? ReferOutNumber { get; set; }

    public string? InchargeDoctor { get; set; }

    public string? AnGuid { get; set; }

    public string? AnLock { get; set; }

    public string? Ergent { get; set; }

    public string? ChartState { get; set; }

    public DateTime? ReceiveChartDateTime { get; set; }

    public string? ReceiveChartStaff { get; set; }

    public string? ReceiveChartNote { get; set; }

    public double? Adjrw { get; set; }

    public string? IptSpclty { get; set; }

    public string? FinanceLock { get; set; }

    public DateTime? LastCheckAutoincome { get; set; }

    public string? AdmitFeeGuid { get; set; }

    public int? LeaveHomeDay { get; set; }

    public string? OperationStatus { get; set; }

    public DateOnly? FinanceSummaryDate { get; set; }

    public DateOnly? EstimateDischargeDate { get; set; }

    public string? OldCauseRevisit { get; set; }

    public string? FinanceTransfer { get; set; }

    public string? ProvisionDx { get; set; }

    public string? TranStatus { get; set; }

    public int? DwHhcListId { get; set; }

    public string? HosGuid { get; set; }

    public string? HosGuidExt { get; set; }

    public int? BodyHeight { get; set; }

    public DateTime? UpdateDatetime { get; set; }

    public string? CurDepCode { get; set; }

    public int? FinanceStatusFlag { get; set; }

    public int? IptAdmitTypeId { get; set; }

    public string? NoVisit { get; set; }

    public string? NoFood { get; set; }

    public string? ConfirmDischarge { get; set; }

    public string? LabStatus { get; set; }

    public string? XrayStatus { get; set; }

    public string? GrouperVersion { get; set; }

    public int? GrouperErr { get; set; }

    public int? GrouperWarn { get; set; }

    public int? GrouperActlos { get; set; }

    public int? Err { get; set; }

    public int? Warn { get; set; }

    public double? AutoChargeAmount { get; set; }

    public string? ProvisionDxIcd { get; set; }

    public int? IptCauseTypeId { get; set; }

    public int? IptSevereTypeId { get; set; }

    public string? IptCauseTypeNote { get; set; }

    public string? Followup { get; set; }

    public int? DchSevereTypeId { get; set; }

    public double? OpdFinanceWaitTr { get; set; }

    public string? HomeLeaveStatus { get; set; }

    public double? GrouperAdjrwPrice { get; set; }

    public double? ReimbursePrice { get; set; }

    public string? Oldcode { get; set; }

    public string? DataOk { get; set; }

    public DateOnly? DataExpDate { get; set; }

    public int? IptSummaryStatusId { get; set; }

    public string? NoChargeRoom { get; set; }

    public string? RxHomeMed { get; set; }

    public string? HhcHospcode { get; set; }

    public string? CurBedno { get; set; }

    public int? OperationStatusId { get; set; }

    public string? IpdNurseEvalRangeCode { get; set; }
}
