using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class AnStat
{
    public string An { get; set; } = null!;

    public string? Pdx { get; set; }

    public string? Hn { get; set; }

    public string? Dx0 { get; set; }

    public string? Dx1 { get; set; }

    public string? Dx2 { get; set; }

    public string? Dx3 { get; set; }

    public string? Dx4 { get; set; }

    public string? Dx5 { get; set; }

    public string? Sex { get; set; }

    public sbyte? AgeY { get; set; }

    public sbyte? AgeM { get; set; }

    public sbyte? AgeD { get; set; }

    public string? Aid { get; set; }

    public sbyte? CountInMonth { get; set; }

    public sbyte? CountInYear { get; set; }

    public string? Pttype { get; set; }

    public double? Income { get; set; }

    public int? Lastvisit { get; set; }

    public DateOnly? Regdate { get; set; }

    public DateOnly? Dchdate { get; set; }

    public int? Admdate { get; set; }

    public string? Drg { get; set; }

    public double? Rw { get; set; }

    public double? Los { get; set; }

    public double? Ot { get; set; }

    public string? Spclty { get; set; }

    public string? Ward { get; set; }

    public string? PrintDone { get; set; }

    public sbyte? PrintCount { get; set; }

    public double? PaidMoney { get; set; }

    public double? RemainMoney { get; set; }

    public double? UcMoney { get; set; }

    public double? ItemMoney { get; set; }

    public string? PttypeInRegion { get; set; }

    public string? Pcode { get; set; }

    public string? Op0 { get; set; }

    public string? Op1 { get; set; }

    public string? Op2 { get; set; }

    public string? Op3 { get; set; }

    public string? Op4 { get; set; }

    public string? Op5 { get; set; }

    public string? Op6 { get; set; }

    public double? Inc01 { get; set; }

    public double? Inc02 { get; set; }

    public double? Inc03 { get; set; }

    public double? Inc04 { get; set; }

    public double? Inc05 { get; set; }

    public double? Inc06 { get; set; }

    public double? Inc07 { get; set; }

    public double? Inc08 { get; set; }

    public double? Inc09 { get; set; }

    public double? Inc10 { get; set; }

    public double? Inc11 { get; set; }

    public double? Inc12 { get; set; }

    public double? Inc13 { get; set; }

    public double? Inc14 { get; set; }

    public double? Inc15 { get; set; }

    public double? Inc16 { get; set; }

    public string? DxDoctor { get; set; }

    public string? Vn { get; set; }

    public double? Inc17 { get; set; }

    public string? RcpnoList { get; set; }

    public string? Pttypeno { get; set; }

    public string? Moopart { get; set; }

    public short? Gr504 { get; set; }

    public string? AccidentCode { get; set; }

    public int? LastvisitHour { get; set; }

    public double? RcptMoney { get; set; }

    public double? DiscountMoney { get; set; }

    public string? OldDiagnosis { get; set; }

    public string? DebtIdList { get; set; }

    public int? AdmitHour { get; set; }

    public string? AnGuid { get; set; }

    public int? AdmdateCut24 { get; set; }

    public string? HosGuid { get; set; }

    public string? ArTransferSk { get; set; }

    public DateTime? ArTransferDatetimeSk { get; set; }

    public string? ArTransferStaffSk { get; set; }

    public double? DebtMoney { get; set; }

    public double? OpdWaitMoney { get; set; }

    public string? PrescNedIncomplete { get; set; }

    public DateTime? LastSyncDatetime { get; set; }

    public string? RxLicenseNo { get; set; }

    public string? HasRefillMedplan { get; set; }

    public string? HasMedRecon { get; set; }

    public string? PttypeListText { get; set; }

    public string? DiagTextList { get; set; }

    public int? LastBps { get; set; }

    public int? LastBpd { get; set; }

    public double? LastTemperature { get; set; }

    public int? LastSosScore { get; set; }
}
