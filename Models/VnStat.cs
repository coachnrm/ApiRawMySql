using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class VnStat
{
    public string Vn { get; set; } = null!;

    public string? Hn { get; set; }

    public string? Pdx { get; set; }

    public short? Gr504 { get; set; }

    public short? Lastvisit { get; set; }

    public string? AccidentCode { get; set; }

    public string? DxDoctor { get; set; }

    public string? Dx0 { get; set; }

    public string? Dx1 { get; set; }

    public string? Dx2 { get; set; }

    public string? Dx3 { get; set; }

    public string? Dx4 { get; set; }

    public string? Dx5 { get; set; }

    public string? Sex { get; set; }

    public short? AgeY { get; set; }

    public short? AgeM { get; set; }

    public short? AgeD { get; set; }

    public string? Aid { get; set; }

    public string? Moopart { get; set; }

    public short? CountInMonth { get; set; }

    public short? CountInYear { get; set; }

    public string? Pttype { get; set; }

    public double? Income { get; set; }

    public double? PaidMoney { get; set; }

    public double? RemainMoney { get; set; }

    public double? UcMoney { get; set; }

    public double? ItemMoney { get; set; }

    public sbyte? Dba { get; set; }

    public string? Spclty { get; set; }

    public DateOnly? Vstdate { get; set; }

    public string? Op0 { get; set; }

    public string? Op1 { get; set; }

    public string? Op2 { get; set; }

    public string? Op3 { get; set; }

    public string? Op4 { get; set; }

    public string? Op5 { get; set; }

    public string? RcpNo { get; set; }

    public sbyte? PrintCount { get; set; }

    public string? PrintDone { get; set; }

    public string? PttypeInRegion { get; set; }

    public string? PttypeInChwpart { get; set; }

    public string? Pcode { get; set; }

    public string? Hcode { get; set; }

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

    public string? Hospmain { get; set; }

    public string? Hospsub { get; set; }

    public string? Pttypeno { get; set; }

    public DateOnly? PttypeExpire { get; set; }

    public string? Cid { get; set; }

    public string? MainPdx { get; set; }

    public double? Inc17 { get; set; }

    public double? IncDrug { get; set; }

    public double? IncNondrug { get; set; }

    public sbyte? PtSubtype { get; set; }

    public string? RcpnoList { get; set; }

    public string? Ym { get; set; }

    public string? NodeId { get; set; }

    public string? IllVisit { get; set; }

    public sbyte? CountInDay { get; set; }

    public DateOnly? PttypeBegin { get; set; }

    public int? LastvisitHour { get; set; }

    public double? RcptMoney { get; set; }

    public double? DiscountMoney { get; set; }

    public string? OldDiagnosis { get; set; }

    public string? DebtIdList { get; set; }

    public string? VnGuid { get; set; }

    public string? LastvisitVn { get; set; }

    public string? HosGuid { get; set; }

    public string? ArTransferSk { get; set; }

    public DateTime? ArTransferDatetimeSk { get; set; }

    public string? ArTransferStaffSk { get; set; }

    public string? RxLicenseNo { get; set; }

    public string? LabPaidOk { get; set; }

    public string? XrayPaidOk { get; set; }
}
