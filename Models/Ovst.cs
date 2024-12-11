using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Ovst
{
    public string HosGuid { get; set; } = null!;

    public string? Vn { get; set; }

    public string? Hn { get; set; }

    public string? An { get; set; }

    public DateOnly? Vstdate { get; set; }

    public TimeOnly? Vsttime { get; set; }

    public string? Doctor { get; set; }

    public string? Hospmain { get; set; }

    public string? Hospsub { get; set; }

    public int? Oqueue { get; set; }

    public string? Ovstist { get; set; }

    public string? Ovstost { get; set; }

    public string? Pttype { get; set; }

    public string? Pttypeno { get; set; }

    public string? Rfrics { get; set; }

    public string? Rfrilct { get; set; }

    public string? Rfrocs { get; set; }

    public string? Rfrolct { get; set; }

    public string? Spclty { get; set; }

    public string? RcptDisease { get; set; }

    public string? Hcode { get; set; }

    public string? CurDep { get; set; }

    public string? CurDepBusy { get; set; }

    public string? LastDep { get; set; }

    public TimeOnly? CurDepTime { get; set; }

    public int? RxQueue { get; set; }

    public string? DiagText { get; set; }

    public sbyte? PtSubtype { get; set; }

    public string? MainDep { get; set; }

    public int? MainDepQueue { get; set; }

    public DateOnly? FinanceSummaryDate { get; set; }

    public string? VisitType { get; set; }

    public string? NodeId { get; set; }

    public int? ContractId { get; set; }

    public string? Waiting { get; set; }

    public string? RfriIcd10 { get; set; }

    public int? OReferNumber { get; set; }

    public string? HasInsurance { get; set; }

    public string? IReferNumber { get; set; }

    public string? ReferType { get; set; }

    public string? OReferDep { get; set; }

    public string? Staff { get; set; }

    public string? CommandDoctor { get; set; }

    public string? SendPerson { get; set; }

    public int? PtPriority { get; set; }

    public string? FinanceLock { get; set; }

    public string? FinanceAlient { get; set; }

    public string? Oldcode { get; set; }

    public string? SignDoctor { get; set; }

    public string? AnonymousVisit { get; set; }

    public string? AnonymousVn { get; set; }

    public int? PtCapabilityTypeId { get; set; }

    public string? AtHospital { get; set; }

    public string? OvstKey { get; set; }
}
