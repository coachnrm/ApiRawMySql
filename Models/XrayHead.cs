using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class XrayHead
{
    public string Vn { get; set; } = null!;

    public string? Hn { get; set; }

    public string? XrayList { get; set; }

    public string? ConfirmAll { get; set; }

    public DateTime? OrderDateTime { get; set; }

    public string? Department { get; set; }

    public int? PtXn { get; set; }

    public int? AgeY { get; set; }

    public string? Pttype { get; set; }

    public string? DoctorList { get; set; }

    public int? ReceiveNo { get; set; }

    public string? PrintStatus { get; set; }

    public string? ConfirmReadFilm { get; set; }

    public string? DepartmentCode { get; set; }

    public string? DepartmentName { get; set; }

    public int? XrayPriorityId { get; set; }

    public double? XrayPrice { get; set; }

    public double? TotalPrice { get; set; }

    public int? XrayOrderNumber { get; set; }

    public string? DoctorCode { get; set; }

    public string? XrayHeadRemark { get; set; }

    public DateOnly? OrderDate { get; set; }

    public DateOnly? ReportDate { get; set; }

    public string? ReporterName { get; set; }

    public TimeOnly? ReportTime { get; set; }

    public string? ConfirmReport { get; set; }

    public TimeOnly? OrderTime { get; set; }

    public DateOnly? BeginDate { get; set; }

    public TimeOnly? BeginTime { get; set; }

    public string? Ward { get; set; }

    public string? ResultNote { get; set; }

    public string? PartName { get; set; }

    public string? HosGuid { get; set; }

    public string? DepartmentList { get; set; }

    public int? XrayHeadFlag { get; set; }

    public int? XrayFilmFlag { get; set; }

    public string? ServiceTypeNameList { get; set; }

    public int? XrayConfirmFlag { get; set; }

    public string? ConfirmRadiologyAll { get; set; }

    public string? WaitXrayList { get; set; }

    public int? TotalRequest { get; set; }

    public int? TotalInyearRequest { get; set; }

    public string? IsPregnant { get; set; }

    public string? AiAvailable { get; set; }

    public string? AiAbnormal { get; set; }
}
