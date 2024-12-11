using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class XrayReport
{
    public int Xn { get; set; }

    public string? Hn { get; set; }

    public DateOnly? ReportDate { get; set; }

    public TimeOnly? ReportTime { get; set; }

    public byte[]? ReportRtf { get; set; }

    public string? ReportText { get; set; }

    public string? Doctor { get; set; }

    public string? Vn { get; set; }

    public DateOnly? ExaminedDate { get; set; }

    public TimeOnly? ExaminedTime { get; set; }

    public int? XrayRoomId { get; set; }

    public int? XrayPtStatus { get; set; }

    public int? XrayType { get; set; }

    public string? RequestDoctor { get; set; }

    public DateOnly? RequestDate { get; set; }

    public TimeOnly? RequestTime { get; set; }

    public string? Confirm { get; set; }

    public string? ClinicalInformation { get; set; }

    public string? RequestNote { get; set; }

    public int? XrayItemsCode { get; set; }

    public string? RequestStaff { get; set; }

    public string? An { get; set; }

    public string? Normal { get; set; }

    public int? XraySide { get; set; }

    public string? Staff { get; set; }

    public string? ConfirmReadFilm { get; set; }

    public string? ReportDoctor { get; set; }

    public double? DoctorFee { get; set; }

    public string? XrayIcd10 { get; set; }

    public string? OpitemreceGuid { get; set; }

    public string? XrayNote { get; set; }

    public string? RequestDepcode { get; set; }

    public int? XrayPriorityId { get; set; }

    public int? VisitTimeTypeId { get; set; }

    public int? XrayKeeperId { get; set; }

    public int? RequestCountInYear { get; set; }

    public int? XrayStatusId { get; set; }

    public int? XrayTimeTypeId { get; set; }

    public int? XrayTryCount { get; set; }

    public int? PtXn { get; set; }

    public int? PtClassXn { get; set; }

    public string? ClinicalDiagnosis { get; set; }

    public string? HosGuid { get; set; }

    public string? FinanceLock { get; set; }

    public int? XrayItemsGroupCountInYear { get; set; }

    public int? XrayItemsCountInYear { get; set; }

    public string? XrayIcd102 { get; set; }

    public string? PackStatus { get; set; }

    public string? XrayIcd103 { get; set; }

    public string? XrayIcd104 { get; set; }

    public string? PacRequest { get; set; }

    public DateTime? PacRequestDatetime { get; set; }

    public string? PacRequestMessage { get; set; }

    public int? PacEmergencyCode { get; set; }

    public int? WalkId { get; set; }

    public int? XrayItemsServiceTypeId { get; set; }

    public DateOnly? AcceptDate { get; set; }

    public TimeOnly? AcceptTime { get; set; }

    public DateOnly? ConfirmReadDate { get; set; }

    public TimeOnly? ConfirmReadTime { get; set; }

    public string? ConfirmRadiology { get; set; }

    public DateOnly? ConfirmRadiologyDate { get; set; }

    public TimeOnly? ConfirmRadiologyTime { get; set; }

    public int? XrayReportStatusId { get; set; }

    public double? ServicePrice { get; set; }

    public int? XrayFormOrderId { get; set; }

    public string? WriteData { get; set; }

    public string? NoRadiologist { get; set; }

    public string? FinanceXrayClear { get; set; }

    public string? Pregnant { get; set; }

    public DateTime? OrderDatetime { get; set; }

    public int? XrayItemsGroupTypeId { get; set; }

    public string? FinalReadFilm { get; set; }

    public string? PacsFlag { get; set; }

    public string? AiAvailable { get; set; }

    public string? RadiologistReportResult { get; set; }

    public int? AiReportTypeId { get; set; }

    public string? ConsultFlag { get; set; }

    public string? ConsultText { get; set; }

    public string? RefOrderCode { get; set; }
}
