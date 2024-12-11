using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Oapp
{
    public int OappId { get; set; }

    public string? Hn { get; set; }

    public string? Vn { get; set; }

    public DateOnly? Vstdate { get; set; }

    public DateOnly? Nextdate { get; set; }

    public TimeOnly? Nexttime { get; set; }

    public string? Clinic { get; set; }

    public string? Depcode { get; set; }

    public string? Doctor { get; set; }

    public string? Note { get; set; }

    public string? Spclty { get; set; }

    public string? AppUser { get; set; }

    public string? AppCause { get; set; }

    public string? ContactPoint { get; set; }

    public string? Note1 { get; set; }

    public string? Note2 { get; set; }

    public sbyte? AppNo { get; set; }

    public string? PrintSticker { get; set; }

    public DateOnly? Enddate { get; set; }

    public TimeOnly? Endtime { get; set; }

    public int? LabelColor { get; set; }

    public int? DoctorScheduleId { get; set; }

    public string? Ward { get; set; }

    public string? PatientVisit { get; set; }

    public TimeOnly? NexttimeEnd { get; set; }

    public string? NextPttype { get; set; }

    public string? VisitVn { get; set; }

    public string? OappIdGuid { get; set; }

    public int? PersonVaccineId { get; set; }

    public string? ProvisAptypeCode { get; set; }

    public int? DateCount { get; set; }

    public string? An { get; set; }

    public string? HosGuid { get; set; }

    public DateOnly? EntryDate { get; set; }

    public TimeOnly? EntryTime { get; set; }

    public string? OperationAppointment { get; set; }

    public string? OperationPatientType { get; set; }

    public string? OperationNote { get; set; }

    public string? OperationDoctorCode { get; set; }

    public string? OperationAnesType { get; set; }

    public DateTime? UpdateDatetime { get; set; }

    public int? OappStatusId { get; set; }

    public string? PerformText { get; set; }

    public string? LabListText { get; set; }

    public string? XrayListText { get; set; }

    public int? ClinicVisitTypeId { get; set; }

    public TimeOnly? OpenTime { get; set; }

    public TimeOnly? CloseTime { get; set; }

    public int? AppLimitQty { get; set; }

    public int? VisitNo { get; set; }

    public int? OpdQueueSlotId { get; set; }

    public int? OpdQueueScheduleId { get; set; }

    public int? OpdQsSlotId { get; set; }

    public int? OappRefId { get; set; }

    public string? ReferinVn { get; set; }

    public int? OappWeekRangeLimitId { get; set; }

    public int? ExtRefId { get; set; }

    public int? MophIcRefId { get; set; }

    public string? IsRefill { get; set; }
}
