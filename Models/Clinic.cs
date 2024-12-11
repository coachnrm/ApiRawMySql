using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Clinic
{
    public string Clinic1 { get; set; } = null!;

    public string? Name { get; set; }

    public string? Labparam { get; set; }

    public string? Icd10 { get; set; }

    public string? Chronic { get; set; }

    public string? ClinicGuid { get; set; }

    public int? PcuCode { get; set; }

    public string? OappActivityId { get; set; }

    public string? DisableDialog { get; set; }

    public string? HosGuid { get; set; }

    public string? NoExport { get; set; }

    public string? ActiveStatus { get; set; }

    public int? HosxpClinicTypeId { get; set; }

    public int? OnlineRegisterTypeId { get; set; }

    public TimeOnly? OpenTime { get; set; }

    public TimeOnly? CloseTime { get; set; }

    public int? AppLimitQty { get; set; }

    public string? OnlineNcdSegmentId { get; set; }

    public string? OnlineNcdKey { get; set; }

    public string? Depcode { get; set; }

    public string? Location { get; set; }

    public string? Phone { get; set; }

    public string? SkhOldName { get; set; }

    public string? Oldcode { get; set; }

    public string? LimitAccess { get; set; }

    public string? SssClinicCode { get; set; }

    public string? UseAppQsSlot { get; set; }

    public string? QueuePrefix { get; set; }

    public int? KioskOpdQsRoomId { get; set; }

    public int? OpdKiosDepMenuId { get; set; }

    public int? OpdQsScheduleTmplTypeId { get; set; }

    public int? MophVaccineNcdId { get; set; }

    public string? PhrSkip { get; set; }
}
