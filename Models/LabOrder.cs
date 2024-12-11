using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class LabOrder
{
    public int LabOrderNumber { get; set; }

    public int LabItemsCode { get; set; }

    public string? LabOrderResult { get; set; }

    public string? LabOrderRemark { get; set; }

    public string? Staff { get; set; }

    public string? Confirm { get; set; }

    public string? LabItemsNameRef { get; set; }

    public string? LabItemsNormalValueRef { get; set; }

    public int? SpecimenCode { get; set; }

    public int? LabItemsSubGroupCode { get; set; }

    public string? OrderType { get; set; }

    public double? ItemCost { get; set; }

    public string? HosGuid { get; set; }

    public string? StaffLockResult { get; set; }

    public DateOnly? LaborderDate { get; set; }

    public string? AbnormalResult { get; set; }

    public string? HosGuidExt { get; set; }

    public string? CheckKey { get; set; }

    public int? LabResultStatus { get; set; }

    public DateTime? UpdateDatetime { get; set; }

    public int? LabHistCompareTypeId { get; set; }

    public string? Printed { get; set; }

    public DateTime? PrintedDatetime { get; set; }

    public string? CriticalResult { get; set; }

    public string? CheckKeyA { get; set; }

    public string? SendNotify { get; set; }
}
