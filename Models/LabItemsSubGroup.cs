using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class LabItemsSubGroup
{
    public int LabItemsSubGroupCode { get; set; }

    public string? LabItemsSubGroupName { get; set; }

    public int? LabItemsGroupCode { get; set; }

    public double? GroupPrice { get; set; }

    public string? GroupIcode { get; set; }

    public double? GroupPrice2 { get; set; }

    public double? GroupPrice3 { get; set; }

    public double? GroupPriceIpd { get; set; }

    public double? GroupPriceIpd2 { get; set; }

    public double? GroupPriceIpd3 { get; set; }

    public string? Ecode { get; set; }

    public string? HosGuid { get; set; }

    public string? ItemsListText { get; set; }

    public int? SpecimenCode { get; set; }

    public int? EstWaitMinute { get; set; }

    public string? EclaimExport { get; set; }

    public string? TmltCode { get; set; }

    public string? NeedNpo { get; set; }

    public int? NpoDaysAhead { get; set; }

    public string? ActiveStatus { get; set; }

    public string? LoincCode { get; set; }
}
