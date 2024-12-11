using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Ward
{
    public string Ward1 { get; set; } = null!;

    public string? Name { get; set; }

    public string? OldCode { get; set; }

    public string? Spclty { get; set; }

    public int? Bedcount { get; set; }

    public string? Shortname { get; set; }

    public string? SssCode { get; set; }

    public string? HosGuid { get; set; }

    public string? NameOldSk { get; set; }

    public string? ShortName { get; set; }

    public string? WardExportCode { get; set; }

    public string? ShortNameBarcode { get; set; }

    public string? WardActive { get; set; }

    public int? IpdRxShiftTypeId { get; set; }

    public string? SelectBednoFromLayout { get; set; }

    public string? IpKey { get; set; }

    public string? StrictAccess { get; set; }
}
