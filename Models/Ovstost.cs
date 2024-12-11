using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Ovstost
{
    public string Ovstost1 { get; set; } = null!;

    public string? Name { get; set; }

    public string? DeathStatus { get; set; }

    public string? HosGuid { get; set; }

    public string? ExportCode { get; set; }

    public int? ItemNo { get; set; }

    public string? OpbkkCode { get; set; }

    public string? DefaultState { get; set; }
}
