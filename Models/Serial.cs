using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Serial
{
    public string Name { get; set; } = null!;

    public int? SerialNo { get; set; }

    public string? NodeId { get; set; }

    public string? HosGuid { get; set; }

    public string? HosGuidExt { get; set; }
}
