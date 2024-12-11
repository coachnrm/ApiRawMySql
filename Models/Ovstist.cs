using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Ovstist
{
    public string Name { get; set; } = null!;

    public string? Ovstist1 { get; set; }

    public string? HosGuid { get; set; }

    public string? ExportCode { get; set; }

    public string? OpbkkCode { get; set; }

    public string? CsmbsCode { get; set; }
}
