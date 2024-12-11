using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Ptcardno
{
    public string Hn { get; set; } = null!;

    public string Cardtype { get; set; } = null!;

    public string? Cardno { get; set; }

    public DateOnly? Expiredate { get; set; }

    public string? HosGuid { get; set; }

    public string? HosGuidExt { get; set; }
}
