using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Spclty
{
    public string Spclty1 { get; set; } = null!;

    public string? Name { get; set; }

    public string? Depcode { get; set; }

    public string? Ovstostlink { get; set; }

    public string? Spname { get; set; }

    public string? Shortname { get; set; }

    public string? IllVisit { get; set; }

    public string? SpcltyGuid { get; set; }

    public string? NoServiceCharge { get; set; }

    public string? NoServiceChargeDoctor { get; set; }

    public string? Oldcode { get; set; }

    public string? NhsoCode { get; set; }

    public string? Operation { get; set; }

    public string? ProvisCode { get; set; }

    public string? HosGuid { get; set; }

    public string? NoExport43 { get; set; }

    public int? DisplayOrder { get; set; }

    public string? AllowOnlineAppointment { get; set; }

    public string? ActiveStatus { get; set; }
}
