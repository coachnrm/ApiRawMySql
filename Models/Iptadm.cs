using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Iptadm
{
    public string An { get; set; } = null!;

    public int? Admday { get; set; }

    public string? Bedno { get; set; }

    public string? Bedtype { get; set; }

    public DateOnly? Indate { get; set; }

    public TimeOnly? Intime { get; set; }

    public DateOnly? Outdate { get; set; }

    public TimeOnly? Outtime { get; set; }

    public double? Rate { get; set; }

    public string? Roomno { get; set; }

    public DateTime? MoveInWardDatetime { get; set; }

    public DateTime? MoveInBedDatetime { get; set; }

    public string? TrxOrderChecksum { get; set; }

    public string? HosGuid { get; set; }

    public string? HosGuidExt { get; set; }

    public string? NoRxMachine { get; set; }

    public int? ActiveMedProfileCount { get; set; }

    public int? RxTransactionId { get; set; }
}
