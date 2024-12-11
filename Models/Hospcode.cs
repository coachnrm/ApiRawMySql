using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Hospcode
{
    public string? Amppart { get; set; }

    public string? Chwpart { get; set; }

    public string Hospcode1 { get; set; } = null!;

    public string? Hosptype { get; set; }

    public string? Name { get; set; }

    public string? Tmbpart { get; set; }

    public string? Moopart { get; set; }

    public string? SssCode { get; set; }

    public string? SssCodeSub { get; set; }

    public string? Hospcode506 { get; set; }

    public int? HospitalTypeId { get; set; }

    public int? BedCount { get; set; }

    public string? PoCode { get; set; }

    public int? HospitalLevelId { get; set; }

    public string? HospitalPhone { get; set; }

    public string? HospitalFax { get; set; }

    public string? HosGuid { get; set; }

    public string? HosGuidExt { get; set; }

    public string? Addrpart { get; set; }

    public string? AreaCode { get; set; }

    public string? ProvinceName { get; set; }

    public string? Zone { get; set; }
}
