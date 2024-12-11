using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Opitemrece
{
    public string HosGuid { get; set; } = null!;

    public string? Vn { get; set; }

    public string? Hn { get; set; }

    public string? An { get; set; }

    public string? Icode { get; set; }

    public int? Qty { get; set; }

    public string? Drugusage { get; set; }

    public string? Idr { get; set; }

    public int? Iperday { get; set; }

    public double? Iperdose { get; set; }

    public TimeOnly? Recetime { get; set; }

    public double? Unitprice { get; set; }

    public DateOnly? Vstdate { get; set; }

    public TimeOnly? Vsttime { get; set; }

    public string? Doctor { get; set; }

    public DateOnly? Rxdate { get; set; }

    public TimeOnly? Rxtime { get; set; }

    public string? SpUse { get; set; }

    public string? Hcode { get; set; }

    public string? Print { get; set; }

    public string? DepCode { get; set; }

    public string? FinanceNumber { get; set; }

    public double? Discount { get; set; }

    public string? UseRight { get; set; }

    public string? NodeId { get; set; }

    public int? OrderNo { get; set; }

    public string? SubType { get; set; }

    public string? Pttype { get; set; }

    public string? Income { get; set; }

    public string? Remain { get; set; }

    public string? ItemType { get; set; }

    public string? Staff { get; set; }

    public string? DoctorLock { get; set; }

    public string? Paidst { get; set; }

    public sbyte? ItemNo { get; set; }

    public DateTime? LastModified { get; set; }

    public double? SumPrice { get; set; }

    public double? Cost { get; set; }

    public int? StockDepartmentId { get; set; }

    public string? CommandDoctor { get; set; }

    public int? OpiDoctorFinanceTypeId { get; set; }
}
