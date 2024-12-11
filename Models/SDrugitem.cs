using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class SDrugitem
{
    public string Icode { get; set; } = null!;

    public string? Name { get; set; }

    public string? Strength { get; set; }

    public string? Units { get; set; }

    public string? Dosageform { get; set; }

    public string? Drugnote { get; set; }

    public string? UseRight { get; set; }

    public string? MustPaid { get; set; }

    public string? Istatus { get; set; }

    public int? AccessLevel { get; set; }

    public string? Paidst { get; set; }

    public int? Displaycolor { get; set; }

    public string? PriceLock { get; set; }

    public string? IcodeGuid { get; set; }

    public string? Ename { get; set; }

    public double? Cost { get; set; }

    public string? Income { get; set; }

    public string? HosGuid { get; set; }

    public string? HosGuidExt { get; set; }

    public string? IsMedication { get; set; }

    public string? UsePaidst { get; set; }

    public string? IsMedsupply { get; set; }

    public string? SubIncome { get; set; }

    public string? Oldcode { get; set; }

    public int? DisplaycolorFocus { get; set; }

    public string? Drugcategory { get; set; }

    public string? Billcode { get; set; }

    public string? Billnumber { get; set; }

    public string? Drugaccount { get; set; }

    public string? Highcost { get; set; }

    public string? NhsoAdpCode { get; set; }

    public int? NhsoAdpTypeId { get; set; }

    public string? SksDfsCode { get; set; }

    public string? SksDrugCode { get; set; }

    public int? SksProductCategoryId { get; set; }

    public string? TpuCodeList { get; set; }

    public double? Unitprice { get; set; }

    public double? IpdPrice { get; set; }

    public string? Icd9cm { get; set; }

    public string? CsmbsClaimCat { get; set; }

    public string? DrgChrgitemId { get; set; }

    public string? ExportOpd { get; set; }

    public string? ExportIpd { get; set; }

    public int? MaxQty { get; set; }

    public int? MaxQtyIpd { get; set; }

    public double? Stdprice { get; set; }

    public string? Did { get; set; }

    public string? Note { get; set; }
}
