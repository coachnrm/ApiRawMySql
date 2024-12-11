using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Drugitem
{
    public string Icode { get; set; } = null!;

    public string? Name { get; set; }

    public string? Strength { get; set; }

    public string? Units { get; set; }

    public double? Unitprice { get; set; }

    public string? Dosageform { get; set; }

    public int? Criticalpriority { get; set; }

    public string? Drugaccount { get; set; }

    public string? Drugcategory { get; set; }

    public string? Drugnote { get; set; }

    public string? Hintcode { get; set; }

    public string? Istatus { get; set; }

    public DateTime? Lastupdatestdprice { get; set; }

    public string? Lockprice { get; set; }

    public string? Lockprint { get; set; }

    public int? Maxlevel { get; set; }

    public int? Minlevel { get; set; }

    public int? Maxunitperdose { get; set; }

    public int? Packqty { get; set; }

    public int? Reorderqty { get; set; }

    public double? Stdprice { get; set; }

    public string? Stdtaken { get; set; }

    public string? Therapeutic { get; set; }

    public string? Therapeuticgroup { get; set; }

    public int? DefaultQty { get; set; }

    public string? GpoCode { get; set; }

    public string? UseRight { get; set; }

    public string? IType { get; set; }

    public string? Drugusage { get; set; }

    public string? HighCost { get; set; }

    public string? MustPaid { get; set; }

    public sbyte? AlertLevel { get; set; }

    public sbyte? AccessLevel { get; set; }

    public string? StickerShortName { get; set; }

    public string? Paidst { get; set; }

    public string? Antibiotic { get; set; }

    public int? Displaycolor { get; set; }

    public string? Empty { get; set; }

    public string? EmptyText { get; set; }

    public double? Unitcost { get; set; }

    public string? Gfmiscode { get; set; }

    public double? IpdPrice { get; set; }

    public string? Oldcode { get; set; }

    public string? HabitForming { get; set; }

    public string? Did { get; set; }

    public string? StockType { get; set; }

    public double? Price2 { get; set; }

    public double? Price3 { get; set; }

    public double? IpdPrice2 { get; set; }

    public double? IpdPrice3 { get; set; }

    public string? PriceLock { get; set; }

    public string? Pregnancy { get; set; }

    public int? PharmacologyGroup1 { get; set; }

    public int? PharmacologyGroup2 { get; set; }

    public int? PharmacologyGroup3 { get; set; }

    public string? GenericName { get; set; }

    public string? ShowPregnancyAlert { get; set; }

    public string? IcodeGuid { get; set; }

    public string? Na { get; set; }

    public string? Invcode { get; set; }

    public string? CheckUserGroup { get; set; }

    public string? CheckUserName { get; set; }

    public string? ShowNotify { get; set; }

    public string? ShowNotifyText { get; set; }

    public string? Income { get; set; }

    public string? PrintStickerPq { get; set; }

    public string? ChargeServiceOpd { get; set; }

    public string? ChargeServiceIpd { get; set; }

    public string? Ename { get; set; }

    public string? DoseType { get; set; }

    public int? HabitFormingType { get; set; }

    public string? NoDiscount { get; set; }

    public string? TherapeuticEng { get; set; }

    public string? HintcodeEng { get; set; }

    public string? LimitDrugusage { get; set; }

    public string? PrintStickerHeader { get; set; }

    public string? CalcIdrQty { get; set; }

    public string? ItemInHospital { get; set; }

    public string? NoSubstock { get; set; }

    public int? VolumeCc { get; set; }

    public string? UsageCode { get; set; }

    public string? FrequencyCode { get; set; }

    public string? TimeCode { get; set; }

    public double? DispenseDose { get; set; }

    public string? UsageUnitCode { get; set; }

    public double? DosePerUnits { get; set; }

    public int? IpdDefaultPay { get; set; }

    public string? Billcode { get; set; }

    public string? Billnumber { get; set; }

    public string? LockprintIpd { get; set; }

    public string? PregnancyNotifyText { get; set; }

    public string? ShowBreastFeedingAlert { get; set; }

    public string? BreastFeedingAlertText { get; set; }

    public string? ShowChildNotify { get; set; }

    public string? ChildNotifyText { get; set; }

    public int? ChildNotifyMinAge { get; set; }

    public int? ChildNotifyMaxAge { get; set; }

    public string? Continuous { get; set; }

    public string? SubstituteIcode { get; set; }

    public string? TradeName { get; set; }

    public string? UseRightAllow { get; set; }

    public int? MedicationMachineId { get; set; }

    public int? IpdMedicationMachineId { get; set; }

    public string? CheckRemedQty { get; set; }

    public string? Addict { get; set; }

    public int? AddictTypeId { get; set; }

    public int? MedicationMachineOpdNo { get; set; }

    public int? MedicationMachineIpdNo { get; set; }

    public string? FpDrug { get; set; }

    public string? UsageCodeIpd { get; set; }

    public double? DispenseDoseIpd { get; set; }

    public string? UsageUnitCodeIpd { get; set; }

    public string? FrequencyCodeIpd { get; set; }

    public string? TimeCodeIpd { get; set; }

    public string? PrintIpdInjectionSticker { get; set; }

    public string? ProvisMedicationUnitCode { get; set; }

    public string? HosGuid { get; set; }

    public int? SksProductCategoryId { get; set; }

    public int? SksClainControlTypeId { get; set; }

    public string? SksDrugCode { get; set; }

    public string? SksDfsCode { get; set; }

    public string? SksDfsText { get; set; }

    public double? SksReimbPrice { get; set; }

    public string? HosGuidExt { get; set; }

    public string? CheckDruginteractionHistory { get; set; }

    public int? CheckDruginteractionHistoryDay { get; set; }

    public int? NhsoAdpTypeId { get; set; }

    public string? NhsoAdpCode { get; set; }

    public DateTime? SkInsertDataDate { get; set; }

    public string? SkSpecprep { get; set; }

    public string? SkUnits { get; set; }

    public int? SksClaimControlTypeId { get; set; }

    public DateOnly? BeginDate { get; set; }

    public DateOnly? FinishDate { get; set; }

    public string? NamePr { get; set; }

    public string? NameEng { get; set; }

    public string? CapacityName { get; set; }

    public string? FinishReason { get; set; }

    public double? ExtraUnitcost { get; set; }

    public int? DrugControlTypeId { get; set; }

    public string? NamePrint { get; set; }

    public double? ActiveIngredientMg { get; set; }

    public string? NoOrderG6pd { get; set; }

    public string? GenderCheck { get; set; }

    public string? NoOrderGender { get; set; }

    public int? MaxQty { get; set; }

    public string? PreferOpdUsageCode { get; set; }

    public double? CapacityQty { get; set; }

    public string? NeedOrderReason { get; set; }

    public int? DrugitemsDueTypeId { get; set; }

    public int? DrugevalHeadId { get; set; }

    public string? LightProtect { get; set; }

    public string? TpuCodeList { get; set; }

    public string? InvMapUpdate { get; set; }

    public string? SpecialAdviceText { get; set; }

    public string? PrecautionAdviceText { get; set; }

    public string? ContraAdviceText { get; set; }

    public string? StorageAdviceText { get; set; }

    public string? QrCodeUrl { get; set; }

    public double? VatPercent { get; set; }

    public string? AccRegist { get; set; }

    public string? UsePaidst { get; set; }

    public string? ThaiName { get; set; }

    public int? FwfItemId { get; set; }

    public int? DrugitemsEm1Id { get; set; }

    public int? DrugitemsEm2Id { get; set; }

    public int? DrugitemsEm3Id { get; set; }

    public int? DrugitemsEm4Id { get; set; }

    public string? TmtTpCode { get; set; }

    public string? TmtGpCode { get; set; }

    public string? LimitPttype { get; set; }

    public string? NoshowNarcotic { get; set; }

    public string? MedicationMachineFlag { get; set; }

    public double? SksPrice { get; set; }

    public string? PrintStickerByFrequency { get; set; }

    public string? PrintStickerPqIpd { get; set; }

    public string? SubIncome { get; set; }

    public string? PreferIpdUsageCode { get; set; }

    public int? DefaultQtyIpd { get; set; }

    public int? MaxQtyIpd { get; set; }

    public string? DrugusageIpd { get; set; }

    public string? NoPopupIpdReason { get; set; }

    public string? Specprep { get; set; }

    public int? MedDoseCalcTypeId { get; set; }

    public string? SendLineNotify { get; set; }

    public string? ShowQrcodeTrade { get; set; }

    public string? WarnG6pd { get; set; }

    public int? IpdRxFreqDay { get; set; }

    public int? DisplaycolorFocus { get; set; }

    public DateTime? LastUpdate { get; set; }

    public string? NoRemed { get; set; }

    public string? ForceRoundQty { get; set; }

    public string? AtcCode { get; set; }

    public int? StateItemId { get; set; }

    public string? MultiplyChargeService { get; set; }

    public string? CsmbsClaimCat { get; set; }

    public string? Simb2005 { get; set; }

    public DateOnly? SksRevDate { get; set; }

    public string? OpiUsageCode { get; set; }

    public string? SctUnitCode { get; set; }

    public string? PrintLabel { get; set; }

    public string? NeedPrescDuration { get; set; }

    public string? Note { get; set; }
}
