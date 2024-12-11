using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Opdscreen
{
    public string HosGuid { get; set; } = null!;

    public string? Vn { get; set; }

    public string? Hn { get; set; }

    public DateOnly? Vstdate { get; set; }

    public TimeOnly? Vsttime { get; set; }

    public TimeOnly? Begintime { get; set; }

    public TimeOnly? Outtime { get; set; }

    public TimeOnly? Endtime { get; set; }

    public double? Bpd { get; set; }

    public double? Bps { get; set; }

    public double? Bw { get; set; }

    public string? Cc { get; set; }

    public double? Hr { get; set; }

    public string? Pe { get; set; }

    public double? Pulse { get; set; }

    public double? Temperature { get; set; }

    public string? Note { get; set; }

    public double? Rr { get; set; }

    public DateOnly? CcBeginDate { get; set; }

    public string? CcCauseOfVisit { get; set; }

    public string? CcSign { get; set; }

    public string? CcDuration { get; set; }

    public string? CcPosition { get; set; }

    public string? CcNote { get; set; }

    public DateOnly? HisBeginDate { get; set; }

    public string? HisFrequency { get; set; }

    public string? HisSeverity { get; set; }

    public string? HisCause { get; set; }

    public string? HisExpand { get; set; }

    public string? HisCauseIncrease { get; set; }

    public string? HisCauseDecrease { get; set; }

    public string? HisRelatedSign { get; set; }

    public int? Height { get; set; }

    public string? ScreenDep { get; set; }

    public string? Waiting { get; set; }

    public double? Fbs { get; set; }

    public string? Help1 { get; set; }

    public string? Help2 { get; set; }

    public string? Help3 { get; set; }

    public string? Help4 { get; set; }

    public TimeOnly? Help1Time { get; set; }

    public int? Help1Bps { get; set; }

    public int? Help1Bpd { get; set; }

    public TimeOnly? Help2Time { get; set; }

    public double? Help2Temp { get; set; }

    public string? Help3Icode { get; set; }

    public TimeOnly? Help3Time { get; set; }

    public sbyte? Help3Qty { get; set; }

    public string? Help4Note { get; set; }

    public string? Advice1 { get; set; }

    public string? Advice2 { get; set; }

    public string? Advice3 { get; set; }

    public string? Advice4 { get; set; }

    public string? Advice5 { get; set; }

    public string? Advice6 { get; set; }

    public string? Advice7 { get; set; }

    public string? Cradle { get; set; }

    public string? PeGa { get; set; }

    public string? PeHeent { get; set; }

    public string? PeHeart { get; set; }

    public string? PeLung { get; set; }

    public string? PeAb { get; set; }

    public string? PeExt { get; set; }

    public string? PeNeuro { get; set; }

    public string? PeGaText { get; set; }

    public string? PeHeentText { get; set; }

    public string? PeHeartText { get; set; }

    public string? PeLungText { get; set; }

    public string? PeAbText { get; set; }

    public string? PeNeuroText { get; set; }

    public string? PeExtText { get; set; }

    public double? Bmi { get; set; }

    public double? Tg { get; set; }

    public double? Hdl { get; set; }

    public string? Glucurine { get; set; }

    public string? Blank1 { get; set; }

    public double? Bun { get; set; }

    public double? Creatinine { get; set; }

    public double? Ua { get; set; }

    public double? Hba1c { get; set; }

    public string? Riskdm { get; set; }

    public string? SkinColor { get; set; }

    public string? FoundAmphetamine { get; set; }

    public string? Pregnancy { get; set; }

    public string? Advice7Note { get; set; }

    public string? Checkup { get; set; }

    public string? ErNote { get; set; }

    public string? FoundAllergy { get; set; }

    public string? Hpi { get; set; }

    public string? Pmh { get; set; }

    public string? Fh { get; set; }

    public string? Sh { get; set; }

    public string? Ros { get; set; }

    public double? Tc { get; set; }

    public double? Ldl { get; set; }

    public double? Ast { get; set; }

    public double? Alt { get; set; }

    public string? Symptom { get; set; }

    public int? WalkId { get; set; }

    public int? PeakFlow { get; set; }

    public double? Cholesterol { get; set; }

    public double? Waist { get; set; }

    public string? Advice8 { get; set; }

    public string? BreastFeeding { get; set; }

    public string? CradleLie { get; set; }

    public int? PainScore { get; set; }

    public int? Pefr { get; set; }

    public int? OpdscreenPatientTypeId { get; set; }

    public double? CreatinineKidneyPercent { get; set; }

    public double? Sodium { get; set; }

    public double? Chloride { get; set; }

    public double? Potassium { get; set; }

    public double? Tco2 { get; set; }

    public int? SmokingTypeId { get; set; }

    public int? DrinkingTypeId { get; set; }

    public int? PulseRegulationTypeId { get; set; }

    public double? Spo2 { get; set; }

    public double? UrineAlbumin { get; set; }

    public double? UrineCreatinine { get; set; }

    public double? PefrPercent { get; set; }

    public int? MacroAlbumin { get; set; }

    public int? MicroAlbumin { get; set; }

    public double? Egfr { get; set; }

    public double? Hb { get; set; }

    public double? Upcr { get; set; }

    public double? Bicarb { get; set; }

    public double? Phosphate { get; set; }

    public double? Pth { get; set; }

    public string? PeGy { get; set; }

    public string? PeGyText { get; set; }

    public string? PeGu { get; set; }

    public string? PeGuText { get; set; }

    public string? PeGi { get; set; }

    public string? PeGiText { get; set; }

    public double? Bsa { get; set; }

    public string? PeHead { get; set; }

    public string? PeHeadText { get; set; }

    public string? PeSkin { get; set; }

    public string? PeSkinText { get; set; }

    public string? G6pd { get; set; }

    public string? PeRtf { get; set; }

    public int? O2sat { get; set; }

    public string? PePv { get; set; }

    public string? PePvText { get; set; }

    public string? PePr { get; set; }

    public string? PePrText { get; set; }

    public string? PeGen { get; set; }

    public string? PeGenText { get; set; }

    public int? PrePainScore { get; set; }

    public int? PostPainScore { get; set; }

    public double? HeadCricumference { get; set; }

    public double? Fev1Percent { get; set; }

    public byte[]? PeRtfBlob { get; set; }

    public string? BpStable { get; set; }

    public string? PeChest { get; set; }

    public string? PeChestText { get; set; }

    public DateOnly? LmpDate { get; set; }

    public int? OpdscreenBpLocTypeId { get; set; }

    public int? MenstrualCycleTypeId { get; set; }

    public double? AdherencePercent { get; set; }

    public double? Fev1Fevc { get; set; }

    public int? VaccineScreenTypeId { get; set; }

    public int? DevelopmentScreenTypeId { get; set; }

    public string? Ambu { get; set; }

    public DateTime? UpdateDatetime { get; set; }
}
