using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Clinicmember
{
    public int ClinicmemberId { get; set; }

    public string? Clinic { get; set; }

    public string? Doctor { get; set; }

    public string? Hn { get; set; }

    public string? Note { get; set; }

    public DateOnly? Regdate { get; set; }

    public DateOnly? Lastvisit { get; set; }

    public string? Pttype { get; set; }

    public string? LastVn { get; set; }

    public DateOnly? NextAppDate { get; set; }

    public int? BeginYear { get; set; }

    public DateOnly? Dchdate { get; set; }

    public string? CurrentStatus { get; set; }

    public sbyte? Subtype { get; set; }

    public int? Number { get; set; }

    public string? NodeId { get; set; }

    public string? SendToPcu { get; set; }

    public DateOnly? SendToPcuDate { get; set; }

    public string? ReferFromPcu { get; set; }

    public DateOnly? ReferFromPcuDate { get; set; }

    public int? ReferFromPcuCause { get; set; }

    public string? ReferFromPcuHcode { get; set; }

    public string? SpecialCase { get; set; }

    public string? NewCase { get; set; }

    public int? AgeY { get; set; }

    public string? Sex { get; set; }

    public string? SpecialId { get; set; }

    public string? SendToPcuHcode { get; set; }

    public int? PtNumber { get; set; }

    public string? ChronicType { get; set; }

    public int? ChronicLevel { get; set; }

    public DateOnly? PeriodBeginDate { get; set; }

    public DateOnly? Mo1VisitDate { get; set; }

    public DateOnly? Mo2VisitDate { get; set; }

    public DateOnly? Mo3VisitDate { get; set; }

    public DateOnly? Mo4VisitDate { get; set; }

    public DateOnly? Mo5VisitDate { get; set; }

    public DateOnly? Mo6VisitDate { get; set; }

    public DateOnly? Mo7VisitDate { get; set; }

    public DateOnly? Mo8VisitDate { get; set; }

    public DateOnly? Mo9VisitDate { get; set; }

    public DateOnly? Mo10VisitDate { get; set; }

    public DateOnly? Mo11VisitDate { get; set; }

    public DateOnly? Mo12VisitDate { get; set; }

    public int? ClinicMemberStatusId { get; set; }

    public DateTime? Lastupdate { get; set; }

    public string? HosGuid { get; set; }

    public int? ClinicSubtypeId { get; set; }

    public string? OtherChronicText { get; set; }

    public string? DwChronicNumber { get; set; }

    public int? AppointmentVisitFrequency { get; set; }

    public DateTime? EntryDatetime { get; set; }

    public string? EntryStaff { get; set; }

    public string? ModifyStaff { get; set; }

    public string? ReferRegisterFromHospcode { get; set; }

    public string? HasEyeCormobidity { get; set; }

    public string? LastCormobidityScreenVn { get; set; }

    public string? HasFootCormobidity { get; set; }

    public string? HasCardiovascularCormobidity { get; set; }

    public string? HasCerebrovascularCormobidity { get; set; }

    public string? HasPeripheralvascularCormobidity { get; set; }

    public string? HasDentalCormobidity { get; set; }

    public string? HasKidneyCormobidity { get; set; }

    public string? RegisterHospcode { get; set; }

    public string? Discharge { get; set; }

    public string? WithPregnancy { get; set; }

    public string? WithHypertension { get; set; }

    public string? WithInsulin { get; set; }

    public string? PreRegister { get; set; }

    public DateOnly? LastHba1cDate { get; set; }

    public double? LastHba1cValue { get; set; }

    public DateOnly? LastUaDate { get; set; }

    public double? LastUaValue { get; set; }

    public DateOnly? LastBpDate { get; set; }

    public double? LastBpBpsValue { get; set; }

    public double? LastBpBpdValue { get; set; }

    public string? NapNumber { get; set; }

    public string? SendToPcuNote { get; set; }

    public int? LastFbsValue { get; set; }

    public DateOnly? LastFbsDate { get; set; }

    public int? HhcRegisterId { get; set; }

    public DateTime? LastCloudSyncDatetime { get; set; }

    public string? MarkCorrect43 { get; set; }

    public DateOnly? DxDate { get; set; }

    public string? DxHospcode { get; set; }

    public DateOnly? BeginDate { get; set; }

    public string? SctId { get; set; }

    public string? SctDesc { get; set; }
}
