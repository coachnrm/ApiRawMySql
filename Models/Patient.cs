using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Patient
{
    public string HosGuid { get; set; } = null!;

    public string? Hn { get; set; }

    public string? Pname { get; set; }

    public string? Fname { get; set; }

    public string? Lname { get; set; }

    public string? Occupation { get; set; }

    public string? Citizenship { get; set; }

    public DateOnly? Birthday { get; set; }

    public string? Addrpart { get; set; }

    public string? Moopart { get; set; }

    public string? Tmbpart { get; set; }

    public string? Amppart { get; set; }

    public string? Chwpart { get; set; }

    public string? Bloodgrp { get; set; }

    public string? Clinic { get; set; }

    public DateOnly? Deathday { get; set; }

    public string? Drugallergy { get; set; }

    public int? Familyno { get; set; }

    public string? Fathername { get; set; }

    public DateOnly? Firstday { get; set; }

    public string? Hometel { get; set; }

    public string? Informaddr { get; set; }

    public string? Informname { get; set; }

    public string? Informrelation { get; set; }

    public string? Informtel { get; set; }

    public string? Marrystatus { get; set; }

    public string? Mathername { get; set; }

    public int? HnInt { get; set; }

    public string? Nationality { get; set; }

    public string? Opdlocation { get; set; }

    public string? Pttype { get; set; }

    public string? Religion { get; set; }

    public string? Sex { get; set; }

    public string? Spsname { get; set; }

    public string? Truebirthday { get; set; }

    public string? Workaddr { get; set; }

    public string? Worktel { get; set; }

    public string? Hcode { get; set; }

    public string? Cid { get; set; }

    public int? Hid { get; set; }

    public string? Educate { get; set; }

    public string? FamilyStatus { get; set; }

    public string? LaborType { get; set; }

    public DateTime? LastUpdate { get; set; }

    public string? TypeArea { get; set; }

    public string? Road { get; set; }

    public string? FatherCid { get; set; }

    public string? MotherCid { get; set; }

    public string? CoupleCid { get; set; }

    public string? PersonType { get; set; }

    public string? PrivateDoctorName { get; set; }

    public string? LegalAction { get; set; }

    public string? DeathCode504 { get; set; }

    public string? DeathDiag { get; set; }

    public string? NodeId { get; set; }

    public string? Admit { get; set; }

    public string? Midname { get; set; }

    public string? PoCode { get; set; }

    public string? Fatherlname { get; set; }

    public string? Motherlname { get; set; }

    public string? Spslname { get; set; }

    public string? Country { get; set; }

    public string? Email { get; set; }

    public TimeOnly? Birthtime { get; set; }

    public string? MotherHn { get; set; }

    public DateOnly? LastVisit { get; set; }

    public string? Death { get; set; }

    public int? Height { get; set; }

    public string? Inregion { get; set; }

    public TimeOnly? RegTime { get; set; }

    public string? Oldcode { get; set; }

    public string? Lang { get; set; }

    public string? GovChronicId { get; set; }

    public string? InCups { get; set; }

    public sbyte? PatientTypeId { get; set; }

    public string? AddrSoi { get; set; }

    public string? WorkAddr { get; set; }

    public string? FatherHn { get; set; }

    public string? AliasName { get; set; }

    public string? Destroyed { get; set; }

    public string? OldAddr { get; set; }

    public string? FnameSoundex { get; set; }

    public string? LnameSoundex { get; set; }

    public string? BloodgroupRh { get; set; }

    public string? TranStatus { get; set; }

    public string? PassportNo { get; set; }

    public string? Addressid { get; set; }

    public string? MobilePhoneNumber { get; set; }

    public string? AnonymousPerson { get; set; }

    public int? HospitalDepartmentId { get; set; }

    public string? MembercardNo { get; set; }

    public string? EcFname { get; set; }

    public string? EcLname { get; set; }

    public int? EcRelationTypeId { get; set; }

    public int? PatientColorId { get; set; }

    public int? NumberOfRelatives { get; set; }

    public int? BirthOrder { get; set; }

    public int? PersonLaborTypeId { get; set; }

    public string? IsCardDestroy { get; set; }

    public DateOnly? CardDestroyDate { get; set; }

    public string? G6pd { get; set; }

    public string? Vid { get; set; }
}
