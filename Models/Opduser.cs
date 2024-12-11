using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Opduser
{
    public string Loginname { get; set; } = null!;

    public string? Name { get; set; }

    public string? Password { get; set; }

    public string? Passweb { get; set; }

    public string? Accessright { get; set; }

    public string? Department { get; set; }

    public string? Departmentposition { get; set; }

    public string? Entryposition { get; set; }

    public byte[]? Picture { get; set; }

    public string? Startfullscreen { get; set; }

    public string? Doctorcode { get; set; }

    public sbyte? DrugAccessLevel { get; set; }

    public string? Groupname { get; set; }

    public string? VisibleMenu { get; set; }

    public string? Viewallmenu { get; set; }

    public string? LabStaff { get; set; }

    public int? HospitalDepartmentId { get; set; }

    public string? NhsoUser { get; set; }

    public string? NhsoPassword { get; set; }

    public int? MaxStation { get; set; }

    public string? ShowTip { get; set; }

    public DateOnly? PasswordExpireDate { get; set; }

    public int? PasswordRecheckDate { get; set; }

    public DateOnly? NewPasswordDate { get; set; }

    public string? CheckLabPassword { get; set; }

    public string? PcuUser { get; set; }

    public string? AccountDisable { get; set; }

    public string? RestrictWardAccess { get; set; }

    public string? RealStaff { get; set; }

    public string? RestrictClinicAccess { get; set; }

    public string? NoLabResultDisplay { get; set; }

    public string? NoDoctorConsultDisplay { get; set; }

    public string? NoAnnounceDisplay { get; set; }

    public int? AnnounceReadCount { get; set; }

    public string? XrayStaff { get; set; }

    public string? HosGuid { get; set; }

    public string? LabCheckPassword { get; set; }

    public string? Cid { get; set; }

    public string? Turbohosxp { get; set; }

    public string? HosGuidExt { get; set; }

    public int? AutoLogoutMinute { get; set; }

    public string? IclaimJwt { get; set; }

    public string? MophAccUser { get; set; }

    public string? MophAccPassword { get; set; }

    public string? SendMophOtp { get; set; }

    public string? PasswordText { get; set; }
}
