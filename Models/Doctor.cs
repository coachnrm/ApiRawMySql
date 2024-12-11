using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Doctor
{
    public string Code { get; set; } = null!;

    public string? Name { get; set; }

    public string? Shortname { get; set; }

    public string? Licenseno { get; set; }

    public string? Department { get; set; }

    public string? Jobposition { get; set; }

    public string? Active { get; set; }

    public string? ForceDiagnosis { get; set; }

    public string? Oldcode { get; set; }

    public string? SearchKeyword { get; set; }

    public string? Cid { get; set; }

    public int? PositionId { get; set; }

    public string? Addrpart { get; set; }

    public string? Moopart { get; set; }

    public string? Zoipart { get; set; }

    public string? Roadpart { get; set; }

    public string? Tmbpart { get; set; }

    public string? Amppart { get; set; }

    public string? Chwpart { get; set; }

    public string? Nationality { get; set; }

    public string? DoctorGuid { get; set; }

    public string? AllowDfEdit { get; set; }

    public string? ForceIcdDiagnosis { get; set; }

    public string? Ename { get; set; }

    public string? Spclty { get; set; }

    public string? Clinic { get; set; }

    public int? DoctorDepartmentId { get; set; }

    public string? NameSoundex { get; set; }

    public int? RepOr { get; set; }

    public string? ChronicStaff { get; set; }

    public string? HosGuid { get; set; }

    public string? ProviderTypeCode { get; set; }

    public string? Sex { get; set; }

    public string? CouncilCode { get; set; }

    public DateOnly? BirthDate { get; set; }

    public DateOnly? StartDate { get; set; }

    public DateOnly? FinishDate { get; set; }

    public string? MoveFromHospcode { get; set; }

    public string? MoveToHospcode { get; set; }

    public DateTime? UpdateDatetime { get; set; }

    public string? Pname { get; set; }

    public string? Lname { get; set; }

    public string? Fname { get; set; }

    public int? EmpId { get; set; }

    public int? SubSpcltyId { get; set; }

    public int? DoctorTypeId { get; set; }

    public string? HospitalList { get; set; }

    public DateOnly? LicenseIssueDate { get; set; }

    public DateOnly? LicenseExpireDate { get; set; }

    public string? UseAppSlot { get; set; }

    public string? AllowOnlineAppointment { get; set; }

    public string? QueuePrefix { get; set; }

    public string? EnableQsCall { get; set; }

    public string? NoRequireQsSlot { get; set; }

    public string? RegistNo { get; set; }

    public string? UseWeekSlot { get; set; }

    public string? CanApproveIpdOrder { get; set; }

    public string? AllowAppointmentOverSlot { get; set; }

    public string? LineNotifyToken { get; set; }

    public string? LineNotifyIpdLabCritical { get; set; }
}
