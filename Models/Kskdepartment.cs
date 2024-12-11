using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Kskdepartment
{
    public string Depcode { get; set; } = null!;

    public string? Department { get; set; }

    public string? Roomno { get; set; }

    public string? DoctorCode { get; set; }

    public DateTime? OnlineTime { get; set; }

    public string? OnDesk { get; set; }

    public string? Spclty { get; set; }

    public string? ScreenVisible { get; set; }

    public string? DoctorVisible { get; set; }

    public string? RegistryVisible { get; set; }

    public string? RxVisible { get; set; }

    public string? StatusOpen { get; set; }

    public string? CashierVisible { get; set; }

    public string? MedicationCheck { get; set; }

    public string? CanPrintSticker { get; set; }

    public string? DepcodeActive { get; set; }

    public int? HospitalDepartmentId { get; set; }

    public string? PrintStickerCheck { get; set; }

    public string? OtServicePay { get; set; }

    public string? HosGuid { get; set; }

    public string? Oldcode { get; set; }

    public string? DepConfirmCode { get; set; }

    public string? IpdVisible { get; set; }

    public string? DepartmentActive { get; set; }

    public string? OpdFileTracking { get; set; }

    public string? SecondLineText { get; set; }

    public double? DfPercent { get; set; }

    public string? DfDecPrice { get; set; }

    public int? CurrentWaitingTime { get; set; }

    public string? AutoConfirmMedpay { get; set; }

    public int? StockDepartmentId { get; set; }

    public int? DefaultStockDepartmentId { get; set; }

    public int? DisplayOrder { get; set; }

    public string? CheckSpcltyWb { get; set; }

    public string? ReferPoint { get; set; }

    public int? EmpDepId { get; set; }

    public string? ForceSelectDoctor { get; set; }

    public string? AutoApplyStockDepartment { get; set; }

    public string? ForceSelectClinicDoctor { get; set; }

    public string? ShowAllAdviceItem { get; set; }

    public string? ShowDoctorRegistQueue { get; set; }

    public int? OpdQsLocationId { get; set; }

    public int? OpdQsRoomId { get; set; }

    public string? DisplayText { get; set; }

    public string? DepCode { get; set; }

    public string? InvNoWarnQty0 { get; set; }

    public string? ForceDxEntry { get; set; }

    public string? ForceNhsoAuthenVisit { get; set; }

    public string? CheckMissmatchSpclty { get; set; }

    public string? CheckMissmatchDepcode { get; set; }

    public string? OtServiceIcode { get; set; }

    public string? ForceScreenSmoking { get; set; }

    public string? PhoneNumber { get; set; }

    public string? ForceDoctorPeEntry { get; set; }

    public string? ShowNhsoFeeSchedule { get; set; }

    public string? ShowNhsoConfirmService { get; set; }
}
