using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class LabHead
{
    public int LabOrderNumber { get; set; }

    public string? DoctorCode { get; set; }

    public int? LabItemsGroupCode { get; set; }

    public string? Vn { get; set; }

    public string? LabHeadRemark { get; set; }

    public string? Hn { get; set; }

    public DateOnly? OrderDate { get; set; }

    public DateOnly? ReportDate { get; set; }

    public string? ReporterName { get; set; }

    public TimeOnly? ReportTime { get; set; }

    public string? ConfirmSpecimen { get; set; }

    public string? ConfirmReport { get; set; }

    public string? Department { get; set; }

    public string? FormName { get; set; }

    public string? SubGroupList { get; set; }

    public TimeOnly? OrderTime { get; set; }

    public DateOnly? ReceiveDate { get; set; }

    public TimeOnly? ReceiveTime { get; set; }

    public string? Ward { get; set; }

    public string? ResultNote { get; set; }

    public string? LockResult { get; set; }

    public string? ResultRtf { get; set; }

    public string? OrderNote { get; set; }

    public int? LabReceiveNumber { get; set; }

    public string? ApproveStaff { get; set; }

    public string? Anonymous { get; set; }

    public string? Appointment { get; set; }

    public DateOnly? AppointmentDate { get; set; }

    public TimeOnly? AppointmentTime { get; set; }

    public string? LisOrderNo { get; set; }

    public string? ConfirmChargeMoney { get; set; }

    public string? FinanceLabConfirm { get; set; }

    public string? LabOrderNumberGuid { get; set; }

    public string? AnonymousRequest { get; set; }

    public string? Clinic { get; set; }

    public string? SendToCashier { get; set; }

    public string? ReceiveComputer { get; set; }

    public string? HosGuid { get; set; }

    public string? OrderDepartment { get; set; }

    public int? LabPerformStatusId { get; set; }

    public int? LabRoomId { get; set; }

    public string? NotifyDepcode { get; set; }

    public int? LabPriorityId { get; set; }

    public string? Spclty { get; set; }

    public string? FinanceLock { get; set; }

    public string? ReportLock { get; set; }

    public DateTime? ReportLockDatetime { get; set; }

    public string? ReportLockComputer { get; set; }

    public int? BatchNumber { get; set; }

    public string? AbnormalResult { get; set; }

    public string? LisTubeMachine { get; set; }

    public string? OrderComputerName { get; set; }

    public string? LisCalcFinance { get; set; }

    public int? HospitalDepartmentId { get; set; }

    public string? SendMobileMsg { get; set; }

    public string? OrderStaff { get; set; }

    public string? HasManualItem { get; set; }

    public int? LinkLabOrderNumber { get; set; }

    public int? SpecimenCode { get; set; }

    public string? LabReceive { get; set; }

    public string? FinanceLabClear { get; set; }

    public string? ReceiveStaff { get; set; }

    public string? FollowUpVn { get; set; }

    public int? LabHeadRejectTypeId { get; set; }

    public string? MasterFormName { get; set; }

    public DateTime? DoctorAckDatetime { get; set; }

    public DateTime? EntryDatetime { get; set; }

    public DateTime? UpdateDatetime { get; set; }

    public int? ItemCount { get; set; }

    public int? LabRequestTypeId { get; set; }

    public int? ImageCount { get; set; }

    public int? FormId { get; set; }

    public int? MasterFormId { get; set; }

    public int? OpdQsSlotId { get; set; }

    public string? RefOrderCode { get; set; }

    public string? IsOutlab { get; set; }

    public int? DoctorCertId { get; set; }

    public int? ReportDoctorCertId { get; set; }
}
