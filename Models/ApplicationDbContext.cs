using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore;
using Pomelo.EntityFrameworkCore.MySql.Scaffolding.Internal;

namespace RealtimeDataApp.Models;

public partial class ApplicationDbContext : DbContext
{
    public ApplicationDbContext()
    {
    }

    public ApplicationDbContext(DbContextOptions<ApplicationDbContext> options)
        : base(options)
    {
    }

    public virtual DbSet<AnStat> AnStats { get; set; }

    public virtual DbSet<Clinic> Clinics { get; set; }

    public virtual DbSet<ClinicVisit> ClinicVisits { get; set; }

    public virtual DbSet<Clinicmember> Clinicmembers { get; set; }

    public virtual DbSet<Doctor> Doctors { get; set; }

    public virtual DbSet<Drugitem> Drugitems { get; set; }

    public virtual DbSet<Drugusage> Drugusages { get; set; }

    public virtual DbSet<Hospcode> Hospcodes { get; set; }

    public virtual DbSet<Icd101> Icd101s { get; set; }

    public virtual DbSet<Ipt> Ipts { get; set; }

    public virtual DbSet<Iptadm> Iptadms { get; set; }

    public virtual DbSet<Kskdepartment> Kskdepartments { get; set; }

    public virtual DbSet<LabHead> LabHeads { get; set; }

    public virtual DbSet<LabItemsSubGroup> LabItemsSubGroups { get; set; }

    public virtual DbSet<LabOrder> LabOrders { get; set; }

    public virtual DbSet<LabOrderImage> LabOrderImages { get; set; }

    public virtual DbSet<Nondrugitem> Nondrugitems { get; set; }

    public virtual DbSet<Oapp> Oapps { get; set; }

    public virtual DbSet<Opdscreen> Opdscreens { get; set; }

    public virtual DbSet<Opduser> Opdusers { get; set; }

    public virtual DbSet<Opitemrece> Opitemreces { get; set; }

    public virtual DbSet<Ovst> Ovsts { get; set; }

    public virtual DbSet<Ovstist> Ovstists { get; set; }

    public virtual DbSet<Ovstost> Ovstosts { get; set; }

    public virtual DbSet<Patient> Patients { get; set; }

    public virtual DbSet<PatientImage> PatientImages { get; set; }

    public virtual DbSet<Ptcardno> Ptcardnos { get; set; }

    public virtual DbSet<Ptnote> Ptnotes { get; set; }

    public virtual DbSet<Pttype> Pttypes { get; set; }

    public virtual DbSet<Pttypeno> Pttypenos { get; set; }

    public virtual DbSet<SDrugitem> SDrugitems { get; set; }

    public virtual DbSet<Serial> Serials { get; set; }

    public virtual DbSet<SpUse> SpUses { get; set; }

    public virtual DbSet<Spclty> Spclties { get; set; }

    public virtual DbSet<VnStat> VnStats { get; set; }

    public virtual DbSet<Ward> Wards { get; set; }

    public virtual DbSet<XrayHead> XrayHeads { get; set; }

    public virtual DbSet<XrayReport> XrayReports { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
#warning To protect potentially sensitive information in your connection string, you should move it out of source code. You can avoid scaffolding the connection string by using the Name= syntax to read it from configuration - see https://go.microsoft.com/fwlink/?linkid=2131148. For more guidance on storing connection strings, see https://go.microsoft.com/fwlink/?LinkId=723263.
        => optionsBuilder.UseMySql("server=172.16.5.39;port=3306;database=hos;userid=hks;password=\"Fi'rpk[k]@!#\"", Microsoft.EntityFrameworkCore.ServerVersion.Parse("10.1.37-mariadb"));

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder
            .UseCollation("tis620_thai_ci")
            .HasCharSet("tis620");

        modelBuilder.Entity<AnStat>(entity =>
        {
            entity.HasKey(e => e.An).HasName("PRIMARY");

            entity.ToTable("an_stat");

            entity.HasIndex(e => e.Dchdate, "ix_dchdate");

            entity.HasIndex(e => new { e.Hn, e.Regdate }, "ix_hn_regdate");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.Pcode, "ix_pcode");

            entity.HasIndex(e => e.Pttype, "ix_pttype");

            entity.HasIndex(e => e.Regdate, "ix_regdate");

            entity.Property(e => e.An)
                .HasMaxLength(9)
                .HasColumnName("an");
            entity.Property(e => e.AccidentCode)
                .HasMaxLength(6)
                .HasColumnName("accident_code");
            entity.Property(e => e.Admdate)
                .HasColumnType("int(11)")
                .HasColumnName("admdate");
            entity.Property(e => e.AdmdateCut24)
                .HasColumnType("int(11)")
                .HasColumnName("admdate_cut24");
            entity.Property(e => e.AdmitHour)
                .HasColumnType("int(11)")
                .HasColumnName("admit_hour");
            entity.Property(e => e.AgeD)
                .HasColumnType("tinyint(4)")
                .HasColumnName("age_d");
            entity.Property(e => e.AgeM)
                .HasColumnType("tinyint(4)")
                .HasColumnName("age_m");
            entity.Property(e => e.AgeY)
                .HasColumnType("tinyint(4)")
                .HasColumnName("age_y");
            entity.Property(e => e.Aid)
                .HasMaxLength(6)
                .HasColumnName("aid");
            entity.Property(e => e.AnGuid)
                .HasMaxLength(38)
                .HasColumnName("an_guid");
            entity.Property(e => e.ArTransferDatetimeSk)
                .HasColumnType("datetime")
                .HasColumnName("ar_transfer_datetime_sk");
            entity.Property(e => e.ArTransferSk)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ar_transfer_sk");
            entity.Property(e => e.ArTransferStaffSk)
                .HasMaxLength(25)
                .HasColumnName("ar_transfer_staff_sk");
            entity.Property(e => e.CountInMonth)
                .HasColumnType("tinyint(4)")
                .HasColumnName("count_in_month");
            entity.Property(e => e.CountInYear)
                .HasColumnType("tinyint(4)")
                .HasColumnName("count_in_year");
            entity.Property(e => e.Dchdate).HasColumnName("dchdate");
            entity.Property(e => e.DebtIdList)
                .HasMaxLength(50)
                .HasColumnName("debt_id_list");
            entity.Property(e => e.DebtMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("debt_money");
            entity.Property(e => e.DiagTextList)
                .HasMaxLength(200)
                .HasColumnName("diag_text_list");
            entity.Property(e => e.DiscountMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("discount_money");
            entity.Property(e => e.Drg)
                .HasMaxLength(6)
                .HasColumnName("drg");
            entity.Property(e => e.Dx0)
                .HasMaxLength(6)
                .HasColumnName("dx0");
            entity.Property(e => e.Dx1)
                .HasMaxLength(6)
                .HasColumnName("dx1");
            entity.Property(e => e.Dx2)
                .HasMaxLength(6)
                .HasColumnName("dx2");
            entity.Property(e => e.Dx3)
                .HasMaxLength(6)
                .HasColumnName("dx3");
            entity.Property(e => e.Dx4)
                .HasMaxLength(6)
                .HasColumnName("dx4");
            entity.Property(e => e.Dx5)
                .HasMaxLength(6)
                .HasColumnName("dx5");
            entity.Property(e => e.DxDoctor)
                .HasMaxLength(7)
                .HasColumnName("dx_doctor");
            entity.Property(e => e.Gr504)
                .HasColumnType("smallint(6)")
                .HasColumnName("gr504");
            entity.Property(e => e.HasMedRecon)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_med_recon");
            entity.Property(e => e.HasRefillMedplan)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_refill_medplan");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.Inc01)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc01");
            entity.Property(e => e.Inc02)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc02");
            entity.Property(e => e.Inc03)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc03");
            entity.Property(e => e.Inc04)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc04");
            entity.Property(e => e.Inc05)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc05");
            entity.Property(e => e.Inc06)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc06");
            entity.Property(e => e.Inc07)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc07");
            entity.Property(e => e.Inc08)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc08");
            entity.Property(e => e.Inc09)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc09");
            entity.Property(e => e.Inc10)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc10");
            entity.Property(e => e.Inc11)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc11");
            entity.Property(e => e.Inc12)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc12");
            entity.Property(e => e.Inc13)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc13");
            entity.Property(e => e.Inc14)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc14");
            entity.Property(e => e.Inc15)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc15");
            entity.Property(e => e.Inc16)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc16");
            entity.Property(e => e.Inc17)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc17");
            entity.Property(e => e.Income)
                .HasColumnType("double(15,3)")
                .HasColumnName("income");
            entity.Property(e => e.ItemMoney)
                .HasColumnType("double(22,3)")
                .HasColumnName("item_money");
            entity.Property(e => e.LastBpd)
                .HasColumnType("int(11)")
                .HasColumnName("last_bpd");
            entity.Property(e => e.LastBps)
                .HasColumnType("int(11)")
                .HasColumnName("last_bps");
            entity.Property(e => e.LastSosScore)
                .HasColumnType("int(11)")
                .HasColumnName("last_sos_score");
            entity.Property(e => e.LastSyncDatetime)
                .HasColumnType("datetime")
                .HasColumnName("last_sync_datetime");
            entity.Property(e => e.LastTemperature)
                .HasColumnType("double(15,1)")
                .HasColumnName("last_temperature");
            entity.Property(e => e.Lastvisit)
                .HasColumnType("int(11)")
                .HasColumnName("lastvisit");
            entity.Property(e => e.LastvisitHour)
                .HasColumnType("int(11)")
                .HasColumnName("lastvisit_hour");
            entity.Property(e => e.Los)
                .HasColumnType("double(15,3)")
                .HasColumnName("los");
            entity.Property(e => e.Moopart)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("moopart");
            entity.Property(e => e.OldDiagnosis)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("old_diagnosis");
            entity.Property(e => e.Op0)
                .HasMaxLength(6)
                .HasColumnName("op0");
            entity.Property(e => e.Op1)
                .HasMaxLength(6)
                .HasColumnName("op1");
            entity.Property(e => e.Op2)
                .HasMaxLength(6)
                .HasColumnName("op2");
            entity.Property(e => e.Op3)
                .HasMaxLength(6)
                .HasColumnName("op3");
            entity.Property(e => e.Op4)
                .HasMaxLength(6)
                .HasColumnName("op4");
            entity.Property(e => e.Op5)
                .HasMaxLength(6)
                .HasColumnName("op5");
            entity.Property(e => e.Op6)
                .HasMaxLength(6)
                .HasColumnName("op6");
            entity.Property(e => e.OpdWaitMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("opd_wait_money");
            entity.Property(e => e.Ot)
                .HasColumnType("double(15,3)")
                .HasColumnName("ot");
            entity.Property(e => e.PaidMoney)
                .HasColumnType("double(22,3)")
                .HasColumnName("paid_money");
            entity.Property(e => e.Pcode)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pcode");
            entity.Property(e => e.Pdx)
                .HasMaxLength(6)
                .HasColumnName("pdx");
            entity.Property(e => e.PrescNedIncomplete)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("presc_ned_incomplete");
            entity.Property(e => e.PrintCount)
                .HasColumnType("tinyint(4)")
                .HasColumnName("print_count");
            entity.Property(e => e.PrintDone)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_done");
            entity.Property(e => e.Pttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pttype");
            entity.Property(e => e.PttypeInRegion)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pttype_in_region");
            entity.Property(e => e.PttypeListText)
                .HasMaxLength(200)
                .HasColumnName("pttype_list_text");
            entity.Property(e => e.Pttypeno)
                .HasMaxLength(25)
                .HasColumnName("pttypeno");
            entity.Property(e => e.RcpnoList)
                .HasMaxLength(100)
                .HasColumnName("rcpno_list");
            entity.Property(e => e.RcptMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("rcpt_money");
            entity.Property(e => e.Regdate).HasColumnName("regdate");
            entity.Property(e => e.RemainMoney)
                .HasColumnType("double(22,3)")
                .HasColumnName("remain_money");
            entity.Property(e => e.Rw)
                .HasColumnType("double(15,5)")
                .HasColumnName("rw");
            entity.Property(e => e.RxLicenseNo)
                .HasMaxLength(15)
                .HasColumnName("rx_license_no");
            entity.Property(e => e.Sex)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("sex");
            entity.Property(e => e.Spclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.UcMoney)
                .HasColumnType("double(22,3)")
                .HasColumnName("uc_money");
            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.Ward)
                .HasMaxLength(4)
                .HasColumnName("ward");
        });

        modelBuilder.Entity<Clinic>(entity =>
        {
            entity.HasKey(e => e.Clinic1).HasName("PRIMARY");

            entity.ToTable("clinic");

            entity.HasIndex(e => e.ClinicGuid, "ix_clinic_guid");

            entity.HasIndex(e => e.Name, "ix_name");

            entity.HasIndex(e => e.SssClinicCode, "ix_sss_clinic_code");

            entity.Property(e => e.Clinic1)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("clinic");
            entity.Property(e => e.ActiveStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("active_status");
            entity.Property(e => e.AppLimitQty)
                .HasColumnType("int(11)")
                .HasColumnName("app_limit_qty");
            entity.Property(e => e.Chronic)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("chronic");
            entity.Property(e => e.ClinicGuid)
                .HasMaxLength(38)
                .HasColumnName("clinic_guid");
            entity.Property(e => e.CloseTime)
                .HasColumnType("time")
                .HasColumnName("close_time");
            entity.Property(e => e.Depcode)
                .HasMaxLength(3)
                .HasColumnName("depcode");
            entity.Property(e => e.DisableDialog)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("disable_dialog");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosxpClinicTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("hosxp_clinic_type_id");
            entity.Property(e => e.Icd10)
                .HasMaxLength(6)
                .HasColumnName("icd10");
            entity.Property(e => e.KioskOpdQsRoomId)
                .HasColumnType("int(11)")
                .HasColumnName("kiosk_opd_qs_room_id");
            entity.Property(e => e.Labparam)
                .HasMaxLength(10)
                .HasColumnName("labparam");
            entity.Property(e => e.LimitAccess)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("limit_access");
            entity.Property(e => e.Location)
                .HasMaxLength(200)
                .HasColumnName("location");
            entity.Property(e => e.MophVaccineNcdId)
                .HasColumnType("int(11)")
                .HasColumnName("moph_vaccine_ncd_id");
            entity.Property(e => e.Name)
                .HasMaxLength(150)
                .HasColumnName("name");
            entity.Property(e => e.NoExport)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_export");
            entity.Property(e => e.OappActivityId)
                .HasMaxLength(5)
                .HasColumnName("oapp_activity_id");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(20)
                .HasColumnName("oldcode");
            entity.Property(e => e.OnlineNcdKey)
                .HasMaxLength(50)
                .HasColumnName("online_ncd_key");
            entity.Property(e => e.OnlineNcdSegmentId)
                .HasMaxLength(50)
                .HasColumnName("online_ncd_segment_id");
            entity.Property(e => e.OnlineRegisterTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("online_register_type_id");
            entity.Property(e => e.OpdKiosDepMenuId)
                .HasColumnType("int(11)")
                .HasColumnName("opd_kios_dep_menu_id");
            entity.Property(e => e.OpdQsScheduleTmplTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("opd_qs_schedule_tmpl_type_id");
            entity.Property(e => e.OpenTime)
                .HasColumnType("time")
                .HasColumnName("open_time");
            entity.Property(e => e.PcuCode)
                .HasColumnType("int(11)")
                .HasColumnName("pcu_code");
            entity.Property(e => e.Phone)
                .HasMaxLength(50)
                .HasColumnName("phone");
            entity.Property(e => e.PhrSkip)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("phr_skip");
            entity.Property(e => e.QueuePrefix)
                .HasMaxLength(5)
                .HasColumnName("queue_prefix");
            entity.Property(e => e.SkhOldName)
                .HasMaxLength(150)
                .HasColumnName("skh_old_name");
            entity.Property(e => e.SssClinicCode)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("sss_clinic_code");
            entity.Property(e => e.UseAppQsSlot)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_app_qs_slot");
        });

        modelBuilder.Entity<ClinicVisit>(entity =>
        {
            entity.HasKey(e => new { e.Clinic, e.Vn })
                .HasName("PRIMARY")
                .HasAnnotation("MySql:IndexPrefixLength", new[] { 0, 0 });

            entity.ToTable("clinic_visit");

            entity.HasIndex(e => e.Clinic, "clinic");

            entity.HasIndex(e => e.Hn, "hn");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.Property(e => e.Clinic)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("clinic");
            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.AfbCheck)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("afb_check");
            entity.Property(e => e.AfbMonthNumber)
                .HasColumnType("int(11)")
                .HasColumnName("afb_month_number");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.SkhInsert)
                .HasColumnType("int(11)")
                .HasColumnName("skh_insert");
            entity.Property(e => e.VisitType)
                .HasColumnType("int(11)")
                .HasColumnName("visit_type");
        });

        modelBuilder.Entity<Clinicmember>(entity =>
        {
            entity.HasKey(e => e.ClinicmemberId).HasName("PRIMARY");

            entity.ToTable("clinicmember");

            entity.HasIndex(e => e.Clinic, "ix_clinic");

            entity.HasIndex(e => new { e.Clinic, e.PtNumber }, "ix_clinic_pt_number");

            entity.HasIndex(e => e.ClinicMemberStatusId, "ix_clinicmember_status_id");

            entity.HasIndex(e => e.Doctor, "ix_doctor");

            entity.HasIndex(e => e.DwChronicNumber, "ix_dwcn");

            entity.HasIndex(e => e.Hn, "ix_hn");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.NodeId, "ix_node_id");

            entity.HasIndex(e => new { e.Hn, e.Clinic }, "ix_unique_hn_clinic").IsUnique();

            entity.Property(e => e.ClinicmemberId)
                .ValueGeneratedNever()
                .HasColumnType("int(11)")
                .HasColumnName("clinicmember_id");
            entity.Property(e => e.AgeY)
                .HasColumnType("int(11)")
                .HasColumnName("age_y");
            entity.Property(e => e.AppointmentVisitFrequency)
                .HasColumnType("int(11)")
                .HasColumnName("appointment_visit_frequency");
            entity.Property(e => e.BeginDate).HasColumnName("begin_date");
            entity.Property(e => e.BeginYear)
                .HasColumnType("int(11)")
                .HasColumnName("begin_year");
            entity.Property(e => e.ChronicLevel)
                .HasColumnType("int(11)")
                .HasColumnName("chronic_level");
            entity.Property(e => e.ChronicType)
                .HasMaxLength(100)
                .HasColumnName("chronic_type");
            entity.Property(e => e.Clinic)
                .HasMaxLength(9)
                .HasColumnName("clinic");
            entity.Property(e => e.ClinicMemberStatusId)
                .HasColumnType("int(11)")
                .HasColumnName("clinic_member_status_id");
            entity.Property(e => e.ClinicSubtypeId)
                .HasColumnType("int(11)")
                .HasColumnName("clinic_subtype_id");
            entity.Property(e => e.CurrentStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("current_status");
            entity.Property(e => e.Dchdate).HasColumnName("dchdate");
            entity.Property(e => e.Discharge)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("discharge");
            entity.Property(e => e.Doctor)
                .HasMaxLength(6)
                .HasColumnName("doctor");
            entity.Property(e => e.DwChronicNumber)
                .HasMaxLength(20)
                .HasColumnName("dw_chronic_number");
            entity.Property(e => e.DxDate).HasColumnName("dx_date");
            entity.Property(e => e.DxHospcode)
                .HasMaxLength(5)
                .IsFixedLength()
                .HasColumnName("dx_hospcode");
            entity.Property(e => e.EntryDatetime)
                .HasColumnType("datetime")
                .HasColumnName("entry_datetime");
            entity.Property(e => e.EntryStaff)
                .HasMaxLength(25)
                .HasColumnName("entry_staff");
            entity.Property(e => e.HasCardiovascularCormobidity)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_cardiovascular_cormobidity");
            entity.Property(e => e.HasCerebrovascularCormobidity)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_cerebrovascular_cormobidity");
            entity.Property(e => e.HasDentalCormobidity)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_dental_cormobidity");
            entity.Property(e => e.HasEyeCormobidity)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_eye_cormobidity");
            entity.Property(e => e.HasFootCormobidity)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_foot_cormobidity");
            entity.Property(e => e.HasKidneyCormobidity)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_kidney_cormobidity");
            entity.Property(e => e.HasPeripheralvascularCormobidity)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_peripheralvascular_cormobidity");
            entity.Property(e => e.HhcRegisterId)
                .HasColumnType("int(11)")
                .HasColumnName("hhc_register_id");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.LastBpBpdValue)
                .HasColumnType("double(15,3)")
                .HasColumnName("last_bp_bpd_value");
            entity.Property(e => e.LastBpBpsValue)
                .HasColumnType("double(15,3)")
                .HasColumnName("last_bp_bps_value");
            entity.Property(e => e.LastBpDate).HasColumnName("last_bp_date");
            entity.Property(e => e.LastCloudSyncDatetime)
                .HasColumnType("datetime")
                .HasColumnName("last_cloud_sync_datetime");
            entity.Property(e => e.LastCormobidityScreenVn)
                .HasMaxLength(12)
                .HasColumnName("last_cormobidity_screen_vn");
            entity.Property(e => e.LastFbsDate).HasColumnName("last_fbs_date");
            entity.Property(e => e.LastFbsValue)
                .HasColumnType("int(11)")
                .HasColumnName("last_fbs_value");
            entity.Property(e => e.LastHba1cDate).HasColumnName("last_hba1c_date");
            entity.Property(e => e.LastHba1cValue)
                .HasColumnType("double(15,3)")
                .HasColumnName("last_hba1c_value");
            entity.Property(e => e.LastUaDate).HasColumnName("last_ua_date");
            entity.Property(e => e.LastUaValue)
                .HasColumnType("double(15,3)")
                .HasColumnName("last_ua_value");
            entity.Property(e => e.LastVn)
                .HasMaxLength(12)
                .HasColumnName("last_vn");
            entity.Property(e => e.Lastupdate)
                .HasColumnType("datetime")
                .HasColumnName("lastupdate");
            entity.Property(e => e.Lastvisit).HasColumnName("lastvisit");
            entity.Property(e => e.MarkCorrect43)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("mark_correct_43");
            entity.Property(e => e.Mo10VisitDate).HasColumnName("mo10_visit_date");
            entity.Property(e => e.Mo11VisitDate).HasColumnName("mo11_visit_date");
            entity.Property(e => e.Mo12VisitDate).HasColumnName("mo12_visit_date");
            entity.Property(e => e.Mo1VisitDate).HasColumnName("mo1_visit_date");
            entity.Property(e => e.Mo2VisitDate).HasColumnName("mo2_visit_date");
            entity.Property(e => e.Mo3VisitDate).HasColumnName("mo3_visit_date");
            entity.Property(e => e.Mo4VisitDate).HasColumnName("mo4_visit_date");
            entity.Property(e => e.Mo5VisitDate).HasColumnName("mo5_visit_date");
            entity.Property(e => e.Mo6VisitDate).HasColumnName("mo6_visit_date");
            entity.Property(e => e.Mo7VisitDate).HasColumnName("mo7_visit_date");
            entity.Property(e => e.Mo8VisitDate).HasColumnName("mo8_visit_date");
            entity.Property(e => e.Mo9VisitDate).HasColumnName("mo9_visit_date");
            entity.Property(e => e.ModifyStaff)
                .HasMaxLength(25)
                .HasColumnName("modify_staff");
            entity.Property(e => e.NapNumber)
                .HasMaxLength(50)
                .HasColumnName("nap_number");
            entity.Property(e => e.NewCase)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("new_case");
            entity.Property(e => e.NextAppDate).HasColumnName("next_app_date");
            entity.Property(e => e.NodeId)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("node_id");
            entity.Property(e => e.Note)
                .HasMaxLength(250)
                .HasColumnName("note");
            entity.Property(e => e.Number)
                .HasColumnType("int(11)")
                .HasColumnName("number");
            entity.Property(e => e.OtherChronicText)
                .HasMaxLength(200)
                .HasColumnName("other_chronic_text");
            entity.Property(e => e.PeriodBeginDate).HasColumnName("period_begin_date");
            entity.Property(e => e.PreRegister)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pre_register");
            entity.Property(e => e.PtNumber)
                .HasColumnType("int(11)")
                .HasColumnName("pt_number");
            entity.Property(e => e.Pttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pttype");
            entity.Property(e => e.ReferFromPcu)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("refer_from_pcu");
            entity.Property(e => e.ReferFromPcuCause)
                .HasColumnType("int(11)")
                .HasColumnName("refer_from_pcu_cause");
            entity.Property(e => e.ReferFromPcuDate).HasColumnName("refer_from_pcu_date");
            entity.Property(e => e.ReferFromPcuHcode)
                .HasMaxLength(5)
                .HasColumnName("refer_from_pcu_hcode");
            entity.Property(e => e.ReferRegisterFromHospcode)
                .HasMaxLength(5)
                .IsFixedLength()
                .HasColumnName("refer_register_from_hospcode");
            entity.Property(e => e.Regdate).HasColumnName("regdate");
            entity.Property(e => e.RegisterHospcode)
                .HasMaxLength(5)
                .IsFixedLength()
                .HasColumnName("register_hospcode");
            entity.Property(e => e.SctDesc)
                .HasMaxLength(200)
                .HasColumnName("sct_desc");
            entity.Property(e => e.SctId)
                .HasMaxLength(18)
                .HasColumnName("sct_id");
            entity.Property(e => e.SendToPcu)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("send_to_pcu");
            entity.Property(e => e.SendToPcuDate).HasColumnName("send_to_pcu_date");
            entity.Property(e => e.SendToPcuHcode)
                .HasMaxLength(5)
                .HasColumnName("send_to_pcu_hcode");
            entity.Property(e => e.SendToPcuNote)
                .HasMaxLength(250)
                .HasColumnName("send_to_pcu_note");
            entity.Property(e => e.Sex)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("sex");
            entity.Property(e => e.SpecialCase)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("special_case");
            entity.Property(e => e.SpecialId)
                .HasMaxLength(50)
                .HasColumnName("special_id");
            entity.Property(e => e.Subtype)
                .HasColumnType("tinyint(4)")
                .HasColumnName("subtype");
            entity.Property(e => e.WithHypertension)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("with_hypertension");
            entity.Property(e => e.WithInsulin)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("with_insulin");
            entity.Property(e => e.WithPregnancy)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("with_pregnancy");
        });

        modelBuilder.Entity<Doctor>(entity =>
        {
            entity.HasKey(e => e.Code).HasName("PRIMARY");

            entity.ToTable("doctor");

            entity.HasIndex(e => e.Department, "department");

            entity.HasIndex(e => e.Active, "ix_active");

            entity.HasIndex(e => e.DoctorGuid, "ix_doctor_guid");

            entity.HasIndex(e => e.EmpId, "ix_emp_id");

            entity.HasIndex(e => e.EnableQsCall, "ix_enable_qs_call");

            entity.HasIndex(e => e.Licenseno, "ix_licenseno");

            entity.HasIndex(e => e.NameSoundex, "ix_name_soundex");

            entity.HasIndex(e => e.PositionId, "ix_position_id");

            entity.HasIndex(e => e.SearchKeyword, "ix_search_keyword");

            entity.HasIndex(e => e.Name, "name");

            entity.HasIndex(e => e.Shortname, "shortname");

            entity.Property(e => e.Code)
                .HasMaxLength(15)
                .HasColumnName("code");
            entity.Property(e => e.Active)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("active");
            entity.Property(e => e.Addrpart)
                .HasMaxLength(20)
                .HasColumnName("addrpart");
            entity.Property(e => e.AllowAppointmentOverSlot)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("allow_appointment_over_slot");
            entity.Property(e => e.AllowDfEdit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("allow_df_edit");
            entity.Property(e => e.AllowOnlineAppointment)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("allow_online_appointment");
            entity.Property(e => e.Amppart)
                .HasMaxLength(20)
                .HasColumnName("amppart");
            entity.Property(e => e.BirthDate).HasColumnName("birth_date");
            entity.Property(e => e.CanApproveIpdOrder)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("can_approve_ipd_order");
            entity.Property(e => e.ChronicStaff)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("chronic_staff");
            entity.Property(e => e.Chwpart)
                .HasMaxLength(20)
                .HasColumnName("chwpart");
            entity.Property(e => e.Cid)
                .HasMaxLength(17)
                .HasColumnName("cid");
            entity.Property(e => e.Clinic)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("clinic");
            entity.Property(e => e.CouncilCode)
                .HasMaxLength(2)
                .HasColumnName("council_code");
            entity.Property(e => e.Department)
                .HasMaxLength(250)
                .HasColumnName("department");
            entity.Property(e => e.DoctorDepartmentId)
                .HasColumnType("int(11)")
                .HasColumnName("doctor_department_id");
            entity.Property(e => e.DoctorGuid)
                .HasMaxLength(38)
                .HasColumnName("doctor_guid");
            entity.Property(e => e.DoctorTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("doctor_type_id");
            entity.Property(e => e.EmpId)
                .HasColumnType("int(11)")
                .HasColumnName("emp_id");
            entity.Property(e => e.EnableQsCall)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("enable_qs_call");
            entity.Property(e => e.Ename)
                .HasMaxLength(200)
                .HasColumnName("ename");
            entity.Property(e => e.FinishDate).HasColumnName("finish_date");
            entity.Property(e => e.Fname)
                .HasMaxLength(100)
                .HasColumnName("fname");
            entity.Property(e => e.ForceDiagnosis)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("force_diagnosis");
            entity.Property(e => e.ForceIcdDiagnosis)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("force_icd_diagnosis");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HospitalList)
                .HasMaxLength(200)
                .HasColumnName("hospital_list");
            entity.Property(e => e.Jobposition)
                .HasMaxLength(50)
                .HasColumnName("jobposition");
            entity.Property(e => e.LicenseExpireDate).HasColumnName("license_expire_date");
            entity.Property(e => e.LicenseIssueDate).HasColumnName("license_issue_date");
            entity.Property(e => e.Licenseno)
                .HasMaxLength(50)
                .HasColumnName("licenseno");
            entity.Property(e => e.LineNotifyIpdLabCritical)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("line_notify_ipd_lab_critical");
            entity.Property(e => e.LineNotifyToken)
                .HasMaxLength(200)
                .HasColumnName("line_notify_token");
            entity.Property(e => e.Lname)
                .HasMaxLength(50)
                .HasColumnName("lname");
            entity.Property(e => e.Moopart)
                .HasMaxLength(20)
                .HasColumnName("moopart");
            entity.Property(e => e.MoveFromHospcode)
                .HasMaxLength(5)
                .IsFixedLength()
                .HasColumnName("move_from_hospcode");
            entity.Property(e => e.MoveToHospcode)
                .HasMaxLength(5)
                .IsFixedLength()
                .HasColumnName("move_to_hospcode");
            entity.Property(e => e.Name)
                .HasMaxLength(150)
                .HasColumnName("name");
            entity.Property(e => e.NameSoundex)
                .HasMaxLength(150)
                .HasColumnName("name_soundex");
            entity.Property(e => e.Nationality)
                .HasMaxLength(20)
                .HasColumnName("nationality");
            entity.Property(e => e.NoRequireQsSlot)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_require_qs_slot");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(25)
                .HasColumnName("oldcode");
            entity.Property(e => e.Pname)
                .HasMaxLength(50)
                .HasColumnName("pname");
            entity.Property(e => e.PositionId)
                .HasColumnType("int(11)")
                .HasColumnName("position_id");
            entity.Property(e => e.ProviderTypeCode)
                .HasMaxLength(3)
                .HasColumnName("provider_type_code");
            entity.Property(e => e.QueuePrefix)
                .HasMaxLength(5)
                .HasColumnName("queue_prefix");
            entity.Property(e => e.RegistNo)
                .HasMaxLength(50)
                .HasColumnName("regist_no");
            entity.Property(e => e.RepOr)
                .HasColumnType("int(11)")
                .HasColumnName("rep_or");
            entity.Property(e => e.Roadpart)
                .HasMaxLength(20)
                .HasColumnName("roadpart");
            entity.Property(e => e.SearchKeyword)
                .HasMaxLength(25)
                .HasColumnName("search_keyword");
            entity.Property(e => e.Sex)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("sex");
            entity.Property(e => e.Shortname)
                .HasMaxLength(50)
                .HasColumnName("shortname");
            entity.Property(e => e.Spclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.StartDate).HasColumnName("start_date");
            entity.Property(e => e.SubSpcltyId)
                .HasColumnType("int(11)")
                .HasColumnName("sub_spclty_id");
            entity.Property(e => e.Tmbpart)
                .HasMaxLength(20)
                .HasColumnName("tmbpart");
            entity.Property(e => e.UpdateDatetime)
                .HasColumnType("datetime")
                .HasColumnName("update_datetime");
            entity.Property(e => e.UseAppSlot)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_app_slot");
            entity.Property(e => e.UseWeekSlot)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_week_slot");
            entity.Property(e => e.Zoipart)
                .HasMaxLength(20)
                .HasColumnName("zoipart");
        });

        modelBuilder.Entity<Drugitem>(entity =>
        {
            entity.HasKey(e => e.Icode).HasName("PRIMARY");

            entity.ToTable("drugitems");

            entity.HasIndex(e => e.Dosageform, "dosageform");

            entity.HasIndex(e => e.Drugaccount, "drugaccount");

            entity.HasIndex(e => e.Drugcategory, "drugcategory");

            entity.HasIndex(e => e.Drugnote, "drugnote");

            entity.HasIndex(e => e.Hintcode, "hintcode");

            entity.HasIndex(e => e.Istatus, "istatus");

            entity.HasIndex(e => e.CheckDruginteractionHistory, "ix_cdh");

            entity.HasIndex(e => e.CheckDruginteractionHistoryDay, "ix_check_druginteraction_history_day");

            entity.HasIndex(e => e.GenericName, "ix_generic_name");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => e.IcodeGuid, "ix_icode_guid");

            entity.HasIndex(e => e.Income, "ix_ix_income");

            entity.HasIndex(e => e.NoDiscount, "ix_no_discount");

            entity.HasIndex(e => e.Oldcode, "ix_oldcode");

            entity.HasIndex(e => e.ShowChildNotify, "ix_show_child_notify");

            entity.HasIndex(e => e.SksProductCategoryId, "ix_sks_product_category_id");

            entity.HasIndex(e => e.Name, "name");

            entity.HasIndex(e => e.Packqty, "packqty");

            entity.HasIndex(e => e.Strength, "strength");

            entity.HasIndex(e => e.Therapeutic, "therapeutic");

            entity.HasIndex(e => e.Therapeuticgroup, "therapeuticgroup");

            entity.HasIndex(e => e.Units, "units");

            entity.Property(e => e.Icode)
                .HasMaxLength(7)
                .HasColumnName("icode");
            entity.Property(e => e.AccRegist)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("acc_regist");
            entity.Property(e => e.AccessLevel)
                .HasColumnType("tinyint(4)")
                .HasColumnName("access_level");
            entity.Property(e => e.ActiveIngredientMg)
                .HasColumnType("double(15,3)")
                .HasColumnName("active_ingredient_mg");
            entity.Property(e => e.Addict)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("addict");
            entity.Property(e => e.AddictTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("addict_type_id");
            entity.Property(e => e.AlertLevel)
                .HasColumnType("tinyint(4)")
                .HasColumnName("alert_level");
            entity.Property(e => e.Antibiotic)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("antibiotic");
            entity.Property(e => e.AtcCode)
                .HasMaxLength(10)
                .HasColumnName("atc_code");
            entity.Property(e => e.BeginDate).HasColumnName("begin_date");
            entity.Property(e => e.Billcode)
                .HasMaxLength(10)
                .HasColumnName("billcode");
            entity.Property(e => e.Billnumber)
                .HasMaxLength(15)
                .HasColumnName("billnumber");
            entity.Property(e => e.BreastFeedingAlertText)
                .HasColumnType("text")
                .HasColumnName("breast_feeding_alert_text");
            entity.Property(e => e.CalcIdrQty)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("calc_idr_qty");
            entity.Property(e => e.CapacityName)
                .HasMaxLength(100)
                .HasColumnName("capacity_name");
            entity.Property(e => e.CapacityQty)
                .HasColumnType("double(15,3)")
                .HasColumnName("capacity_qty");
            entity.Property(e => e.ChargeServiceIpd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("charge_service_ipd");
            entity.Property(e => e.ChargeServiceOpd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("charge_service_opd");
            entity.Property(e => e.CheckDruginteractionHistory)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("check_druginteraction_history");
            entity.Property(e => e.CheckDruginteractionHistoryDay)
                .HasColumnType("int(11)")
                .HasColumnName("check_druginteraction_history_day");
            entity.Property(e => e.CheckRemedQty)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("check_remed_qty");
            entity.Property(e => e.CheckUserGroup)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("check_user_group");
            entity.Property(e => e.CheckUserName)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("check_user_name");
            entity.Property(e => e.ChildNotifyMaxAge)
                .HasColumnType("int(11)")
                .HasColumnName("child_notify_max_age");
            entity.Property(e => e.ChildNotifyMinAge)
                .HasColumnType("int(11)")
                .HasColumnName("child_notify_min_age");
            entity.Property(e => e.ChildNotifyText)
                .HasColumnType("text")
                .HasColumnName("child_notify_text");
            entity.Property(e => e.Continuous)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("continuous");
            entity.Property(e => e.ContraAdviceText)
                .HasColumnType("text")
                .HasColumnName("contra_advice_text");
            entity.Property(e => e.Criticalpriority)
                .HasColumnType("int(11)")
                .HasColumnName("criticalpriority");
            entity.Property(e => e.CsmbsClaimCat)
                .HasMaxLength(1)
                .HasColumnName("csmbs_claim_cat");
            entity.Property(e => e.DefaultQty)
                .HasColumnType("int(11)")
                .HasColumnName("default_qty");
            entity.Property(e => e.DefaultQtyIpd)
                .HasColumnType("int(11)")
                .HasColumnName("default_qty_ipd");
            entity.Property(e => e.Did)
                .HasMaxLength(30)
                .HasColumnName("did");
            entity.Property(e => e.DispenseDose)
                .HasColumnType("double(15,3)")
                .HasColumnName("dispense_dose");
            entity.Property(e => e.DispenseDoseIpd)
                .HasColumnType("double(15,3)")
                .HasColumnName("dispense_dose_ipd");
            entity.Property(e => e.Displaycolor)
                .HasColumnType("int(11)")
                .HasColumnName("displaycolor");
            entity.Property(e => e.DisplaycolorFocus)
                .HasColumnType("int(11)")
                .HasColumnName("displaycolor_focus");
            entity.Property(e => e.Dosageform)
                .HasMaxLength(100)
                .HasColumnName("dosageform");
            entity.Property(e => e.DosePerUnits)
                .HasColumnType("double(15,3)")
                .HasColumnName("dose_per_units");
            entity.Property(e => e.DoseType)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("dose_type");
            entity.Property(e => e.DrugControlTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("drug_control_type_id");
            entity.Property(e => e.Drugaccount)
                .HasMaxLength(2)
                .HasColumnName("drugaccount");
            entity.Property(e => e.Drugcategory)
                .HasMaxLength(150)
                .HasColumnName("drugcategory");
            entity.Property(e => e.DrugevalHeadId)
                .HasColumnType("int(11)")
                .HasColumnName("drugeval_head_id");
            entity.Property(e => e.DrugitemsDueTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("drugitems_due_type_id");
            entity.Property(e => e.DrugitemsEm1Id)
                .HasColumnType("int(11)")
                .HasColumnName("drugitems_em1_id");
            entity.Property(e => e.DrugitemsEm2Id)
                .HasColumnType("int(11)")
                .HasColumnName("drugitems_em2_id");
            entity.Property(e => e.DrugitemsEm3Id)
                .HasColumnType("int(11)")
                .HasColumnName("drugitems_em3_id");
            entity.Property(e => e.DrugitemsEm4Id)
                .HasColumnType("int(11)")
                .HasColumnName("drugitems_em4_id");
            entity.Property(e => e.Drugnote)
                .HasMaxLength(150)
                .HasColumnName("drugnote");
            entity.Property(e => e.Drugusage)
                .HasMaxLength(30)
                .HasColumnName("drugusage");
            entity.Property(e => e.DrugusageIpd)
                .HasMaxLength(30)
                .HasColumnName("drugusage_ipd");
            entity.Property(e => e.Empty)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("empty");
            entity.Property(e => e.EmptyText)
                .HasColumnType("text")
                .HasColumnName("empty_text");
            entity.Property(e => e.Ename)
                .HasMaxLength(150)
                .HasColumnName("ename");
            entity.Property(e => e.ExtraUnitcost)
                .HasColumnType("double(15,3)")
                .HasColumnName("extra_unitcost");
            entity.Property(e => e.FinishDate).HasColumnName("finish_date");
            entity.Property(e => e.FinishReason)
                .HasMaxLength(100)
                .HasColumnName("finish_reason");
            entity.Property(e => e.ForceRoundQty)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("force_round_qty");
            entity.Property(e => e.FpDrug)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("fp_drug");
            entity.Property(e => e.FrequencyCode)
                .HasMaxLength(10)
                .HasColumnName("frequency_code");
            entity.Property(e => e.FrequencyCodeIpd)
                .HasMaxLength(10)
                .HasColumnName("frequency_code_ipd");
            entity.Property(e => e.FwfItemId)
                .HasColumnType("int(11)")
                .HasColumnName("fwf_item_id");
            entity.Property(e => e.GenderCheck)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("gender_check");
            entity.Property(e => e.GenericName)
                .HasMaxLength(250)
                .HasColumnName("generic_name");
            entity.Property(e => e.Gfmiscode)
                .HasMaxLength(14)
                .HasColumnName("gfmiscode");
            entity.Property(e => e.GpoCode)
                .HasMaxLength(7)
                .HasColumnName("gpo_code");
            entity.Property(e => e.HabitForming)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("habit_forming");
            entity.Property(e => e.HabitFormingType)
                .HasColumnType("int(11)")
                .HasColumnName("habit_forming_type");
            entity.Property(e => e.HighCost)
                .HasMaxLength(1)
                .HasDefaultValueSql("'N'")
                .HasColumnName("high_cost");
            entity.Property(e => e.Hintcode)
                .HasMaxLength(4)
                .HasColumnName("hintcode");
            entity.Property(e => e.HintcodeEng)
                .HasMaxLength(200)
                .HasColumnName("hintcode_eng");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.IType)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("i_type");
            entity.Property(e => e.IcodeGuid)
                .HasMaxLength(38)
                .HasColumnName("icode_guid");
            entity.Property(e => e.Income)
                .HasMaxLength(2)
                .HasColumnName("income");
            entity.Property(e => e.InvMapUpdate)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("inv_map_update");
            entity.Property(e => e.Invcode)
                .HasMaxLength(10)
                .HasColumnName("invcode");
            entity.Property(e => e.IpdDefaultPay)
                .HasColumnType("int(11)")
                .HasColumnName("ipd_default_pay");
            entity.Property(e => e.IpdMedicationMachineId)
                .HasColumnType("int(11)")
                .HasColumnName("ipd_medication_machine_id");
            entity.Property(e => e.IpdPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("ipd_price");
            entity.Property(e => e.IpdPrice2)
                .HasColumnType("double(15,3)")
                .HasColumnName("ipd_price2");
            entity.Property(e => e.IpdPrice3)
                .HasColumnType("double(15,3)")
                .HasColumnName("ipd_price3");
            entity.Property(e => e.IpdRxFreqDay)
                .HasColumnType("int(11)")
                .HasColumnName("ipd_rx_freq_day");
            entity.Property(e => e.Istatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("istatus");
            entity.Property(e => e.ItemInHospital)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("item_in_hospital");
            entity.Property(e => e.LastUpdate)
                .HasColumnType("datetime")
                .HasColumnName("last_update");
            entity.Property(e => e.Lastupdatestdprice)
                .HasColumnType("datetime")
                .HasColumnName("lastupdatestdprice");
            entity.Property(e => e.LightProtect)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("light_protect");
            entity.Property(e => e.LimitDrugusage)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("limit_drugusage");
            entity.Property(e => e.LimitPttype)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("limit_pttype");
            entity.Property(e => e.Lockprice)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lockprice");
            entity.Property(e => e.Lockprint)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lockprint");
            entity.Property(e => e.LockprintIpd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lockprint_ipd");
            entity.Property(e => e.MaxQty)
                .HasColumnType("int(11)")
                .HasColumnName("max_qty");
            entity.Property(e => e.MaxQtyIpd)
                .HasColumnType("int(11)")
                .HasColumnName("max_qty_ipd");
            entity.Property(e => e.Maxlevel)
                .HasColumnType("int(11)")
                .HasColumnName("maxlevel");
            entity.Property(e => e.Maxunitperdose)
                .HasColumnType("int(11)")
                .HasColumnName("maxunitperdose");
            entity.Property(e => e.MedDoseCalcTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("med_dose_calc_type_id");
            entity.Property(e => e.MedicationMachineFlag)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("medication_machine_flag");
            entity.Property(e => e.MedicationMachineId)
                .HasColumnType("int(11)")
                .HasColumnName("medication_machine_id");
            entity.Property(e => e.MedicationMachineIpdNo)
                .HasColumnType("int(11)")
                .HasColumnName("medication_machine_ipd_no");
            entity.Property(e => e.MedicationMachineOpdNo)
                .HasColumnType("int(11)")
                .HasColumnName("medication_machine_opd_no");
            entity.Property(e => e.Minlevel)
                .HasColumnType("int(11)")
                .HasColumnName("minlevel");
            entity.Property(e => e.MultiplyChargeService)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("multiply_charge_service");
            entity.Property(e => e.MustPaid)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("must_paid");
            entity.Property(e => e.Na)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("na");
            entity.Property(e => e.Name)
                .HasMaxLength(100)
                .HasColumnName("name");
            entity.Property(e => e.NameEng)
                .HasMaxLength(100)
                .HasColumnName("name_eng");
            entity.Property(e => e.NamePr)
                .HasMaxLength(100)
                .HasColumnName("name_pr");
            entity.Property(e => e.NamePrint)
                .HasMaxLength(100)
                .HasColumnName("name_print");
            entity.Property(e => e.NeedOrderReason)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("need_order_reason");
            entity.Property(e => e.NeedPrescDuration)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("need_presc_duration");
            entity.Property(e => e.NhsoAdpCode)
                .HasMaxLength(15)
                .HasColumnName("nhso_adp_code");
            entity.Property(e => e.NhsoAdpTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("nhso_adp_type_id");
            entity.Property(e => e.NoDiscount)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_discount");
            entity.Property(e => e.NoOrderG6pd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_order_g6pd");
            entity.Property(e => e.NoOrderGender)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_order_gender");
            entity.Property(e => e.NoPopupIpdReason)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_popup_ipd_reason");
            entity.Property(e => e.NoRemed)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_remed");
            entity.Property(e => e.NoSubstock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_substock");
            entity.Property(e => e.NoshowNarcotic)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("noshow_narcotic");
            entity.Property(e => e.Note)
                .HasMaxLength(100)
                .HasColumnName("note");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(25)
                .HasColumnName("oldcode");
            entity.Property(e => e.OpiUsageCode)
                .HasMaxLength(100)
                .HasColumnName("opi_usage_code");
            entity.Property(e => e.Packqty)
                .HasColumnType("int(11)")
                .HasColumnName("packqty");
            entity.Property(e => e.Paidst)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("paidst");
            entity.Property(e => e.PharmacologyGroup1)
                .HasColumnType("int(11)")
                .HasColumnName("pharmacology_group1");
            entity.Property(e => e.PharmacologyGroup2)
                .HasColumnType("int(11)")
                .HasColumnName("pharmacology_group2");
            entity.Property(e => e.PharmacologyGroup3)
                .HasColumnType("int(11)")
                .HasColumnName("pharmacology_group3");
            entity.Property(e => e.PrecautionAdviceText)
                .HasColumnType("text")
                .HasColumnName("precaution_advice_text");
            entity.Property(e => e.PreferIpdUsageCode)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("prefer_ipd_usage_code");
            entity.Property(e => e.PreferOpdUsageCode)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("prefer_opd_usage_code");
            entity.Property(e => e.Pregnancy)
                .HasMaxLength(10)
                .HasColumnName("pregnancy");
            entity.Property(e => e.PregnancyNotifyText)
                .HasColumnType("text")
                .HasColumnName("pregnancy_notify_text");
            entity.Property(e => e.Price2)
                .HasColumnType("double(15,3)")
                .HasColumnName("price2");
            entity.Property(e => e.Price3)
                .HasColumnType("double(15,3)")
                .HasColumnName("price3");
            entity.Property(e => e.PriceLock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("price_lock");
            entity.Property(e => e.PrintIpdInjectionSticker)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_ipd_injection_sticker");
            entity.Property(e => e.PrintLabel)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_label");
            entity.Property(e => e.PrintStickerByFrequency)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_sticker_by_frequency");
            entity.Property(e => e.PrintStickerHeader)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_sticker_header");
            entity.Property(e => e.PrintStickerPq)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_sticker_pq");
            entity.Property(e => e.PrintStickerPqIpd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_sticker_pq_ipd");
            entity.Property(e => e.ProvisMedicationUnitCode)
                .HasMaxLength(10)
                .HasColumnName("provis_medication_unit_code");
            entity.Property(e => e.QrCodeUrl)
                .HasMaxLength(200)
                .HasColumnName("qr_code_url");
            entity.Property(e => e.Reorderqty)
                .HasColumnType("int(11)")
                .HasColumnName("reorderqty");
            entity.Property(e => e.SctUnitCode)
                .HasMaxLength(20)
                .HasColumnName("sct_unit_code");
            entity.Property(e => e.SendLineNotify)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("send_line_notify");
            entity.Property(e => e.ShowBreastFeedingAlert)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_breast_feeding_alert");
            entity.Property(e => e.ShowChildNotify)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_child_notify");
            entity.Property(e => e.ShowNotify)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_notify");
            entity.Property(e => e.ShowNotifyText)
                .HasColumnType("text")
                .HasColumnName("show_notify_text");
            entity.Property(e => e.ShowPregnancyAlert)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_pregnancy_alert");
            entity.Property(e => e.ShowQrcodeTrade)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_qrcode_trade");
            entity.Property(e => e.Simb2005)
                .HasMaxLength(10)
                .HasColumnName("simb_2005");
            entity.Property(e => e.SkInsertDataDate)
                .HasColumnType("timestamp")
                .HasColumnName("sk_insert_data_date");
            entity.Property(e => e.SkSpecprep)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("sk_specprep");
            entity.Property(e => e.SkUnits)
                .HasMaxLength(50)
                .HasColumnName("sk_units");
            entity.Property(e => e.SksClaimControlTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("sks_claim_control_type_id");
            entity.Property(e => e.SksClainControlTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("sks_clain_control_type_id");
            entity.Property(e => e.SksDfsCode)
                .HasMaxLength(50)
                .HasColumnName("sks_dfs_code");
            entity.Property(e => e.SksDfsText)
                .HasMaxLength(150)
                .HasColumnName("sks_dfs_text");
            entity.Property(e => e.SksDrugCode)
                .HasMaxLength(25)
                .HasColumnName("sks_drug_code");
            entity.Property(e => e.SksPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("sks_price");
            entity.Property(e => e.SksProductCategoryId)
                .HasColumnType("int(11)")
                .HasColumnName("sks_product_category_id");
            entity.Property(e => e.SksReimbPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("sks_reimb_price");
            entity.Property(e => e.SksRevDate).HasColumnName("sks_rev_date");
            entity.Property(e => e.SpecialAdviceText)
                .HasColumnType("text")
                .HasColumnName("special_advice_text");
            entity.Property(e => e.Specprep)
                .HasMaxLength(10)
                .HasColumnName("specprep");
            entity.Property(e => e.StateItemId)
                .HasColumnType("int(11)")
                .HasColumnName("state_item_id");
            entity.Property(e => e.Stdprice)
                .HasColumnType("double(22,3)")
                .HasColumnName("stdprice");
            entity.Property(e => e.Stdtaken)
                .HasMaxLength(30)
                .HasColumnName("stdtaken");
            entity.Property(e => e.StickerShortName)
                .HasMaxLength(150)
                .HasColumnName("sticker_short_name");
            entity.Property(e => e.StockType)
                .HasMaxLength(4)
                .HasColumnName("stock_type");
            entity.Property(e => e.StorageAdviceText)
                .HasColumnType("text")
                .HasColumnName("storage_advice_text");
            entity.Property(e => e.Strength)
                .HasMaxLength(50)
                .HasColumnName("strength");
            entity.Property(e => e.SubIncome)
                .HasMaxLength(3)
                .HasColumnName("sub_income");
            entity.Property(e => e.SubstituteIcode)
                .HasMaxLength(7)
                .IsFixedLength()
                .HasColumnName("substitute_icode");
            entity.Property(e => e.ThaiName)
                .HasMaxLength(200)
                .HasColumnName("thai_name");
            entity.Property(e => e.Therapeutic)
                .HasMaxLength(150)
                .HasColumnName("therapeutic");
            entity.Property(e => e.TherapeuticEng)
                .HasMaxLength(200)
                .HasColumnName("therapeutic_eng");
            entity.Property(e => e.Therapeuticgroup)
                .HasMaxLength(150)
                .HasColumnName("therapeuticgroup");
            entity.Property(e => e.TimeCode)
                .HasMaxLength(10)
                .HasColumnName("time_code");
            entity.Property(e => e.TimeCodeIpd)
                .HasMaxLength(10)
                .HasColumnName("time_code_ipd");
            entity.Property(e => e.TmtGpCode)
                .HasMaxLength(10)
                .HasColumnName("tmt_gp_code");
            entity.Property(e => e.TmtTpCode)
                .HasMaxLength(10)
                .HasColumnName("tmt_tp_code");
            entity.Property(e => e.TpuCodeList)
                .HasMaxLength(200)
                .HasColumnName("tpu_code_list");
            entity.Property(e => e.TradeName)
                .HasMaxLength(200)
                .HasColumnName("trade_name");
            entity.Property(e => e.Unitcost)
                .HasColumnType("double(15,3)")
                .HasColumnName("unitcost");
            entity.Property(e => e.Unitprice)
                .HasColumnType("double(22,3)")
                .HasColumnName("unitprice");
            entity.Property(e => e.Units)
                .HasMaxLength(50)
                .HasColumnName("units");
            entity.Property(e => e.UsageCode)
                .HasMaxLength(10)
                .HasColumnName("usage_code");
            entity.Property(e => e.UsageCodeIpd)
                .HasMaxLength(10)
                .HasColumnName("usage_code_ipd");
            entity.Property(e => e.UsageUnitCode)
                .HasMaxLength(10)
                .HasColumnName("usage_unit_code");
            entity.Property(e => e.UsageUnitCodeIpd)
                .HasMaxLength(10)
                .HasColumnName("usage_unit_code_ipd");
            entity.Property(e => e.UsePaidst)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_paidst");
            entity.Property(e => e.UseRight)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_right");
            entity.Property(e => e.UseRightAllow)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_right_allow");
            entity.Property(e => e.VatPercent)
                .HasColumnType("double(15,3)")
                .HasColumnName("vat_percent");
            entity.Property(e => e.VolumeCc)
                .HasColumnType("int(11)")
                .HasColumnName("volume_cc");
            entity.Property(e => e.WarnG6pd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("warn_g6pd");
        });

        modelBuilder.Entity<Drugusage>(entity =>
        {
            entity.HasKey(e => e.Drugusage1).HasName("PRIMARY");

            entity.ToTable("drugusage");

            entity.HasIndex(e => e.Code, "code");

            entity.HasIndex(e => e.DrugusageGuid, "ix_drugusage_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => e.Idrlink, "ix_idrlink");

            entity.HasIndex(e => new { e.Code, e.Name1, e.Name2, e.Name3 }, "ix_search1");

            entity.HasIndex(e => e.Shortlist, "ix_shortlist");

            entity.HasIndex(e => e.Status, "status");

            entity.Property(e => e.Drugusage1)
                .HasMaxLength(7)
                .HasColumnName("drugusage");
            entity.Property(e => e.Code)
                .HasMaxLength(200)
                .HasColumnName("code");
            entity.Property(e => e.CommonName)
                .HasMaxLength(250)
                .HasColumnName("common_name");
            entity.Property(e => e.DisplayOrder)
                .HasColumnType("int(11)")
                .HasColumnName("display_order");
            entity.Property(e => e.DivideAmount)
                .HasColumnType("int(11)")
                .HasColumnName("divide_amount");
            entity.Property(e => e.DoctorUse)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("doctor_use");
            entity.Property(e => e.Dosageform)
                .HasMaxLength(250)
                .HasColumnName("dosageform");
            entity.Property(e => e.DrugusageActive)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("drugusage_active");
            entity.Property(e => e.DrugusageGuid)
                .HasMaxLength(38)
                .HasColumnName("drugusage_guid");
            entity.Property(e => e.Ename1)
                .HasMaxLength(150)
                .HasColumnName("ename1");
            entity.Property(e => e.Ename2)
                .HasMaxLength(150)
                .HasColumnName("ename2");
            entity.Property(e => e.Ename3)
                .HasMaxLength(150)
                .HasColumnName("ename3");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.Idrlink)
                .HasMaxLength(100)
                .HasColumnName("idrlink");
            entity.Property(e => e.Interval1)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("interval1");
            entity.Property(e => e.Interval2)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("interval2");
            entity.Property(e => e.Interval3)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("interval3");
            entity.Property(e => e.Interval4)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("interval4");
            entity.Property(e => e.Interval5)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("interval5");
            entity.Property(e => e.Interval6)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("interval6");
            entity.Property(e => e.Iperday)
                .HasColumnType("int(11)")
                .HasColumnName("iperday");
            entity.Property(e => e.Iperdose)
                .HasColumnType("double(15,3)")
                .HasColumnName("iperdose");
            entity.Property(e => e.IptInjectionStickerCount)
                .HasColumnType("int(11)")
                .HasColumnName("ipt_injection_sticker_count");
            entity.Property(e => e.Mname1)
                .HasMaxLength(150)
                .HasColumnName("mname1");
            entity.Property(e => e.Mname2)
                .HasMaxLength(150)
                .HasColumnName("mname2");
            entity.Property(e => e.Mname3)
                .HasMaxLength(150)
                .HasColumnName("mname3");
            entity.Property(e => e.Name1)
                .HasMaxLength(150)
                .HasColumnName("name1");
            entity.Property(e => e.Name2)
                .HasMaxLength(150)
                .HasColumnName("name2");
            entity.Property(e => e.Name3)
                .HasMaxLength(150)
                .HasColumnName("name3");
            entity.Property(e => e.NoDispMachine)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_disp_machine");
            entity.Property(e => e.OpiAcpcId)
                .HasColumnType("int(11)")
                .HasColumnName("opi_acpc_id");
            entity.Property(e => e.OpiDose)
                .HasColumnType("double(15,3)")
                .HasColumnName("opi_dose");
            entity.Property(e => e.OpiFrequencyCode)
                .HasMaxLength(10)
                .HasColumnName("opi_frequency_code");
            entity.Property(e => e.OpiTimeCode)
                .HasMaxLength(10)
                .HasColumnName("opi_time_code");
            entity.Property(e => e.OpiUnitName)
                .HasMaxLength(25)
                .HasColumnName("opi_unit_name");
            entity.Property(e => e.OpiUsageCode)
                .HasMaxLength(10)
                .HasColumnName("opi_usage_code");
            entity.Property(e => e.OpiUsageUnitCode)
                .HasMaxLength(10)
                .HasColumnName("opi_usage_unit_code");
            entity.Property(e => e.Shortlist)
                .HasMaxLength(250)
                .HasColumnName("shortlist");
            entity.Property(e => e.Status)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("status");
            entity.Property(e => e.UseOpiMode2)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_opi_mode2");
        });

        modelBuilder.Entity<Hospcode>(entity =>
        {
            entity.HasKey(e => e.Hospcode1).HasName("PRIMARY");

            entity.ToTable("hospcode");

            entity.HasIndex(e => e.Amppart, "amppart");

            entity.HasIndex(e => e.Chwpart, "chwpart");

            entity.HasIndex(e => e.Hosptype, "hosptype");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => e.SssCode, "ix_sss_code");

            entity.HasIndex(e => new { e.Amppart, e.Chwpart, e.Tmbpart }, "ix_tmbpart_amppart");

            entity.HasIndex(e => e.Name, "name");

            entity.HasIndex(e => e.Tmbpart, "tmbpart");

            entity.Property(e => e.Hospcode1)
                .HasMaxLength(5)
                .HasColumnName("hospcode");
            entity.Property(e => e.Addrpart)
                .HasMaxLength(150)
                .HasColumnName("addrpart");
            entity.Property(e => e.Amppart)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("amppart");
            entity.Property(e => e.AreaCode)
                .HasMaxLength(2)
                .HasColumnName("area_code");
            entity.Property(e => e.BedCount)
                .HasColumnType("int(11)")
                .HasColumnName("bed_count");
            entity.Property(e => e.Chwpart)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("chwpart");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.Hospcode506)
                .HasMaxLength(15)
                .HasColumnName("hospcode506");
            entity.Property(e => e.HospitalFax)
                .HasMaxLength(50)
                .HasColumnName("hospital_fax");
            entity.Property(e => e.HospitalLevelId)
                .HasColumnType("int(11)")
                .HasColumnName("hospital_level_id");
            entity.Property(e => e.HospitalPhone)
                .HasMaxLength(50)
                .HasColumnName("hospital_phone");
            entity.Property(e => e.HospitalTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("hospital_type_id");
            entity.Property(e => e.Hosptype)
                .HasMaxLength(50)
                .HasColumnName("hosptype");
            entity.Property(e => e.Moopart)
                .HasMaxLength(3)
                .HasColumnName("moopart");
            entity.Property(e => e.Name)
                .HasMaxLength(100)
                .HasColumnName("name");
            entity.Property(e => e.PoCode)
                .HasMaxLength(5)
                .HasColumnName("po_code");
            entity.Property(e => e.ProvinceName)
                .HasMaxLength(60)
                .HasColumnName("province_name");
            entity.Property(e => e.SssCode)
                .HasMaxLength(12)
                .HasColumnName("sss_code");
            entity.Property(e => e.SssCodeSub)
                .HasMaxLength(12)
                .HasColumnName("sss_code_sub");
            entity.Property(e => e.Tmbpart)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("tmbpart");
            entity.Property(e => e.Zone)
                .HasMaxLength(2)
                .HasColumnName("zone");
        });

        modelBuilder.Entity<Icd101>(entity =>
        {
            entity.HasKey(e => e.Code).HasName("PRIMARY");

            entity.ToTable("icd101");

            entity.HasIndex(e => e.Code3, "code3");

            entity.HasIndex(e => e.Code4, "code4");

            entity.HasIndex(e => e.Code5, "code5");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => e.Name, "name");

            entity.Property(e => e.Code)
                .HasMaxLength(7)
                .HasColumnName("code");
            entity.Property(e => e.ActiveStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("active_status");
            entity.Property(e => e.Code3)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("code3");
            entity.Property(e => e.Code4)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("code4");
            entity.Property(e => e.Code5)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("code5");
            entity.Property(e => e.Codeset)
                .HasMaxLength(5)
                .HasColumnName("codeset");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .IsFixedLength()
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.Icd10compat)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("icd10compat");
            entity.Property(e => e.Icd10tmcompat)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("icd10tmcompat");
            entity.Property(e => e.IpdValid)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ipd_valid");
            entity.Property(e => e.Name)
                .HasMaxLength(200)
                .HasColumnName("name");
            entity.Property(e => e.Sex)
                .HasColumnType("int(11)")
                .HasColumnName("sex");
            entity.Property(e => e.Spclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.Tname)
                .HasMaxLength(150)
                .HasColumnName("tname");
        });

        modelBuilder.Entity<Ipt>(entity =>
        {
            entity.HasKey(e => e.An).HasName("PRIMARY");

            entity.ToTable("ipt");

            entity.HasIndex(e => e.Admdoctor, "ix_admdoctor");

            entity.HasIndex(e => e.AnGuid, "ix_an_guid");

            entity.HasIndex(e => new { e.An, e.Hn }, "ix_an_hn");

            entity.HasIndex(e => e.ConfirmDischarge, "ix_confirm_discharge");

            entity.HasIndex(e => new { e.DataOk, e.DataExpDate }, "ix_data_ok_date");

            entity.HasIndex(e => new { e.DataOk, e.Dchdate }, "ix_data_ok_dchdate");

            entity.HasIndex(e => e.Dchdate, "ix_dchdate");

            entity.HasIndex(e => e.Dchstts, "ix_dchstts");

            entity.HasIndex(e => new { e.Dchstts, e.Ward }, "ix_dchstts_ward");

            entity.HasIndex(e => e.Hn, "ix_hn");

            entity.HasIndex(e => new { e.Hn, e.Dchdate, e.Dchstts }, "ix_hn_dchdate_dchstts");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => e.IptType, "ix_ipt_type");

            entity.HasIndex(e => e.Pttype, "ix_pttype");

            entity.HasIndex(e => e.Regdate, "ix_regdate");

            entity.HasIndex(e => e.Regtime, "ix_regtime");

            entity.HasIndex(e => e.Spclty, "ix_spclty");

            entity.HasIndex(e => e.Vn, "ix_vn");

            entity.HasIndex(e => e.Ward, "ix_ward");

            entity.Property(e => e.An)
                .HasMaxLength(9)
                .HasColumnName("an");
            entity.Property(e => e.ActMoneyLimit)
                .HasColumnType("double(15,3)")
                .HasColumnName("act_money_limit");
            entity.Property(e => e.Adjrw)
                .HasColumnType("double(15,5)")
                .HasColumnName("adjrw");
            entity.Property(e => e.Admdoctor)
                .HasMaxLength(7)
                .HasColumnName("admdoctor");
            entity.Property(e => e.AdmitFeeGuid)
                .HasMaxLength(38)
                .HasColumnName("admit_fee_guid");
            entity.Property(e => e.AnGuid)
                .HasMaxLength(38)
                .HasColumnName("an_guid");
            entity.Property(e => e.AnLock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("an_lock");
            entity.Property(e => e.AutoChargeAmount)
                .HasColumnType("double(15,3)")
                .HasColumnName("auto_charge_amount");
            entity.Property(e => e.BodyHeight)
                .HasColumnType("int(11)")
                .HasColumnName("body_height");
            entity.Property(e => e.Bw)
                .HasColumnType("int(11)")
                .HasColumnName("bw");
            entity.Property(e => e.ChartState)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("chart_state");
            entity.Property(e => e.ConfirmDischarge)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm_discharge");
            entity.Property(e => e.CurBedno)
                .HasMaxLength(6)
                .HasColumnName("cur_bedno");
            entity.Property(e => e.CurDepCode)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("cur_dep_code");
            entity.Property(e => e.DataExpDate).HasColumnName("data_exp_date");
            entity.Property(e => e.DataOk)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("data_ok");
            entity.Property(e => e.DchDoctor)
                .HasMaxLength(7)
                .HasColumnName("dch_doctor");
            entity.Property(e => e.DchSevereTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("dch_severe_type_id");
            entity.Property(e => e.Dchdate).HasColumnName("dchdate");
            entity.Property(e => e.Dchstts)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("dchstts");
            entity.Property(e => e.Dchtime)
                .HasColumnType("time")
                .HasColumnName("dchtime");
            entity.Property(e => e.Dchtype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("dchtype");
            entity.Property(e => e.Drg)
                .HasMaxLength(5)
                .HasColumnName("drg");
            entity.Property(e => e.Dthdiagdct)
                .HasMaxLength(7)
                .HasColumnName("dthdiagdct");
            entity.Property(e => e.DwHhcListId)
                .HasColumnType("int(11)")
                .HasColumnName("dw_hhc_list_id");
            entity.Property(e => e.Ergent)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ergent");
            entity.Property(e => e.Err)
                .HasColumnType("int(2)")
                .HasColumnName("err");
            entity.Property(e => e.EstimateDischargeDate).HasColumnName("estimate_discharge_date");
            entity.Property(e => e.FinanceLock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("finance_lock");
            entity.Property(e => e.FinanceStatusFlag)
                .HasColumnType("int(11)")
                .HasColumnName("finance_status_flag");
            entity.Property(e => e.FinanceSummaryDate).HasColumnName("finance_summary_date");
            entity.Property(e => e.FinanceTransfer)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("finance_transfer");
            entity.Property(e => e.FirstWard)
                .HasMaxLength(4)
                .HasColumnName("first_ward");
            entity.Property(e => e.Followup)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("followup");
            entity.Property(e => e.Gravidity)
                .HasColumnType("tinyint(4)")
                .HasColumnName("gravidity");
            entity.Property(e => e.GrouperActlos)
                .HasColumnType("int(11)")
                .HasColumnName("grouper_actlos");
            entity.Property(e => e.GrouperAdjrwPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("grouper_adjrw_price");
            entity.Property(e => e.GrouperErr)
                .HasColumnType("int(11)")
                .HasColumnName("grouper_err");
            entity.Property(e => e.GrouperVersion)
                .HasMaxLength(15)
                .HasColumnName("grouper_version");
            entity.Property(e => e.GrouperWarn)
                .HasColumnType("int(11)")
                .HasColumnName("grouper_warn");
            entity.Property(e => e.HhcHospcode)
                .HasMaxLength(5)
                .HasColumnName("hhc_hospcode");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HomeLeaveStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("home_leave_status");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.InchargeDoctor)
                .HasMaxLength(7)
                .HasColumnName("incharge_doctor");
            entity.Property(e => e.Ipacc)
                .HasColumnType("int(11)")
                .HasColumnName("ipacc");
            entity.Property(e => e.IpdNurseEvalRangeCode)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("ipd_nurse_eval_range_code");
            entity.Property(e => e.IptAdmitTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("ipt_admit_type_id");
            entity.Property(e => e.IptCauseTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("ipt_cause_type_id");
            entity.Property(e => e.IptCauseTypeNote)
                .HasMaxLength(150)
                .HasColumnName("ipt_cause_type_note");
            entity.Property(e => e.IptSevereTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("ipt_severe_type_id");
            entity.Property(e => e.IptSpclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("ipt_spclty");
            entity.Property(e => e.IptSummaryStatusId)
                .HasColumnType("int(11)")
                .HasColumnName("ipt_summary_status_id");
            entity.Property(e => e.IptType)
                .HasColumnType("tinyint(4)")
                .HasColumnName("ipt_type");
            entity.Property(e => e.IrefType)
                .HasMaxLength(4)
                .HasColumnName("iref_type");
            entity.Property(e => e.Ivstist)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("ivstist");
            entity.Property(e => e.Ivstost)
                .HasMaxLength(4)
                .HasColumnName("ivstost");
            entity.Property(e => e.LabStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lab_status");
            entity.Property(e => e.LastCheckAutoincome)
                .HasColumnType("datetime")
                .HasColumnName("last_check_autoincome");
            entity.Property(e => e.LeaveHomeDay)
                .HasColumnType("int(11)")
                .HasColumnName("leave_home_day");
            entity.Property(e => e.LivingChildren)
                .HasColumnType("tinyint(4)")
                .HasColumnName("living_children");
            entity.Property(e => e.Lockdx)
                .HasColumnType("tinyint(4)")
                .HasColumnName("lockdx");
            entity.Property(e => e.Mdc)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("mdc");
            entity.Property(e => e.NoChargeRoom)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_charge_room");
            entity.Property(e => e.NoFood)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_food");
            entity.Property(e => e.NoVisit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_visit");
            entity.Property(e => e.OldCauseRevisit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("old_cause_revisit");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(20)
                .HasColumnName("oldcode");
            entity.Property(e => e.OpdFinanceWaitTr)
                .HasColumnType("double(15,3)")
                .HasColumnName("opd_finance_wait_tr");
            entity.Property(e => e.OperationStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("operation_status");
            entity.Property(e => e.OperationStatusId)
                .HasColumnType("int(11)")
                .HasColumnName("operation_status_id");
            entity.Property(e => e.Ot)
                .HasColumnType("int(11)")
                .HasColumnName("ot");
            entity.Property(e => e.Parity)
                .HasColumnType("tinyint(4)")
                .HasColumnName("parity");
            entity.Property(e => e.Prediag)
                .HasMaxLength(250)
                .HasColumnName("prediag");
            entity.Property(e => e.ProvisionDx)
                .HasMaxLength(200)
                .HasColumnName("provision_dx");
            entity.Property(e => e.ProvisionDxIcd)
                .HasMaxLength(9)
                .HasColumnName("provision_dx_icd");
            entity.Property(e => e.Pttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pttype");
            entity.Property(e => e.RcptDisease)
                .HasMaxLength(100)
                .HasColumnName("rcpt_disease");
            entity.Property(e => e.ReceiveChartDateTime)
                .HasColumnType("datetime")
                .HasColumnName("receive_chart_date_time");
            entity.Property(e => e.ReceiveChartNote)
                .HasMaxLength(100)
                .HasColumnName("receive_chart_note");
            entity.Property(e => e.ReceiveChartStaff)
                .HasMaxLength(25)
                .HasColumnName("receive_chart_staff");
            entity.Property(e => e.ReferOutNumber)
                .HasMaxLength(15)
                .HasColumnName("refer_out_number");
            entity.Property(e => e.Regdate).HasColumnName("regdate");
            entity.Property(e => e.Regtime)
                .HasColumnType("time")
                .HasColumnName("regtime");
            entity.Property(e => e.ReimbursePrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("reimburse_price");
            entity.Property(e => e.Result)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("result");
            entity.Property(e => e.Rfrics)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("rfrics");
            entity.Property(e => e.Rfrilct)
                .HasMaxLength(5)
                .HasColumnName("rfrilct");
            entity.Property(e => e.Rfrocs)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("rfrocs");
            entity.Property(e => e.Rfrolct)
                .HasMaxLength(5)
                .HasColumnName("rfrolct");
            entity.Property(e => e.Rw)
                .HasColumnType("double(15,5)")
                .HasColumnName("rw");
            entity.Property(e => e.RxHomeMed)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("rx_home_med");
            entity.Property(e => e.Rxdoctor)
                .HasMaxLength(7)
                .HasColumnName("rxdoctor");
            entity.Property(e => e.Spclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.Staff)
                .HasMaxLength(25)
                .HasColumnName("staff");
            entity.Property(e => e.TranStatus)
                .HasMaxLength(1)
                .HasColumnName("tran_status");
            entity.Property(e => e.UpdateDatetime)
                .HasColumnType("datetime")
                .HasColumnName("update_datetime");
            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.Ward)
                .HasMaxLength(4)
                .HasColumnName("ward");
            entity.Property(e => e.Warn)
                .HasColumnType("int(4)")
                .HasColumnName("warn");
            entity.Property(e => e.Wtlos)
                .HasColumnType("double(15,3)")
                .HasColumnName("wtlos");
            entity.Property(e => e.XrayStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("xray_status");
        });

        modelBuilder.Entity<Iptadm>(entity =>
        {
            entity.HasKey(e => e.An).HasName("PRIMARY");

            entity.ToTable("iptadm");

            entity.HasIndex(e => e.Bedno, "bedno");

            entity.HasIndex(e => e.Bedtype, "bedtype");

            entity.HasIndex(e => e.Indate, "indate");

            entity.HasIndex(e => e.Intime, "intime");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => new { e.An, e.Roomno }, "ix_roomno_an");

            entity.HasIndex(e => e.Outdate, "outdate");

            entity.HasIndex(e => e.Outtime, "outtime");

            entity.HasIndex(e => e.Roomno, "roomno");

            entity.Property(e => e.An)
                .HasMaxLength(9)
                .HasColumnName("an");
            entity.Property(e => e.ActiveMedProfileCount)
                .HasColumnType("int(11)")
                .HasColumnName("active_med_profile_count");
            entity.Property(e => e.Admday)
                .HasColumnType("int(11)")
                .HasColumnName("admday");
            entity.Property(e => e.Bedno)
                .HasMaxLength(6)
                .HasColumnName("bedno");
            entity.Property(e => e.Bedtype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("bedtype");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.Indate).HasColumnName("indate");
            entity.Property(e => e.Intime)
                .HasColumnType("time")
                .HasColumnName("intime");
            entity.Property(e => e.MoveInBedDatetime)
                .HasColumnType("datetime")
                .HasColumnName("move_in_bed_datetime");
            entity.Property(e => e.MoveInWardDatetime)
                .HasColumnType("datetime")
                .HasColumnName("move_in_ward_datetime");
            entity.Property(e => e.NoRxMachine)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_rx_machine");
            entity.Property(e => e.Outdate).HasColumnName("outdate");
            entity.Property(e => e.Outtime)
                .HasColumnType("time")
                .HasColumnName("outtime");
            entity.Property(e => e.Rate)
                .HasColumnType("double(22,3)")
                .HasColumnName("rate");
            entity.Property(e => e.Roomno)
                .HasMaxLength(4)
                .HasColumnName("roomno");
            entity.Property(e => e.RxTransactionId)
                .HasColumnType("int(11)")
                .HasColumnName("rx_transaction_id");
            entity.Property(e => e.TrxOrderChecksum)
                .HasMaxLength(150)
                .HasColumnName("trx_order_checksum");
        });

        modelBuilder.Entity<Kskdepartment>(entity =>
        {
            entity.HasKey(e => e.Depcode).HasName("PRIMARY");

            entity.ToTable("kskdepartment");

            entity.HasIndex(e => e.Department, "ix_department");

            entity.HasIndex(e => e.DepartmentActive, "ix_department_active");

            entity.HasIndex(e => e.EmpDepId, "ix_emp_dep_id");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.OnlineTime, "ix_onlinetime");

            entity.Property(e => e.Depcode)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("depcode");
            entity.Property(e => e.AutoApplyStockDepartment)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("auto_apply_stock_department");
            entity.Property(e => e.AutoConfirmMedpay)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("auto_confirm_medpay");
            entity.Property(e => e.CanPrintSticker)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("can_print_sticker");
            entity.Property(e => e.CashierVisible)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("cashier_visible");
            entity.Property(e => e.CheckMissmatchDepcode)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("check_missmatch_depcode");
            entity.Property(e => e.CheckMissmatchSpclty)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("check_missmatch_spclty");
            entity.Property(e => e.CheckSpcltyWb)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("check_spclty_wb");
            entity.Property(e => e.CurrentWaitingTime)
                .HasColumnType("int(11)")
                .HasColumnName("current_waiting_time");
            entity.Property(e => e.DefaultStockDepartmentId)
                .HasColumnType("int(11)")
                .HasColumnName("default_stock_department_id");
            entity.Property(e => e.DepCode)
                .HasMaxLength(3)
                .HasDefaultValueSql("''")
                .HasColumnName("dep_code");
            entity.Property(e => e.DepConfirmCode)
                .HasMaxLength(10)
                .HasColumnName("dep_confirm_code");
            entity.Property(e => e.Department)
                .HasMaxLength(150)
                .HasColumnName("department");
            entity.Property(e => e.DepartmentActive)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("department_active");
            entity.Property(e => e.DepcodeActive)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("depcode_active");
            entity.Property(e => e.DfDecPrice)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("df_dec_price");
            entity.Property(e => e.DfPercent)
                .HasColumnType("double(15,3)")
                .HasColumnName("df_percent");
            entity.Property(e => e.DisplayOrder)
                .HasColumnType("int(11)")
                .HasColumnName("display_order");
            entity.Property(e => e.DisplayText)
                .HasMaxLength(250)
                .HasColumnName("display_text");
            entity.Property(e => e.DoctorCode)
                .HasMaxLength(7)
                .HasColumnName("doctor_code");
            entity.Property(e => e.DoctorVisible)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("doctor_visible");
            entity.Property(e => e.EmpDepId)
                .HasColumnType("int(11)")
                .HasColumnName("emp_dep_id");
            entity.Property(e => e.ForceDoctorPeEntry)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("force_doctor_pe_entry");
            entity.Property(e => e.ForceDxEntry)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("force_dx_entry");
            entity.Property(e => e.ForceNhsoAuthenVisit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("force_nhso_authen_visit");
            entity.Property(e => e.ForceScreenSmoking)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("force_screen_smoking");
            entity.Property(e => e.ForceSelectClinicDoctor)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("force_select_clinic_doctor");
            entity.Property(e => e.ForceSelectDoctor)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("force_select_doctor");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HospitalDepartmentId)
                .HasColumnType("int(11)")
                .HasColumnName("hospital_department_id");
            entity.Property(e => e.InvNoWarnQty0)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("inv_no_warn_qty0");
            entity.Property(e => e.IpdVisible)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ipd_visible");
            entity.Property(e => e.MedicationCheck)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("medication_check");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(25)
                .HasColumnName("oldcode");
            entity.Property(e => e.OnDesk)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("on_desk");
            entity.Property(e => e.OnlineTime)
                .HasColumnType("datetime")
                .HasColumnName("online_time");
            entity.Property(e => e.OpdFileTracking)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("opd_file_tracking");
            entity.Property(e => e.OpdQsLocationId)
                .HasColumnType("int(11)")
                .HasColumnName("opd_qs_location_id");
            entity.Property(e => e.OpdQsRoomId)
                .HasColumnType("int(11)")
                .HasColumnName("opd_qs_room_id");
            entity.Property(e => e.OtServiceIcode)
                .HasMaxLength(7)
                .HasColumnName("ot_service_icode");
            entity.Property(e => e.OtServicePay)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ot_service_pay");
            entity.Property(e => e.PhoneNumber)
                .HasMaxLength(50)
                .HasColumnName("phone_number");
            entity.Property(e => e.PrintStickerCheck)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_sticker_check");
            entity.Property(e => e.ReferPoint)
                .HasMaxLength(5)
                .HasColumnName("refer_point");
            entity.Property(e => e.RegistryVisible)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("registry_visible");
            entity.Property(e => e.Roomno)
                .HasMaxLength(5)
                .HasColumnName("roomno");
            entity.Property(e => e.RxVisible)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("rx_visible");
            entity.Property(e => e.ScreenVisible)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("screen_visible");
            entity.Property(e => e.SecondLineText)
                .HasMaxLength(100)
                .HasColumnName("second_line_text");
            entity.Property(e => e.ShowAllAdviceItem)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_all_advice_item");
            entity.Property(e => e.ShowDoctorRegistQueue)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_doctor_regist_queue");
            entity.Property(e => e.ShowNhsoConfirmService)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_nhso_confirm_service");
            entity.Property(e => e.ShowNhsoFeeSchedule)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_nhso_fee_schedule");
            entity.Property(e => e.Spclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.StatusOpen)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("status_open");
            entity.Property(e => e.StockDepartmentId)
                .HasColumnType("int(11)")
                .HasColumnName("stock_department_id");
        });

        modelBuilder.Entity<LabHead>(entity =>
        {
            entity.HasKey(e => e.LabOrderNumber).HasName("PRIMARY");

            entity.ToTable("lab_head");

            entity.HasIndex(e => e.ConfirmSpecimen, "confirm_specimen");

            entity.HasIndex(e => e.Hn, "hn");

            entity.HasIndex(e => e.FollowUpVn, "ix_follow_up_vn");

            entity.HasIndex(e => new { e.Hn, e.OrderDate }, "ix_hn_order_date");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.LabOrderNumberGuid, "ix_lab_order_number_guid");

            entity.HasIndex(e => e.LabReceiveNumber, "ix_lab_receive_number");

            entity.HasIndex(e => e.LinkLabOrderNumber, "ix_link_lab_order_number");

            entity.HasIndex(e => new { e.LinkLabOrderNumber, e.LabOrderNumber, e.FormName }, "ix_link_order_form");

            entity.HasIndex(e => e.OrderDate, "ix_order_date");

            entity.HasIndex(e => e.RefOrderCode, "ix_ref_order_code");

            entity.HasIndex(e => e.ReportDate, "ix_report_date");

            entity.HasIndex(e => e.ReportTime, "ix_report_time");

            entity.HasIndex(e => e.Vn, "vn");

            entity.Property(e => e.LabOrderNumber)
                .ValueGeneratedNever()
                .HasColumnType("int(11)")
                .HasColumnName("lab_order_number");
            entity.Property(e => e.AbnormalResult)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("abnormal_result");
            entity.Property(e => e.Anonymous)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("anonymous");
            entity.Property(e => e.AnonymousRequest)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("anonymous_request");
            entity.Property(e => e.Appointment)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("appointment");
            entity.Property(e => e.AppointmentDate).HasColumnName("appointment_date");
            entity.Property(e => e.AppointmentTime)
                .HasColumnType("time")
                .HasColumnName("appointment_time");
            entity.Property(e => e.ApproveStaff)
                .HasMaxLength(25)
                .HasColumnName("approve_staff");
            entity.Property(e => e.BatchNumber)
                .HasColumnType("int(11)")
                .HasColumnName("batch_number");
            entity.Property(e => e.Clinic)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("clinic");
            entity.Property(e => e.ConfirmChargeMoney)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm_charge_money");
            entity.Property(e => e.ConfirmReport)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm_report");
            entity.Property(e => e.ConfirmSpecimen)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm_specimen");
            entity.Property(e => e.Department)
                .HasMaxLength(5)
                .HasColumnName("department");
            entity.Property(e => e.DoctorAckDatetime)
                .HasColumnType("datetime")
                .HasColumnName("doctor_ack_datetime");
            entity.Property(e => e.DoctorCertId)
                .HasColumnType("int(11)")
                .HasColumnName("doctor_cert_id");
            entity.Property(e => e.DoctorCode)
                .HasMaxLength(7)
                .HasColumnName("doctor_code");
            entity.Property(e => e.EntryDatetime)
                .HasColumnType("datetime")
                .HasColumnName("entry_datetime");
            entity.Property(e => e.FinanceLabClear)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("finance_lab_clear");
            entity.Property(e => e.FinanceLabConfirm)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("finance_lab_confirm");
            entity.Property(e => e.FinanceLock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("finance_lock");
            entity.Property(e => e.FollowUpVn)
                .HasMaxLength(12)
                .HasColumnName("follow_up_vn");
            entity.Property(e => e.FormId)
                .HasColumnType("int(11)")
                .HasColumnName("form_id");
            entity.Property(e => e.FormName)
                .HasMaxLength(200)
                .HasColumnName("form_name");
            entity.Property(e => e.HasManualItem)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_manual_item");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HospitalDepartmentId)
                .HasColumnType("int(11)")
                .HasColumnName("hospital_department_id");
            entity.Property(e => e.ImageCount)
                .HasColumnType("int(11)")
                .HasColumnName("image_count");
            entity.Property(e => e.IsOutlab)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("is_outlab");
            entity.Property(e => e.ItemCount)
                .HasColumnType("int(11)")
                .HasColumnName("item_count");
            entity.Property(e => e.LabHeadRejectTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("lab_head_reject_type_id");
            entity.Property(e => e.LabHeadRemark)
                .HasMaxLength(250)
                .HasColumnName("lab_head_remark");
            entity.Property(e => e.LabItemsGroupCode)
                .HasColumnType("int(11)")
                .HasColumnName("lab_items_group_code");
            entity.Property(e => e.LabOrderNumberGuid)
                .HasMaxLength(38)
                .HasColumnName("lab_order_number_guid");
            entity.Property(e => e.LabPerformStatusId)
                .HasColumnType("int(11)")
                .HasColumnName("lab_perform_status_id");
            entity.Property(e => e.LabPriorityId)
                .HasColumnType("int(11)")
                .HasColumnName("lab_priority_id");
            entity.Property(e => e.LabReceive)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lab_receive");
            entity.Property(e => e.LabReceiveNumber)
                .HasColumnType("int(11)")
                .HasColumnName("lab_receive_number");
            entity.Property(e => e.LabRequestTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("lab_request_type_id");
            entity.Property(e => e.LabRoomId)
                .HasColumnType("int(11)")
                .HasColumnName("lab_room_id");
            entity.Property(e => e.LinkLabOrderNumber)
                .HasColumnType("int(11)")
                .HasColumnName("link_lab_order_number");
            entity.Property(e => e.LisCalcFinance)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lis_calc_finance");
            entity.Property(e => e.LisOrderNo)
                .HasMaxLength(25)
                .HasColumnName("lis_order_no");
            entity.Property(e => e.LisTubeMachine)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lis_tube_machine");
            entity.Property(e => e.LockResult)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lock_result");
            entity.Property(e => e.MasterFormId)
                .HasColumnType("int(11)")
                .HasColumnName("master_form_id");
            entity.Property(e => e.MasterFormName)
                .HasMaxLength(200)
                .HasColumnName("master_form_name");
            entity.Property(e => e.NotifyDepcode)
                .HasMaxLength(3)
                .HasColumnName("notify_depcode");
            entity.Property(e => e.OpdQsSlotId)
                .HasColumnType("int(11)")
                .HasColumnName("opd_qs_slot_id");
            entity.Property(e => e.OrderComputerName)
                .HasMaxLength(50)
                .HasColumnName("order_computer_name");
            entity.Property(e => e.OrderDate).HasColumnName("order_date");
            entity.Property(e => e.OrderDepartment)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("order_department");
            entity.Property(e => e.OrderNote)
                .HasColumnType("text")
                .HasColumnName("order_note");
            entity.Property(e => e.OrderStaff)
                .HasMaxLength(25)
                .HasColumnName("order_staff");
            entity.Property(e => e.OrderTime)
                .HasColumnType("time")
                .HasColumnName("order_time");
            entity.Property(e => e.ReceiveComputer)
                .HasMaxLength(25)
                .HasColumnName("receive_computer");
            entity.Property(e => e.ReceiveDate).HasColumnName("receive_date");
            entity.Property(e => e.ReceiveStaff)
                .HasMaxLength(25)
                .HasColumnName("receive_staff");
            entity.Property(e => e.ReceiveTime)
                .HasColumnType("time")
                .HasColumnName("receive_time");
            entity.Property(e => e.RefOrderCode)
                .HasMaxLength(50)
                .HasColumnName("ref_order_code");
            entity.Property(e => e.ReportDate).HasColumnName("report_date");
            entity.Property(e => e.ReportDoctorCertId)
                .HasColumnType("int(11)")
                .HasColumnName("report_doctor_cert_id");
            entity.Property(e => e.ReportLock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("report_lock");
            entity.Property(e => e.ReportLockComputer)
                .HasMaxLength(50)
                .HasColumnName("report_lock_computer");
            entity.Property(e => e.ReportLockDatetime)
                .HasColumnType("datetime")
                .HasColumnName("report_lock_datetime");
            entity.Property(e => e.ReportTime)
                .HasColumnType("time")
                .HasColumnName("report_time");
            entity.Property(e => e.ReporterName)
                .HasMaxLength(100)
                .HasColumnName("reporter_name");
            entity.Property(e => e.ResultNote)
                .HasColumnType("text")
                .HasColumnName("result_note");
            entity.Property(e => e.ResultRtf).HasColumnName("result_rtf");
            entity.Property(e => e.SendMobileMsg)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("send_mobile_msg");
            entity.Property(e => e.SendToCashier)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("send_to_cashier");
            entity.Property(e => e.Spclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.SpecimenCode)
                .HasColumnType("int(11)")
                .HasColumnName("specimen_code");
            entity.Property(e => e.SubGroupList)
                .HasMaxLength(200)
                .HasColumnName("sub_group_list");
            entity.Property(e => e.UpdateDatetime)
                .HasColumnType("datetime")
                .HasColumnName("update_datetime");
            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.Ward)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("ward");
        });

        modelBuilder.Entity<LabItemsSubGroup>(entity =>
        {
            entity.HasKey(e => e.LabItemsSubGroupCode).HasName("PRIMARY");

            entity.ToTable("lab_items_sub_group");

            entity.HasIndex(e => e.ActiveStatus, "ix_active_status");

            entity.HasIndex(e => new { e.ActiveStatus, e.LabItemsSubGroupName }, "ix_active_status_name");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.LabItemsSubGroupName, "ix_lab_items_sub_group_name");

            entity.Property(e => e.LabItemsSubGroupCode)
                .ValueGeneratedNever()
                .HasColumnType("int(11)")
                .HasColumnName("lab_items_sub_group_code");
            entity.Property(e => e.ActiveStatus)
                .HasMaxLength(1)
                .HasColumnName("active_status");
            entity.Property(e => e.EclaimExport)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("eclaim_export");
            entity.Property(e => e.Ecode)
                .HasMaxLength(25)
                .HasColumnName("ecode");
            entity.Property(e => e.EstWaitMinute)
                .HasColumnType("int(11)")
                .HasColumnName("est_wait_minute");
            entity.Property(e => e.GroupIcode)
                .HasMaxLength(7)
                .HasColumnName("group_icode");
            entity.Property(e => e.GroupPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("group_price");
            entity.Property(e => e.GroupPrice2)
                .HasColumnType("double(15,3)")
                .HasColumnName("group_price2");
            entity.Property(e => e.GroupPrice3)
                .HasColumnType("double(15,3)")
                .HasColumnName("group_price3");
            entity.Property(e => e.GroupPriceIpd)
                .HasColumnType("double(15,3)")
                .HasColumnName("group_price_ipd");
            entity.Property(e => e.GroupPriceIpd2)
                .HasColumnType("double(15,3)")
                .HasColumnName("group_price_ipd2");
            entity.Property(e => e.GroupPriceIpd3)
                .HasColumnType("double(15,3)")
                .HasColumnName("group_price_ipd3");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.ItemsListText)
                .HasColumnType("text")
                .HasColumnName("items_list_text");
            entity.Property(e => e.LabItemsGroupCode)
                .HasColumnType("int(11)")
                .HasColumnName("lab_items_group_code");
            entity.Property(e => e.LabItemsSubGroupName)
                .HasMaxLength(250)
                .HasColumnName("lab_items_sub_group_name");
            entity.Property(e => e.LoincCode)
                .HasMaxLength(15)
                .HasColumnName("loinc_code");
            entity.Property(e => e.NeedNpo)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("need_npo");
            entity.Property(e => e.NpoDaysAhead)
                .HasColumnType("int(11)")
                .HasColumnName("npo_days_ahead");
            entity.Property(e => e.SpecimenCode)
                .HasColumnType("int(11)")
                .HasColumnName("specimen_code");
            entity.Property(e => e.TmltCode)
                .HasMaxLength(10)
                .HasColumnName("tmlt_code");
        });

        modelBuilder.Entity<LabOrder>(entity =>
        {
            entity.HasKey(e => new { e.LabOrderNumber, e.LabItemsCode })
                .HasName("PRIMARY")
                .HasAnnotation("MySql:IndexPrefixLength", new[] { 0, 0 });

            entity.ToTable("lab_order");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => e.LabItemsCode, "ix_lab_items_code");

            entity.HasIndex(e => e.LaborderDate, "ix_laborder_date");

            entity.HasIndex(e => e.LabOrderNumber, "lab_order_number");

            entity.Property(e => e.LabOrderNumber)
                .HasColumnType("int(11)")
                .HasColumnName("lab_order_number");
            entity.Property(e => e.LabItemsCode)
                .HasColumnType("int(11)")
                .HasColumnName("lab_items_code");
            entity.Property(e => e.AbnormalResult)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("abnormal_result");
            entity.Property(e => e.CheckKey)
                .HasMaxLength(50)
                .HasColumnName("check_key");
            entity.Property(e => e.CheckKeyA)
                .HasMaxLength(50)
                .HasColumnName("check_key_a");
            entity.Property(e => e.Confirm)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm");
            entity.Property(e => e.CriticalResult)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("critical_result");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.ItemCost)
                .HasColumnType("double(15,3)")
                .HasColumnName("item_cost");
            entity.Property(e => e.LabHistCompareTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("lab_hist_compare_type_id");
            entity.Property(e => e.LabItemsNameRef)
                .HasMaxLength(250)
                .HasColumnName("lab_items_name_ref");
            entity.Property(e => e.LabItemsNormalValueRef)
                .HasMaxLength(150)
                .HasColumnName("lab_items_normal_value_ref");
            entity.Property(e => e.LabItemsSubGroupCode)
                .HasColumnType("int(11)")
                .HasColumnName("lab_items_sub_group_code");
            entity.Property(e => e.LabOrderRemark)
                .HasMaxLength(250)
                .HasColumnName("lab_order_remark");
            entity.Property(e => e.LabOrderResult)
                .HasMaxLength(250)
                .HasColumnName("lab_order_result");
            entity.Property(e => e.LabResultStatus)
                .HasColumnType("int(11)")
                .HasColumnName("lab_result_status");
            entity.Property(e => e.LaborderDate).HasColumnName("laborder_date");
            entity.Property(e => e.OrderType)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("order_type");
            entity.Property(e => e.Printed)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("printed");
            entity.Property(e => e.PrintedDatetime)
                .HasColumnType("datetime")
                .HasColumnName("printed_datetime");
            entity.Property(e => e.SendNotify)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("send_notify");
            entity.Property(e => e.SpecimenCode)
                .HasColumnType("int(11)")
                .HasColumnName("specimen_code");
            entity.Property(e => e.Staff)
                .HasMaxLength(50)
                .HasColumnName("staff");
            entity.Property(e => e.StaffLockResult)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("staff_lock_result");
            entity.Property(e => e.UpdateDatetime)
                .HasColumnType("datetime")
                .HasColumnName("update_datetime");
        });

        modelBuilder.Entity<LabOrderImage>(entity =>
        {
            entity.HasKey(e => e.LabOrderNumber).HasName("PRIMARY");

            entity.ToTable("lab_order_image");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.Property(e => e.LabOrderNumber)
                .ValueGeneratedNever()
                .HasColumnType("int(11)")
                .HasColumnName("lab_order_number");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .IsFixedLength()
                .HasColumnName("hos_guid");
            entity.Property(e => e.Image1).HasColumnName("image1");
            entity.Property(e => e.Image1Note)
                .HasColumnType("text")
                .HasColumnName("image1_note");
            entity.Property(e => e.Image2).HasColumnName("image2");
            entity.Property(e => e.Image2Note)
                .HasColumnType("text")
                .HasColumnName("image2_note");
            entity.Property(e => e.Image3).HasColumnName("image3");
            entity.Property(e => e.Image3Note)
                .HasColumnType("text")
                .HasColumnName("image3_note");
            entity.Property(e => e.Image4).HasColumnName("image4");
            entity.Property(e => e.Image4Note)
                .HasColumnType("text")
                .HasColumnName("image4_note");
            entity.Property(e => e.Image5).HasColumnName("image5");
            entity.Property(e => e.Image5Note)
                .HasColumnType("text")
                .HasColumnName("image5_note");
        });

        modelBuilder.Entity<Nondrugitem>(entity =>
        {
            entity.HasKey(e => e.Icode).HasName("PRIMARY");

            entity.ToTable("nondrugitems");

            entity.HasIndex(e => e.Iflag, "iflag");

            entity.HasIndex(e => e.Billcode, "ix_billcode");

            entity.HasIndex(e => e.EnableSksIpd, "ix_enable_sks_ipd");

            entity.HasIndex(e => e.EnableSksOpd, "ix_enable_sks_opd");

            entity.HasIndex(e => new { e.EnableSksOpd, e.Income }, "ix_enable_sks_opd_income");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => e.IcodeGuid, "ix_icode_guid");

            entity.HasIndex(e => e.Oldcode, "ix_ix_oldcode");

            entity.HasIndex(e => e.NoDiscount, "ix_no_discount");

            entity.HasIndex(e => e.NoIpdTransfer, "ix_no_ipd_transfer");

            entity.HasIndex(e => e.Name, "name");

            entity.HasIndex(e => e.NhsoAdpCode, "nhso_adp_code");

            entity.HasIndex(e => e.Note, "note");

            entity.Property(e => e.Icode)
                .HasMaxLength(7)
                .HasColumnName("icode");
            entity.Property(e => e.Billcode)
                .HasMaxLength(10)
                .HasColumnName("billcode");
            entity.Property(e => e.Billnumber)
                .HasMaxLength(15)
                .HasColumnName("billnumber");
            entity.Property(e => e.ChargePaidst)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("charge_paidst");
            entity.Property(e => e.ChargeServiceIpd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("charge_service_ipd");
            entity.Property(e => e.ChargeServiceOpd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("charge_service_opd");
            entity.Property(e => e.CsmbsClaimCat)
                .HasMaxLength(1)
                .HasColumnName("csmbs_claim_cat");
            entity.Property(e => e.DefaultQty)
                .HasColumnType("int(11)")
                .HasColumnName("default_qty");
            entity.Property(e => e.DefaultQtyIpd)
                .HasColumnType("int(11)")
                .HasColumnName("default_qty_ipd");
            entity.Property(e => e.Detail)
                .HasMaxLength(250)
                .HasColumnName("detail");
            entity.Property(e => e.DfPercent)
                .HasColumnType("double(15,3)")
                .HasColumnName("df_percent");
            entity.Property(e => e.DfSearchCode)
                .HasMaxLength(10)
                .HasColumnName("df_search_code");
            entity.Property(e => e.DfTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("df_type_id");
            entity.Property(e => e.Displaycolor)
                .HasColumnType("int(11)")
                .HasColumnName("displaycolor");
            entity.Property(e => e.DisplaycolorFocus)
                .HasColumnType("int(11)")
                .HasColumnName("displaycolor_focus");
            entity.Property(e => e.DrgChrgitemId)
                .HasMaxLength(2)
                .HasColumnName("drg_chrgitem_id");
            entity.Property(e => e.Drugcategory)
                .HasMaxLength(150)
                .HasColumnName("drugcategory");
            entity.Property(e => e.Drugusage)
                .HasMaxLength(10)
                .HasColumnName("drugusage");
            entity.Property(e => e.EnableSksIpd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("enable_sks_ipd");
            entity.Property(e => e.EnableSksOpd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("enable_sks_opd");
            entity.Property(e => e.Ename)
                .HasMaxLength(150)
                .HasColumnName("ename");
            entity.Property(e => e.ExtIcode)
                .HasMaxLength(7)
                .HasColumnName("ext_icode");
            entity.Property(e => e.ExtraUnitcost)
                .HasColumnType("double(15,3)")
                .HasColumnName("extra_unitcost");
            entity.Property(e => e.FwfItemId)
                .HasColumnType("int(11)")
                .HasColumnName("fwf_item_id");
            entity.Property(e => e.Highcost)
                .HasMaxLength(1)
                .HasDefaultValueSql("'N'")
                .HasColumnName("highcost");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.HospitalHighcostCode)
                .HasMaxLength(15)
                .HasColumnName("hospital_highcost_code");
            entity.Property(e => e.IType)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("i_type");
            entity.Property(e => e.Icd9cm)
                .HasMaxLength(7)
                .HasColumnName("icd9cm");
            entity.Property(e => e.IcodeGuid)
                .HasMaxLength(38)
                .HasColumnName("icode_guid");
            entity.Property(e => e.Iflag)
                .HasMaxLength(50)
                .HasColumnName("iflag");
            entity.Property(e => e.Income)
                .HasMaxLength(2)
                .HasColumnName("income");
            entity.Property(e => e.InvMapUpdate)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("inv_map_update");
            entity.Property(e => e.IpdDefaultPay)
                .HasColumnType("int(11)")
                .HasColumnName("ipd_default_pay");
            entity.Property(e => e.IpdPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("ipd_price");
            entity.Property(e => e.IpdPrice2)
                .HasColumnType("double(15,3)")
                .HasColumnName("ipd_price2");
            entity.Property(e => e.IpdPrice3)
                .HasColumnType("double(15,3)")
                .HasColumnName("ipd_price3");
            entity.Property(e => e.IsAccm)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("is_accm");
            entity.Property(e => e.IsFood)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("is_food");
            entity.Property(e => e.Istatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("istatus");
            entity.Property(e => e.ItemIsDf)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("item_is_df");
            entity.Property(e => e.ItemSubtypeId)
                .HasColumnType("int(11)")
                .HasColumnName("item_subtype_id");
            entity.Property(e => e.LastUpdate)
                .HasColumnType("datetime")
                .HasColumnName("last_update");
            entity.Property(e => e.LimitPttype)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("limit_pttype");
            entity.Property(e => e.LockPttype)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lock_pttype");
            entity.Property(e => e.LockPttypeCode)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("lock_pttype_code");
            entity.Property(e => e.Lockprice)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lockprice");
            entity.Property(e => e.Lockprint)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lockprint");
            entity.Property(e => e.LockprintIpd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lockprint_ipd");
            entity.Property(e => e.MaxPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("max_price");
            entity.Property(e => e.MaxQty)
                .HasColumnType("int(11)")
                .HasColumnName("max_qty");
            entity.Property(e => e.MaxQtyIpd)
                .HasColumnType("int(11)")
                .HasColumnName("max_qty_ipd");
            entity.Property(e => e.MultiplyChargeService)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("multiply_charge_service");
            entity.Property(e => e.MustPaid)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("must_paid");
            entity.Property(e => e.Name)
                .HasMaxLength(200)
                .HasColumnName("name");
            entity.Property(e => e.NhsoAdpCode)
                .HasMaxLength(15)
                .HasColumnName("nhso_adp_code");
            entity.Property(e => e.NhsoAdpTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("nhso_adp_type_id");
            entity.Property(e => e.NhsoProjectCode)
                .HasMaxLength(15)
                .HasColumnName("nhso_project_code");
            entity.Property(e => e.NoDiscount)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_discount");
            entity.Property(e => e.NoIpdTransfer)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_ipd_transfer");
            entity.Property(e => e.NoRemed)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_remed");
            entity.Property(e => e.NoSubstock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_substock");
            entity.Property(e => e.NondrugitemsTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("nondrugitems_type_id");
            entity.Property(e => e.Note)
                .HasMaxLength(100)
                .HasColumnName("note");
            entity.Property(e => e.ObjctiveText)
                .HasColumnType("text")
                .HasColumnName("objctive_text");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(25)
                .HasColumnName("oldcode");
            entity.Property(e => e.OrganCode)
                .HasMaxLength(10)
                .HasColumnName("organ_code");
            entity.Property(e => e.Paidst)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("paidst");
            entity.Property(e => e.Price)
                .HasColumnType("double(22,3)")
                .HasColumnName("price");
            entity.Property(e => e.Price2)
                .HasColumnType("double(15,3)")
                .HasColumnName("price2");
            entity.Property(e => e.Price3)
                .HasColumnType("double(15,3)")
                .HasColumnName("price3");
            entity.Property(e => e.PriceLock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("price_lock");
            entity.Property(e => e.PrintStickerHeader)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_sticker_header");
            entity.Property(e => e.PropertyText)
                .HasColumnType("text")
                .HasColumnName("property_text");
            entity.Property(e => e.RemoveWhenAdmit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("remove_when_admit");
            entity.Property(e => e.RxUnique)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("rx_unique");
            entity.Property(e => e.ShowNotify)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_notify");
            entity.Property(e => e.ShowNotifyText)
                .HasColumnType("text")
                .HasColumnName("show_notify_text");
            entity.Property(e => e.Simb2005)
                .HasMaxLength(10)
                .HasColumnName("simb_2005");
            entity.Property(e => e.SksClaimCategoryTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("sks_claim_category_type_id");
            entity.Property(e => e.SksCoveragePrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("sks_coverage_price");
            entity.Property(e => e.SksDrugCode)
                .HasMaxLength(25)
                .HasColumnName("sks_drug_code");
            entity.Property(e => e.SksProductCategoryId)
                .HasColumnType("int(11)")
                .HasColumnName("sks_product_category_id");
            entity.Property(e => e.SksRevDate).HasColumnName("sks_rev_date");
            entity.Property(e => e.SksTmltCode)
                .HasMaxLength(15)
                .HasColumnName("sks_tmlt_code");
            entity.Property(e => e.StateItemId)
                .HasColumnType("int(11)")
                .HasColumnName("state_item_id");
            entity.Property(e => e.Stdprice)
                .HasColumnType("double(15,3)")
                .HasColumnName("stdprice");
            entity.Property(e => e.SubIncome)
                .HasMaxLength(3)
                .HasColumnName("sub_income");
            entity.Property(e => e.TmltCode)
                .HasMaxLength(10)
                .HasColumnName("tmlt_code");
            entity.Property(e => e.TpuCodeList)
                .HasMaxLength(200)
                .HasColumnName("tpu_code_list");
            entity.Property(e => e.UcefCode)
                .HasMaxLength(20)
                .HasColumnName("ucef_code");
            entity.Property(e => e.Unit)
                .HasMaxLength(100)
                .HasColumnName("unit");
            entity.Property(e => e.Unitcost)
                .HasColumnType("double(15,3)")
                .HasColumnName("unitcost");
            entity.Property(e => e.UsePaidst)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_paidst");
            entity.Property(e => e.UseRight)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_right");
            entity.Property(e => e.VatPercent)
                .HasColumnType("double(15,3)")
                .HasColumnName("vat_percent");
            entity.Property(e => e.Vorder)
                .HasColumnType("int(11)")
                .HasColumnName("vorder");
            entity.Property(e => e.Xrayfilm)
                .HasMaxLength(150)
                .HasColumnName("xrayfilm");
        });

        modelBuilder.Entity<Oapp>(entity =>
        {
            entity.HasKey(e => e.OappId).HasName("PRIMARY");

            entity.ToTable("oapp");

            entity.HasIndex(e => e.Clinic, "ix_clinic");

            entity.HasIndex(e => e.Doctor, "ix_doctor");

            entity.HasIndex(e => e.DoctorScheduleId, "ix_doctor_schedule_id");

            entity.HasIndex(e => e.Hn, "ix_hn");

            entity.HasIndex(e => e.Nextdate, "ix_nextdate");

            entity.HasIndex(e => e.OappIdGuid, "ix_oapp_id_guid");

            entity.HasIndex(e => e.OappRefId, "ix_oapp_ref_id");

            entity.HasIndex(e => e.OpdQsSlotId, "ix_qs_slot_id");

            entity.HasIndex(e => e.Spclty, "ix_spclty");

            entity.HasIndex(e => new { e.Vstdate, e.Hn }, "ix_vh");

            entity.HasIndex(e => e.VisitVn, "ix_visit_vn");

            entity.HasIndex(e => new { e.VisitVn, e.VisitNo }, "ix_visit_vn_no");

            entity.HasIndex(e => e.Vn, "ix_vn");

            entity.HasIndex(e => new { e.Vn, e.ExtRefId }, "ix_vn_ext_ref_id");

            entity.HasIndex(e => e.Vstdate, "ix_vstdate");

            entity.Property(e => e.OappId)
                .ValueGeneratedNever()
                .HasColumnType("int(11)")
                .HasColumnName("oapp_id");
            entity.Property(e => e.An)
                .HasMaxLength(9)
                .IsFixedLength()
                .HasColumnName("an");
            entity.Property(e => e.AppCause)
                .HasMaxLength(250)
                .HasColumnName("app_cause");
            entity.Property(e => e.AppLimitQty)
                .HasColumnType("int(11)")
                .HasColumnName("app_limit_qty");
            entity.Property(e => e.AppNo)
                .HasColumnType("tinyint(4)")
                .HasColumnName("app_no");
            entity.Property(e => e.AppUser)
                .HasMaxLength(150)
                .HasColumnName("app_user");
            entity.Property(e => e.Clinic)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("clinic");
            entity.Property(e => e.ClinicVisitTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("clinic_visit_type_id");
            entity.Property(e => e.CloseTime)
                .HasColumnType("time")
                .HasColumnName("close_time");
            entity.Property(e => e.ContactPoint)
                .HasMaxLength(250)
                .HasColumnName("contact_point");
            entity.Property(e => e.DateCount)
                .HasColumnType("int(11)")
                .HasColumnName("date_count");
            entity.Property(e => e.Depcode)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("depcode");
            entity.Property(e => e.Doctor)
                .HasMaxLength(15)
                .HasColumnName("doctor");
            entity.Property(e => e.DoctorScheduleId)
                .HasColumnType("int(11)")
                .HasColumnName("doctor_schedule_id");
            entity.Property(e => e.Enddate).HasColumnName("enddate");
            entity.Property(e => e.Endtime)
                .HasColumnType("time")
                .HasColumnName("endtime");
            entity.Property(e => e.EntryDate).HasColumnName("entry_date");
            entity.Property(e => e.EntryTime)
                .HasColumnType("time")
                .HasColumnName("entry_time");
            entity.Property(e => e.ExtRefId)
                .HasColumnType("int(11)")
                .HasColumnName("ext_ref_id");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.IsRefill)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("is_refill");
            entity.Property(e => e.LabListText)
                .HasColumnType("text")
                .HasColumnName("lab_list_text");
            entity.Property(e => e.LabelColor)
                .HasColumnType("int(11)")
                .HasColumnName("label_color");
            entity.Property(e => e.MophIcRefId)
                .HasColumnType("int(11)")
                .HasColumnName("moph_ic_ref_id");
            entity.Property(e => e.NextPttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("next_pttype");
            entity.Property(e => e.Nextdate).HasColumnName("nextdate");
            entity.Property(e => e.Nexttime)
                .HasColumnType("time")
                .HasColumnName("nexttime");
            entity.Property(e => e.NexttimeEnd)
                .HasColumnType("time")
                .HasColumnName("nexttime_end");
            entity.Property(e => e.Note)
                .HasMaxLength(200)
                .HasColumnName("note");
            entity.Property(e => e.Note1)
                .HasColumnType("text")
                .HasColumnName("note1");
            entity.Property(e => e.Note2)
                .HasColumnType("text")
                .HasColumnName("note2");
            entity.Property(e => e.OappIdGuid)
                .HasMaxLength(38)
                .HasColumnName("oapp_id_guid");
            entity.Property(e => e.OappRefId)
                .HasColumnType("int(11)")
                .HasColumnName("oapp_ref_id");
            entity.Property(e => e.OappStatusId)
                .HasColumnType("int(11)")
                .HasColumnName("oapp_status_id");
            entity.Property(e => e.OappWeekRangeLimitId)
                .HasColumnType("int(11)")
                .HasColumnName("oapp_week_range_limit_id");
            entity.Property(e => e.OpdQsSlotId)
                .HasColumnType("int(11)")
                .HasColumnName("opd_qs_slot_id");
            entity.Property(e => e.OpdQueueScheduleId)
                .HasColumnType("int(11)")
                .HasColumnName("opd_queue_schedule_id");
            entity.Property(e => e.OpdQueueSlotId)
                .HasColumnType("int(11)")
                .HasColumnName("opd_queue_slot_id");
            entity.Property(e => e.OpenTime)
                .HasColumnType("time")
                .HasColumnName("open_time");
            entity.Property(e => e.OperationAnesType)
                .HasMaxLength(15)
                .HasColumnName("operation_anes_type");
            entity.Property(e => e.OperationAppointment)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("operation_appointment");
            entity.Property(e => e.OperationDoctorCode)
                .HasMaxLength(25)
                .HasColumnName("operation_doctor_code");
            entity.Property(e => e.OperationNote)
                .HasMaxLength(200)
                .HasColumnName("operation_note");
            entity.Property(e => e.OperationPatientType)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("operation_patient_type");
            entity.Property(e => e.PatientVisit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("patient_visit");
            entity.Property(e => e.PerformText)
                .HasColumnType("text")
                .HasColumnName("perform_text");
            entity.Property(e => e.PersonVaccineId)
                .HasColumnType("int(11)")
                .HasColumnName("person_vaccine_id");
            entity.Property(e => e.PrintSticker)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_sticker");
            entity.Property(e => e.ProvisAptypeCode)
                .HasMaxLength(5)
                .HasColumnName("provis_aptype_code");
            entity.Property(e => e.ReferinVn)
                .HasMaxLength(12)
                .HasColumnName("referin_vn");
            entity.Property(e => e.Spclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.UpdateDatetime)
                .HasColumnType("datetime")
                .HasColumnName("update_datetime");
            entity.Property(e => e.VisitNo)
                .HasColumnType("int(11)")
                .HasColumnName("visit_no");
            entity.Property(e => e.VisitVn)
                .HasMaxLength(13)
                .HasColumnName("visit_vn");
            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.Vstdate).HasColumnName("vstdate");
            entity.Property(e => e.Ward)
                .HasMaxLength(4)
                .HasColumnName("ward");
            entity.Property(e => e.XrayListText)
                .HasColumnType("text")
                .HasColumnName("xray_list_text");
        });

        modelBuilder.Entity<Opdscreen>(entity =>
        {
            entity.HasKey(e => e.HosGuid).HasName("PRIMARY");

            entity.ToTable("opdscreen");

            entity.HasIndex(e => e.Bw, "ix_bw");

            entity.HasIndex(e => e.Hn, "ix_hn");

            entity.HasIndex(e => new { e.Height, e.Hn }, "ix_hn_height");

            entity.HasIndex(e => e.Vn, "ix_vn_unique").IsUnique();

            entity.HasIndex(e => e.Vstdate, "ix_vstdate");

            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.AdherencePercent)
                .HasColumnType("double(15,3)")
                .HasColumnName("adherence_percent");
            entity.Property(e => e.Advice1)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("advice1");
            entity.Property(e => e.Advice2)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("advice2");
            entity.Property(e => e.Advice3)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("advice3");
            entity.Property(e => e.Advice4)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("advice4");
            entity.Property(e => e.Advice5)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("advice5");
            entity.Property(e => e.Advice6)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("advice6");
            entity.Property(e => e.Advice7)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("advice7");
            entity.Property(e => e.Advice7Note)
                .HasMaxLength(250)
                .HasColumnName("advice7_note");
            entity.Property(e => e.Advice8)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("advice8");
            entity.Property(e => e.Alt)
                .HasColumnType("double(15,3)")
                .HasColumnName("alt");
            entity.Property(e => e.Ambu)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ambu");
            entity.Property(e => e.Ast)
                .HasColumnType("double(15,3)")
                .HasColumnName("ast");
            entity.Property(e => e.Begintime)
                .HasColumnType("time")
                .HasColumnName("begintime");
            entity.Property(e => e.Bicarb)
                .HasColumnType("double(15,3)")
                .HasColumnName("bicarb");
            entity.Property(e => e.Blank1)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("blank1");
            entity.Property(e => e.Bmi)
                .HasColumnType("double(15,3)")
                .HasColumnName("bmi");
            entity.Property(e => e.BpStable)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("bp_stable");
            entity.Property(e => e.Bpd)
                .HasColumnType("double(15,3)")
                .HasColumnName("bpd");
            entity.Property(e => e.Bps)
                .HasColumnType("double(15,3)")
                .HasColumnName("bps");
            entity.Property(e => e.BreastFeeding)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("breast_feeding");
            entity.Property(e => e.Bsa)
                .HasColumnType("double(15,3)")
                .HasColumnName("bsa");
            entity.Property(e => e.Bun)
                .HasColumnType("double(15,3)")
                .HasColumnName("bun");
            entity.Property(e => e.Bw)
                .HasColumnType("double(15,3)")
                .HasColumnName("bw");
            entity.Property(e => e.Cc)
                .HasColumnType("text")
                .HasColumnName("cc");
            entity.Property(e => e.CcBeginDate).HasColumnName("cc_begin_date");
            entity.Property(e => e.CcCauseOfVisit)
                .HasMaxLength(250)
                .HasColumnName("cc_cause_of_visit");
            entity.Property(e => e.CcDuration)
                .HasMaxLength(150)
                .HasColumnName("cc_duration");
            entity.Property(e => e.CcNote)
                .HasColumnType("text")
                .HasColumnName("cc_note");
            entity.Property(e => e.CcPosition)
                .HasMaxLength(250)
                .HasColumnName("cc_position");
            entity.Property(e => e.CcSign)
                .HasMaxLength(250)
                .HasColumnName("cc_sign");
            entity.Property(e => e.Checkup)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("checkup");
            entity.Property(e => e.Chloride)
                .HasColumnType("double(15,3)")
                .HasColumnName("chloride");
            entity.Property(e => e.Cholesterol)
                .HasColumnType("double(15,3)")
                .HasColumnName("cholesterol");
            entity.Property(e => e.Cradle)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("cradle");
            entity.Property(e => e.CradleLie)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("cradle_lie");
            entity.Property(e => e.Creatinine)
                .HasColumnType("double(15,3)")
                .HasColumnName("creatinine");
            entity.Property(e => e.CreatinineKidneyPercent)
                .HasColumnType("double(15,3)")
                .HasColumnName("creatinine_kidney_percent");
            entity.Property(e => e.DevelopmentScreenTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("development_screen_type_id");
            entity.Property(e => e.DrinkingTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("drinking_type_id");
            entity.Property(e => e.Egfr)
                .HasColumnType("double(15,3)")
                .HasColumnName("egfr");
            entity.Property(e => e.Endtime)
                .HasColumnType("time")
                .HasColumnName("endtime");
            entity.Property(e => e.ErNote)
                .HasColumnType("text")
                .HasColumnName("er_note");
            entity.Property(e => e.Fbs)
                .HasColumnType("double(15,3)")
                .HasColumnName("fbs");
            entity.Property(e => e.Fev1Fevc)
                .HasColumnType("double(15,3)")
                .HasColumnName("fev1_fevc");
            entity.Property(e => e.Fev1Percent)
                .HasColumnType("double(15,3)")
                .HasColumnName("fev1_percent");
            entity.Property(e => e.Fh)
                .HasColumnType("text")
                .HasColumnName("fh");
            entity.Property(e => e.FoundAllergy)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("found_allergy");
            entity.Property(e => e.FoundAmphetamine)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("found_amphetamine");
            entity.Property(e => e.G6pd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("g6pd");
            entity.Property(e => e.Glucurine)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("glucurine");
            entity.Property(e => e.Hb)
                .HasColumnType("double(15,3)")
                .HasColumnName("hb");
            entity.Property(e => e.Hba1c)
                .HasColumnType("double(15,3)")
                .HasColumnName("hba1c");
            entity.Property(e => e.Hdl)
                .HasColumnType("double(15,3)")
                .HasColumnName("hdl");
            entity.Property(e => e.HeadCricumference)
                .HasColumnType("double(15,2)")
                .HasColumnName("head_cricumference");
            entity.Property(e => e.Height)
                .HasColumnType("int(11)")
                .HasColumnName("height");
            entity.Property(e => e.Help1)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("help1");
            entity.Property(e => e.Help1Bpd)
                .HasColumnType("int(11)")
                .HasColumnName("help1_bpd");
            entity.Property(e => e.Help1Bps)
                .HasColumnType("int(11)")
                .HasColumnName("help1_bps");
            entity.Property(e => e.Help1Time)
                .HasColumnType("time")
                .HasColumnName("help1_time");
            entity.Property(e => e.Help2)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("help2");
            entity.Property(e => e.Help2Temp)
                .HasColumnType("double(15,3)")
                .HasColumnName("help2_temp");
            entity.Property(e => e.Help2Time)
                .HasColumnType("time")
                .HasColumnName("help2_time");
            entity.Property(e => e.Help3)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("help3");
            entity.Property(e => e.Help3Icode)
                .HasMaxLength(100)
                .HasColumnName("help3_icode");
            entity.Property(e => e.Help3Qty)
                .HasColumnType("tinyint(4)")
                .HasColumnName("help3_qty");
            entity.Property(e => e.Help3Time)
                .HasColumnType("time")
                .HasColumnName("help3_time");
            entity.Property(e => e.Help4)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("help4");
            entity.Property(e => e.Help4Note)
                .HasColumnType("text")
                .HasColumnName("help4_note");
            entity.Property(e => e.HisBeginDate).HasColumnName("his_begin_date");
            entity.Property(e => e.HisCause)
                .HasMaxLength(250)
                .HasColumnName("his_cause");
            entity.Property(e => e.HisCauseDecrease)
                .HasMaxLength(250)
                .HasColumnName("his_cause_decrease");
            entity.Property(e => e.HisCauseIncrease)
                .HasMaxLength(250)
                .HasColumnName("his_cause_increase");
            entity.Property(e => e.HisExpand)
                .HasMaxLength(250)
                .HasColumnName("his_expand");
            entity.Property(e => e.HisFrequency)
                .HasMaxLength(250)
                .HasColumnName("his_frequency");
            entity.Property(e => e.HisRelatedSign)
                .HasMaxLength(250)
                .HasColumnName("his_related_sign");
            entity.Property(e => e.HisSeverity)
                .HasMaxLength(250)
                .HasColumnName("his_severity");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.Hpi)
                .HasColumnType("text")
                .HasColumnName("hpi");
            entity.Property(e => e.Hr)
                .HasColumnType("double(15,3)")
                .HasColumnName("hr");
            entity.Property(e => e.Ldl)
                .HasColumnType("double(15,3)")
                .HasColumnName("ldl");
            entity.Property(e => e.LmpDate).HasColumnName("lmp_date");
            entity.Property(e => e.MacroAlbumin)
                .HasColumnType("int(11)")
                .HasColumnName("macro_albumin");
            entity.Property(e => e.MenstrualCycleTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("menstrual_cycle_type_id");
            entity.Property(e => e.MicroAlbumin)
                .HasColumnType("int(11)")
                .HasColumnName("micro_albumin");
            entity.Property(e => e.Note)
                .HasColumnType("text")
                .HasColumnName("note");
            entity.Property(e => e.O2sat)
                .HasColumnType("int(11)")
                .HasColumnName("o2sat");
            entity.Property(e => e.OpdscreenBpLocTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("opdscreen_bp_loc_type_id");
            entity.Property(e => e.OpdscreenPatientTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("opdscreen_patient_type_id");
            entity.Property(e => e.Outtime)
                .HasColumnType("time")
                .HasColumnName("outtime");
            entity.Property(e => e.PainScore)
                .HasColumnType("int(11)")
                .HasColumnName("pain_score");
            entity.Property(e => e.Pe)
                .HasColumnType("text")
                .HasColumnName("pe");
            entity.Property(e => e.PeAb)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_ab");
            entity.Property(e => e.PeAbText)
                .HasMaxLength(250)
                .HasColumnName("pe_ab_text");
            entity.Property(e => e.PeChest)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_chest");
            entity.Property(e => e.PeChestText)
                .HasMaxLength(150)
                .HasColumnName("pe_chest_text");
            entity.Property(e => e.PeExt)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_ext");
            entity.Property(e => e.PeExtText)
                .HasMaxLength(250)
                .HasColumnName("pe_ext_text");
            entity.Property(e => e.PeGa)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_ga");
            entity.Property(e => e.PeGaText)
                .HasMaxLength(250)
                .HasColumnName("pe_ga_text");
            entity.Property(e => e.PeGen)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_gen");
            entity.Property(e => e.PeGenText)
                .HasMaxLength(250)
                .HasColumnName("pe_gen_text");
            entity.Property(e => e.PeGi)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_gi");
            entity.Property(e => e.PeGiText)
                .HasMaxLength(250)
                .HasColumnName("pe_gi_text");
            entity.Property(e => e.PeGu)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_gu");
            entity.Property(e => e.PeGuText)
                .HasMaxLength(250)
                .HasColumnName("pe_gu_text");
            entity.Property(e => e.PeGy)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_gy");
            entity.Property(e => e.PeGyText)
                .HasMaxLength(250)
                .HasColumnName("pe_gy_text");
            entity.Property(e => e.PeHead)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_head");
            entity.Property(e => e.PeHeadText)
                .HasMaxLength(250)
                .HasColumnName("pe_head_text");
            entity.Property(e => e.PeHeart)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_heart");
            entity.Property(e => e.PeHeartText)
                .HasMaxLength(250)
                .HasColumnName("pe_heart_text");
            entity.Property(e => e.PeHeent)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_heent");
            entity.Property(e => e.PeHeentText)
                .HasMaxLength(250)
                .HasColumnName("pe_heent_text");
            entity.Property(e => e.PeLung)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_lung");
            entity.Property(e => e.PeLungText)
                .HasMaxLength(250)
                .HasColumnName("pe_lung_text");
            entity.Property(e => e.PeNeuro)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_neuro");
            entity.Property(e => e.PeNeuroText)
                .HasMaxLength(250)
                .HasColumnName("pe_neuro_text");
            entity.Property(e => e.PePr)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_pr");
            entity.Property(e => e.PePrText)
                .HasMaxLength(250)
                .HasColumnName("pe_pr_text");
            entity.Property(e => e.PePv)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_pv");
            entity.Property(e => e.PePvText)
                .HasMaxLength(250)
                .HasColumnName("pe_pv_text");
            entity.Property(e => e.PeRtf).HasColumnName("pe_rtf");
            entity.Property(e => e.PeRtfBlob).HasColumnName("pe_rtf_blob");
            entity.Property(e => e.PeSkin)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pe_skin");
            entity.Property(e => e.PeSkinText)
                .HasMaxLength(250)
                .HasColumnName("pe_skin_text");
            entity.Property(e => e.PeakFlow)
                .HasColumnType("int(11)")
                .HasColumnName("peak_flow");
            entity.Property(e => e.Pefr)
                .HasColumnType("int(11)")
                .HasColumnName("pefr");
            entity.Property(e => e.PefrPercent)
                .HasColumnType("double(15,3)")
                .HasColumnName("pefr_percent");
            entity.Property(e => e.Phosphate)
                .HasColumnType("double(15,3)")
                .HasColumnName("phosphate");
            entity.Property(e => e.Pmh)
                .HasColumnType("text")
                .HasColumnName("pmh");
            entity.Property(e => e.PostPainScore)
                .HasColumnType("int(11)")
                .HasColumnName("post_pain_score");
            entity.Property(e => e.Potassium)
                .HasColumnType("double(15,3)")
                .HasColumnName("potassium");
            entity.Property(e => e.PrePainScore)
                .HasColumnType("int(11)")
                .HasColumnName("pre_pain_score");
            entity.Property(e => e.Pregnancy)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pregnancy");
            entity.Property(e => e.Pth)
                .HasColumnType("double(15,3)")
                .HasColumnName("pth");
            entity.Property(e => e.Pulse)
                .HasColumnType("double(15,3)")
                .HasColumnName("pulse");
            entity.Property(e => e.PulseRegulationTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("pulse_regulation_type_id");
            entity.Property(e => e.Riskdm)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("riskdm");
            entity.Property(e => e.Ros)
                .HasColumnType("text")
                .HasColumnName("ros");
            entity.Property(e => e.Rr)
                .HasColumnType("double(15,3)")
                .HasColumnName("rr");
            entity.Property(e => e.ScreenDep)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("screen_dep");
            entity.Property(e => e.Sh)
                .HasColumnType("text")
                .HasColumnName("sh");
            entity.Property(e => e.SkinColor)
                .HasMaxLength(20)
                .HasColumnName("skin_color");
            entity.Property(e => e.SmokingTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("smoking_type_id");
            entity.Property(e => e.Sodium)
                .HasColumnType("double(15,3)")
                .HasColumnName("sodium");
            entity.Property(e => e.Spo2)
                .HasColumnType("double(15,3)")
                .HasColumnName("spo2");
            entity.Property(e => e.Symptom)
                .HasMaxLength(200)
                .HasColumnName("symptom");
            entity.Property(e => e.Tc)
                .HasColumnType("double(15,3)")
                .HasColumnName("tc");
            entity.Property(e => e.Tco2)
                .HasColumnType("double(15,3)")
                .HasColumnName("tco2");
            entity.Property(e => e.Temperature)
                .HasColumnType("double(15,3)")
                .HasColumnName("temperature");
            entity.Property(e => e.Tg)
                .HasColumnType("double(15,3)")
                .HasColumnName("tg");
            entity.Property(e => e.Ua)
                .HasColumnType("double(15,3)")
                .HasColumnName("ua");
            entity.Property(e => e.Upcr)
                .HasColumnType("double(15,3)")
                .HasColumnName("upcr");
            entity.Property(e => e.UpdateDatetime)
                .HasColumnType("datetime")
                .HasColumnName("update_datetime");
            entity.Property(e => e.UrineAlbumin)
                .HasColumnType("double(15,3)")
                .HasColumnName("urine_albumin");
            entity.Property(e => e.UrineCreatinine)
                .HasColumnType("double(15,3)")
                .HasColumnName("urine_creatinine");
            entity.Property(e => e.VaccineScreenTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("vaccine_screen_type_id");
            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.Vstdate).HasColumnName("vstdate");
            entity.Property(e => e.Vsttime)
                .HasColumnType("time")
                .HasColumnName("vsttime");
            entity.Property(e => e.Waist)
                .HasColumnType("double(15,3)")
                .HasColumnName("waist");
            entity.Property(e => e.Waiting)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("waiting");
            entity.Property(e => e.WalkId)
                .HasColumnType("int(11)")
                .HasColumnName("walk_id");
        });

        modelBuilder.Entity<Opduser>(entity =>
        {
            entity.HasKey(e => e.Loginname).HasName("PRIMARY");

            entity.ToTable("opduser");

            entity.HasIndex(e => e.Department, "department");

            entity.HasIndex(e => e.Groupname, "groupname");

            entity.HasIndex(e => e.Cid, "ix_cid");

            entity.HasIndex(e => e.Doctorcode, "ix_doctorcode");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => e.Name, "name");

            entity.HasIndex(e => e.Password, "password");

            entity.Property(e => e.Loginname)
                .HasMaxLength(250)
                .HasColumnName("loginname");
            entity.Property(e => e.Accessright)
                .HasColumnType("text")
                .HasColumnName("accessright");
            entity.Property(e => e.AccountDisable)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("account_disable");
            entity.Property(e => e.AnnounceReadCount)
                .HasColumnType("int(11)")
                .HasColumnName("announce_read_count");
            entity.Property(e => e.AutoLogoutMinute)
                .HasColumnType("int(11)")
                .HasColumnName("auto_logout_minute");
            entity.Property(e => e.CheckLabPassword)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("check_lab_password");
            entity.Property(e => e.Cid)
                .HasMaxLength(13)
                .HasColumnName("cid");
            entity.Property(e => e.Department)
                .HasMaxLength(250)
                .HasColumnName("department");
            entity.Property(e => e.Departmentposition)
                .HasMaxLength(250)
                .HasColumnName("departmentposition");
            entity.Property(e => e.Doctorcode)
                .HasMaxLength(7)
                .HasColumnName("doctorcode");
            entity.Property(e => e.DrugAccessLevel)
                .HasColumnType("tinyint(4)")
                .HasColumnName("drug_access_level");
            entity.Property(e => e.Entryposition)
                .HasMaxLength(250)
                .HasColumnName("entryposition");
            entity.Property(e => e.Groupname)
                .HasMaxLength(250)
                .HasColumnName("groupname");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.HospitalDepartmentId)
                .HasColumnType("int(11)")
                .HasColumnName("hospital_department_id");
            entity.Property(e => e.IclaimJwt)
                .HasColumnType("text")
                .HasColumnName("iclaim_jwt");
            entity.Property(e => e.LabCheckPassword)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lab_check_password");
            entity.Property(e => e.LabStaff)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lab_staff");
            entity.Property(e => e.MaxStation)
                .HasColumnType("int(11)")
                .HasColumnName("max_station");
            entity.Property(e => e.MophAccPassword)
                .HasMaxLength(250)
                .HasColumnName("moph_acc_password");
            entity.Property(e => e.MophAccUser)
                .HasMaxLength(50)
                .HasColumnName("moph_acc_user");
            entity.Property(e => e.Name)
                .HasMaxLength(250)
                .HasColumnName("name");
            entity.Property(e => e.NewPasswordDate).HasColumnName("new_password_date");
            entity.Property(e => e.NhsoPassword)
                .HasMaxLength(250)
                .HasColumnName("nhso_password");
            entity.Property(e => e.NhsoUser)
                .HasMaxLength(250)
                .HasColumnName("nhso_user");
            entity.Property(e => e.NoAnnounceDisplay)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_announce_display");
            entity.Property(e => e.NoDoctorConsultDisplay)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_doctor_consult_display");
            entity.Property(e => e.NoLabResultDisplay)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_lab_result_display");
            entity.Property(e => e.Passweb)
                .HasMaxLength(250)
                .HasColumnName("passweb");
            entity.Property(e => e.Password)
                .HasMaxLength(250)
                .HasColumnName("password");
            entity.Property(e => e.PasswordExpireDate).HasColumnName("password_expire_date");
            entity.Property(e => e.PasswordRecheckDate)
                .HasColumnType("int(11)")
                .HasColumnName("password_recheck_date");
            entity.Property(e => e.PasswordText)
                .HasMaxLength(32)
                .HasColumnName("password_text");
            entity.Property(e => e.PcuUser)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pcu_user");
            entity.Property(e => e.Picture).HasColumnName("picture");
            entity.Property(e => e.RealStaff)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("real_staff");
            entity.Property(e => e.RestrictClinicAccess)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("restrict_clinic_access");
            entity.Property(e => e.RestrictWardAccess)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("restrict_ward_access");
            entity.Property(e => e.SendMophOtp)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("send_moph_otp");
            entity.Property(e => e.ShowTip)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_tip");
            entity.Property(e => e.Startfullscreen)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("startfullscreen");
            entity.Property(e => e.Turbohosxp)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("turbohosxp");
            entity.Property(e => e.Viewallmenu)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("viewallmenu");
            entity.Property(e => e.VisibleMenu)
                .HasColumnType("text")
                .HasColumnName("visible_menu");
            entity.Property(e => e.XrayStaff)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("xray_staff");
        });

        modelBuilder.Entity<Opitemrece>(entity =>
        {
            entity.HasKey(e => e.HosGuid).HasName("PRIMARY");

            entity.ToTable("opitemrece");

            entity.HasIndex(e => e.An, "ix_an");

            entity.HasIndex(e => new { e.An, e.OrderNo }, "ix_an_order_no");

            entity.HasIndex(e => new { e.An, e.Rxdate, e.Icode }, "ix_an_rxdate_icode");

            entity.HasIndex(e => e.Doctor, "ix_doctor");

            entity.HasIndex(e => e.Drugusage, "ix_drugusage");

            entity.HasIndex(e => e.FinanceNumber, "ix_finance_number");

            entity.HasIndex(e => e.Hcode, "ix_hcode");

            entity.HasIndex(e => e.Hn, "ix_hn");

            entity.HasIndex(e => e.Icode, "ix_icode");

            entity.HasIndex(e => e.Income, "ix_income");

            entity.HasIndex(e => e.ItemType, "ix_item_type");

            entity.HasIndex(e => e.Pttype, "ix_pttype");

            entity.HasIndex(e => e.Rxdate, "ix_rxdate");

            entity.HasIndex(e => e.Staff, "ix_staff");

            entity.HasIndex(e => e.SubType, "ix_sub_type");

            entity.HasIndex(e => e.Vn, "ix_vn");

            entity.HasIndex(e => e.Vstdate, "ix_vstdate");

            entity.HasIndex(e => e.OrderNo, "order_no");

            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.An)
                .HasMaxLength(9)
                .HasColumnName("an");
            entity.Property(e => e.CommandDoctor)
                .HasMaxLength(10)
                .HasColumnName("command_doctor");
            entity.Property(e => e.Cost)
                .HasColumnType("double(15,3)")
                .HasColumnName("cost");
            entity.Property(e => e.DepCode)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("dep_code");
            entity.Property(e => e.Discount)
                .HasColumnType("double(15,3)")
                .HasColumnName("discount");
            entity.Property(e => e.Doctor)
                .HasMaxLength(10)
                .HasColumnName("doctor");
            entity.Property(e => e.DoctorLock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("doctor_lock");
            entity.Property(e => e.Drugusage)
                .HasMaxLength(7)
                .HasColumnName("drugusage");
            entity.Property(e => e.FinanceNumber)
                .HasMaxLength(7)
                .HasColumnName("finance_number");
            entity.Property(e => e.Hcode)
                .HasMaxLength(5)
                .HasColumnName("hcode");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.Icode)
                .HasMaxLength(7)
                .HasColumnName("icode");
            entity.Property(e => e.Idr)
                .HasMaxLength(30)
                .HasColumnName("idr");
            entity.Property(e => e.Income)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("income");
            entity.Property(e => e.Iperday)
                .HasColumnType("int(11)")
                .HasColumnName("iperday");
            entity.Property(e => e.Iperdose)
                .HasColumnType("double(12,3)")
                .HasColumnName("iperdose");
            entity.Property(e => e.ItemNo)
                .HasColumnType("tinyint(4)")
                .HasColumnName("item_no");
            entity.Property(e => e.ItemType)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("item_type");
            entity.Property(e => e.LastModified)
                .HasColumnType("datetime")
                .HasColumnName("last_modified");
            entity.Property(e => e.NodeId)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("node_id");
            entity.Property(e => e.OpiDoctorFinanceTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("opi_doctor_finance_type_id");
            entity.Property(e => e.OrderNo)
                .HasColumnType("int(11)")
                .HasColumnName("order_no");
            entity.Property(e => e.Paidst)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("paidst");
            entity.Property(e => e.Print)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print");
            entity.Property(e => e.Pttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pttype");
            entity.Property(e => e.Qty)
                .HasColumnType("int(11)")
                .HasColumnName("qty");
            entity.Property(e => e.Recetime)
                .HasColumnType("time")
                .HasColumnName("recetime");
            entity.Property(e => e.Remain)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("remain");
            entity.Property(e => e.Rxdate).HasColumnName("rxdate");
            entity.Property(e => e.Rxtime)
                .HasColumnType("time")
                .HasColumnName("rxtime");
            entity.Property(e => e.SpUse)
                .HasMaxLength(7)
                .HasColumnName("sp_use");
            entity.Property(e => e.Staff)
                .HasMaxLength(25)
                .HasColumnName("staff");
            entity.Property(e => e.StockDepartmentId)
                .HasColumnType("int(11)")
                .HasColumnName("stock_department_id");
            entity.Property(e => e.SubType)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("sub_type");
            entity.Property(e => e.SumPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("sum_price");
            entity.Property(e => e.Unitprice)
                .HasColumnType("double(12,3)")
                .HasColumnName("unitprice");
            entity.Property(e => e.UseRight)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_right");
            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.Vstdate).HasColumnName("vstdate");
            entity.Property(e => e.Vsttime)
                .HasColumnType("time")
                .HasColumnName("vsttime");
        });

        modelBuilder.Entity<Ovst>(entity =>
        {
            entity.HasKey(e => e.HosGuid).HasName("PRIMARY");

            entity.ToTable("ovst");

            entity.HasIndex(e => e.An, "ix_an");

            entity.HasIndex(e => e.AnonymousVn, "ix_anonymous_vn");

            entity.HasIndex(e => e.Doctor, "ix_doctor");

            entity.HasIndex(e => e.Hcode, "ix_hcode");

            entity.HasIndex(e => e.Hn, "ix_hn");

            entity.HasIndex(e => new { e.Hn, e.Vstdate }, "ix_hn_vstdate");

            entity.HasIndex(e => e.IReferNumber, "ix_i_refer_number");

            entity.HasIndex(e => e.OReferNumber, "ix_o_refer_number");

            entity.HasIndex(e => e.Pttype, "ix_pttype");

            entity.HasIndex(e => e.Spclty, "ix_spclty");

            entity.HasIndex(e => e.Staff, "ix_staff");

            entity.HasIndex(e => new { e.Vn, e.AnonymousVisit, e.AnonymousVn }, "ix_vn_anonymous_vs_vn");

            entity.HasIndex(e => new { e.Vn, e.Vstdate }, "ix_vn_date");

            entity.HasIndex(e => e.Vn, "ix_vn_unique").IsUnique();

            entity.HasIndex(e => e.Vstdate, "ix_vstdate");

            entity.HasIndex(e => new { e.Vstdate, e.CurDep }, "ix_vstdate_curdep");

            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.An)
                .HasMaxLength(9)
                .HasColumnName("an");
            entity.Property(e => e.AnonymousVisit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("anonymous_visit");
            entity.Property(e => e.AnonymousVn)
                .HasMaxLength(12)
                .HasColumnName("anonymous_vn");
            entity.Property(e => e.AtHospital)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("at_hospital");
            entity.Property(e => e.CommandDoctor)
                .HasMaxLength(6)
                .HasColumnName("command_doctor");
            entity.Property(e => e.ContractId)
                .HasColumnType("int(11)")
                .HasColumnName("contract_id");
            entity.Property(e => e.CurDep)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("cur_dep");
            entity.Property(e => e.CurDepBusy)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("cur_dep_busy");
            entity.Property(e => e.CurDepTime)
                .HasColumnType("time")
                .HasColumnName("cur_dep_time");
            entity.Property(e => e.DiagText)
                .HasMaxLength(250)
                .HasColumnName("diag_text");
            entity.Property(e => e.Doctor)
                .HasMaxLength(7)
                .HasColumnName("doctor");
            entity.Property(e => e.FinanceAlient)
                .HasMaxLength(50)
                .HasColumnName("finance_alient");
            entity.Property(e => e.FinanceLock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("finance_lock");
            entity.Property(e => e.FinanceSummaryDate).HasColumnName("finance_summary_date");
            entity.Property(e => e.HasInsurance)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_insurance");
            entity.Property(e => e.Hcode)
                .HasMaxLength(5)
                .HasColumnName("hcode");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.Hospmain)
                .HasMaxLength(5)
                .HasColumnName("hospmain");
            entity.Property(e => e.Hospsub)
                .HasMaxLength(5)
                .HasColumnName("hospsub");
            entity.Property(e => e.IReferNumber)
                .HasMaxLength(25)
                .HasColumnName("i_refer_number");
            entity.Property(e => e.LastDep)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("last_dep");
            entity.Property(e => e.MainDep)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("main_dep");
            entity.Property(e => e.MainDepQueue)
                .HasColumnType("int(11)")
                .HasColumnName("main_dep_queue");
            entity.Property(e => e.NodeId)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("node_id");
            entity.Property(e => e.OReferDep)
                .HasMaxLength(5)
                .HasColumnName("o_refer_dep");
            entity.Property(e => e.OReferNumber)
                .HasColumnType("int(11)")
                .HasColumnName("o_refer_number");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(20)
                .HasColumnName("oldcode");
            entity.Property(e => e.Oqueue)
                .HasColumnType("int(11)")
                .HasColumnName("oqueue");
            entity.Property(e => e.OvstKey)
                .HasMaxLength(40)
                .HasColumnName("ovst_key");
            entity.Property(e => e.Ovstist)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("ovstist");
            entity.Property(e => e.Ovstost)
                .HasMaxLength(4)
                .HasColumnName("ovstost");
            entity.Property(e => e.PtCapabilityTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("pt_capability_type_id");
            entity.Property(e => e.PtPriority)
                .HasColumnType("int(11)")
                .HasColumnName("pt_priority");
            entity.Property(e => e.PtSubtype)
                .HasColumnType("tinyint(4)")
                .HasColumnName("pt_subtype");
            entity.Property(e => e.Pttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pttype");
            entity.Property(e => e.Pttypeno)
                .HasMaxLength(50)
                .HasColumnName("pttypeno");
            entity.Property(e => e.RcptDisease)
                .HasMaxLength(100)
                .HasColumnName("rcpt_disease");
            entity.Property(e => e.ReferType)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("refer_type");
            entity.Property(e => e.RfriIcd10)
                .HasMaxLength(6)
                .HasColumnName("rfri_icd10");
            entity.Property(e => e.Rfrics)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("rfrics");
            entity.Property(e => e.Rfrilct)
                .HasMaxLength(5)
                .HasColumnName("rfrilct");
            entity.Property(e => e.Rfrocs)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("rfrocs");
            entity.Property(e => e.Rfrolct)
                .HasMaxLength(5)
                .HasColumnName("rfrolct");
            entity.Property(e => e.RxQueue)
                .HasColumnType("int(11)")
                .HasColumnName("rx_queue");
            entity.Property(e => e.SendPerson)
                .HasMaxLength(150)
                .HasColumnName("send_person");
            entity.Property(e => e.SignDoctor)
                .HasMaxLength(10)
                .HasColumnName("sign_doctor");
            entity.Property(e => e.Spclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.Staff)
                .HasMaxLength(25)
                .HasColumnName("staff");
            entity.Property(e => e.VisitType)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("visit_type");
            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.Vstdate).HasColumnName("vstdate");
            entity.Property(e => e.Vsttime)
                .HasColumnType("time")
                .HasColumnName("vsttime");
            entity.Property(e => e.Waiting)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("waiting");
        });

        modelBuilder.Entity<Ovstist>(entity =>
        {
            entity.HasKey(e => e.Name).HasName("PRIMARY");

            entity.ToTable("ovstist");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.Ovstist1, "ovstist_unique").IsUnique();

            entity.Property(e => e.Name)
                .HasMaxLength(50)
                .HasColumnName("name");
            entity.Property(e => e.CsmbsCode)
                .HasMaxLength(1)
                .HasColumnName("csmbs_code");
            entity.Property(e => e.ExportCode)
                .HasMaxLength(5)
                .HasColumnName("export_code");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.OpbkkCode)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("opbkk_code");
            entity.Property(e => e.Ovstist1)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("ovstist");
        });

        modelBuilder.Entity<Ovstost>(entity =>
        {
            entity.HasKey(e => e.Ovstost1).HasName("PRIMARY");

            entity.ToTable("ovstost");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.Property(e => e.Ovstost1)
                .HasMaxLength(4)
                .HasColumnName("ovstost");
            entity.Property(e => e.DeathStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("death_status");
            entity.Property(e => e.DefaultState)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("default_state");
            entity.Property(e => e.ExportCode)
                .HasMaxLength(5)
                .HasColumnName("export_code");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.ItemNo)
                .HasColumnType("int(11)")
                .HasColumnName("item_no");
            entity.Property(e => e.Name)
                .HasMaxLength(200)
                .HasColumnName("name");
            entity.Property(e => e.OpbkkCode)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("opbkk_code");
        });

        modelBuilder.Entity<Patient>(entity =>
        {
            entity.HasKey(e => e.HosGuid).HasName("PRIMARY");

            entity.ToTable("patient");

            entity.HasIndex(e => new { e.Chwpart, e.Amppart, e.Tmbpart }, "ix_address");

            entity.HasIndex(e => e.Chwpart, "ix_chwpart");

            entity.HasIndex(e => e.Cid, "ix_cid");

            entity.HasIndex(e => e.Clinic, "ix_clinic");

            entity.HasIndex(e => e.Deathday, "ix_deathday");

            entity.HasIndex(e => e.Firstday, "ix_firstday");

            entity.HasIndex(e => e.Fname, "ix_fname");

            entity.HasIndex(e => new { e.Fname, e.Lname }, "ix_fname_lname");

            entity.HasIndex(e => e.FnameSoundex, "ix_fname_soundex");

            entity.HasIndex(e => e.Hcode, "ix_hcode");

            entity.HasIndex(e => e.Hn, "ix_hn_unique").IsUnique();

            entity.HasIndex(e => e.LastUpdate, "ix_last_update");

            entity.HasIndex(e => e.LastVisit, "ix_lastvisit");

            entity.HasIndex(e => e.Lname, "ix_lname");

            entity.HasIndex(e => e.LnameSoundex, "ix_lname_soundex");

            entity.HasIndex(e => e.MembercardNo, "ix_membercard_no");

            entity.HasIndex(e => e.Oldcode, "ix_oldcode");

            entity.HasIndex(e => e.PassportNo, "ix_passport_no");

            entity.HasIndex(e => e.Pname, "ix_pname");

            entity.HasIndex(e => e.Pttype, "ix_pttype");

            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.AddrSoi)
                .HasMaxLength(100)
                .HasColumnName("addr_soi");
            entity.Property(e => e.Addressid)
                .HasMaxLength(6)
                .HasColumnName("addressid");
            entity.Property(e => e.Addrpart)
                .HasMaxLength(50)
                .HasColumnName("addrpart");
            entity.Property(e => e.Admit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("admit");
            entity.Property(e => e.AliasName)
                .HasMaxLength(100)
                .HasColumnName("alias_name");
            entity.Property(e => e.Amppart)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("amppart");
            entity.Property(e => e.AnonymousPerson)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("anonymous_person");
            entity.Property(e => e.BirthOrder)
                .HasColumnType("int(11)")
                .HasColumnName("birth_order");
            entity.Property(e => e.Birthday).HasColumnName("birthday");
            entity.Property(e => e.Birthtime)
                .HasColumnType("time")
                .HasColumnName("birthtime");
            entity.Property(e => e.BloodgroupRh)
                .HasMaxLength(5)
                .HasColumnName("bloodgroup_rh");
            entity.Property(e => e.Bloodgrp)
                .HasMaxLength(20)
                .HasColumnName("bloodgrp");
            entity.Property(e => e.CardDestroyDate).HasColumnName("card_destroy_date");
            entity.Property(e => e.Chwpart)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("chwpart");
            entity.Property(e => e.Cid)
                .HasMaxLength(13)
                .HasColumnName("cid");
            entity.Property(e => e.Citizenship)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("citizenship");
            entity.Property(e => e.Clinic)
                .HasMaxLength(100)
                .HasColumnName("clinic");
            entity.Property(e => e.Country)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("country");
            entity.Property(e => e.CoupleCid)
                .HasMaxLength(13)
                .HasColumnName("couple_cid");
            entity.Property(e => e.Death)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("death");
            entity.Property(e => e.DeathCode504)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("death_code504");
            entity.Property(e => e.DeathDiag)
                .HasMaxLength(6)
                .HasColumnName("death_diag");
            entity.Property(e => e.Deathday).HasColumnName("deathday");
            entity.Property(e => e.Destroyed)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("destroyed");
            entity.Property(e => e.Drugallergy)
                .HasMaxLength(250)
                .HasColumnName("drugallergy");
            entity.Property(e => e.EcFname)
                .HasMaxLength(50)
                .HasColumnName("ec_fname");
            entity.Property(e => e.EcLname)
                .HasMaxLength(50)
                .HasColumnName("ec_lname");
            entity.Property(e => e.EcRelationTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("ec_relation_type_id");
            entity.Property(e => e.Educate)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("educate");
            entity.Property(e => e.Email)
                .HasMaxLength(50)
                .HasColumnName("email");
            entity.Property(e => e.FamilyStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("family_status");
            entity.Property(e => e.Familyno)
                .HasColumnType("int(11)")
                .HasColumnName("familyno");
            entity.Property(e => e.FatherCid)
                .HasMaxLength(13)
                .HasColumnName("father_cid");
            entity.Property(e => e.FatherHn)
                .HasMaxLength(9)
                .HasColumnName("father_hn");
            entity.Property(e => e.Fatherlname)
                .HasMaxLength(30)
                .HasColumnName("fatherlname");
            entity.Property(e => e.Fathername)
                .HasMaxLength(50)
                .HasColumnName("fathername");
            entity.Property(e => e.Firstday).HasColumnName("firstday");
            entity.Property(e => e.Fname)
                .HasMaxLength(30)
                .HasColumnName("fname");
            entity.Property(e => e.FnameSoundex)
                .HasMaxLength(50)
                .HasColumnName("fname_soundex");
            entity.Property(e => e.G6pd)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("g6pd");
            entity.Property(e => e.GovChronicId)
                .HasMaxLength(10)
                .HasColumnName("gov_chronic_id");
            entity.Property(e => e.Hcode)
                .HasMaxLength(5)
                .HasColumnName("hcode");
            entity.Property(e => e.Height)
                .HasColumnType("int(11)")
                .HasColumnName("height");
            entity.Property(e => e.Hid)
                .HasColumnType("int(11)")
                .HasColumnName("hid");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HnInt)
                .HasColumnType("int(11)")
                .HasColumnName("hn_int");
            entity.Property(e => e.Hometel)
                .HasMaxLength(20)
                .HasColumnName("hometel");
            entity.Property(e => e.HospitalDepartmentId)
                .HasColumnType("int(11)")
                .HasColumnName("hospital_department_id");
            entity.Property(e => e.InCups)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("in_cups");
            entity.Property(e => e.Informaddr)
                .HasMaxLength(200)
                .HasColumnName("informaddr");
            entity.Property(e => e.Informname)
                .HasMaxLength(50)
                .HasColumnName("informname");
            entity.Property(e => e.Informrelation)
                .HasMaxLength(50)
                .HasColumnName("informrelation");
            entity.Property(e => e.Informtel)
                .HasMaxLength(20)
                .HasColumnName("informtel");
            entity.Property(e => e.Inregion)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("inregion");
            entity.Property(e => e.IsCardDestroy)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("is_card_destroy");
            entity.Property(e => e.LaborType)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("labor_type");
            entity.Property(e => e.Lang)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("lang");
            entity.Property(e => e.LastUpdate)
                .HasColumnType("datetime")
                .HasColumnName("last_update");
            entity.Property(e => e.LastVisit).HasColumnName("last_visit");
            entity.Property(e => e.LegalAction)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("legal_action");
            entity.Property(e => e.Lname)
                .HasMaxLength(30)
                .HasColumnName("lname");
            entity.Property(e => e.LnameSoundex)
                .HasMaxLength(50)
                .HasColumnName("lname_soundex");
            entity.Property(e => e.Marrystatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("marrystatus");
            entity.Property(e => e.Mathername)
                .HasMaxLength(50)
                .HasColumnName("mathername");
            entity.Property(e => e.MembercardNo)
                .HasMaxLength(15)
                .HasColumnName("membercard_no");
            entity.Property(e => e.Midname)
                .HasMaxLength(25)
                .HasColumnName("midname");
            entity.Property(e => e.MobilePhoneNumber)
                .HasMaxLength(20)
                .HasColumnName("mobile_phone_number");
            entity.Property(e => e.Moopart)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("moopart");
            entity.Property(e => e.MotherCid)
                .HasMaxLength(13)
                .HasColumnName("mother_cid");
            entity.Property(e => e.MotherHn)
                .HasMaxLength(9)
                .HasColumnName("mother_hn");
            entity.Property(e => e.Motherlname)
                .HasMaxLength(30)
                .HasColumnName("motherlname");
            entity.Property(e => e.Nationality)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("nationality");
            entity.Property(e => e.NodeId)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("node_id");
            entity.Property(e => e.NumberOfRelatives)
                .HasColumnType("int(11)")
                .HasColumnName("number_of_relatives");
            entity.Property(e => e.Occupation)
                .HasMaxLength(4)
                .HasColumnName("occupation");
            entity.Property(e => e.OldAddr)
                .HasMaxLength(250)
                .HasColumnName("old_addr");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(50)
                .HasColumnName("oldcode");
            entity.Property(e => e.Opdlocation)
                .HasMaxLength(50)
                .HasColumnName("opdlocation");
            entity.Property(e => e.PassportNo)
                .HasMaxLength(25)
                .HasColumnName("passport_no");
            entity.Property(e => e.PatientColorId)
                .HasColumnType("int(11)")
                .HasColumnName("patient_color_id");
            entity.Property(e => e.PatientTypeId)
                .HasColumnType("tinyint(4)")
                .HasColumnName("patient_type_id");
            entity.Property(e => e.PersonLaborTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("person_labor_type_id");
            entity.Property(e => e.PersonType)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("person_type");
            entity.Property(e => e.Pname)
                .HasMaxLength(25)
                .HasColumnName("pname");
            entity.Property(e => e.PoCode)
                .HasMaxLength(5)
                .HasColumnName("po_code");
            entity.Property(e => e.PrivateDoctorName)
                .HasMaxLength(75)
                .HasColumnName("private_doctor_name");
            entity.Property(e => e.Pttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pttype");
            entity.Property(e => e.RegTime)
                .HasColumnType("time")
                .HasColumnName("reg_time");
            entity.Property(e => e.Religion)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("religion");
            entity.Property(e => e.Road)
                .HasMaxLength(50)
                .HasColumnName("road");
            entity.Property(e => e.Sex)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("sex");
            entity.Property(e => e.Spslname)
                .HasMaxLength(30)
                .HasColumnName("spslname");
            entity.Property(e => e.Spsname)
                .HasMaxLength(50)
                .HasColumnName("spsname");
            entity.Property(e => e.Tmbpart)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("tmbpart");
            entity.Property(e => e.TranStatus)
                .HasMaxLength(1)
                .HasColumnName("tran_status");
            entity.Property(e => e.Truebirthday)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("truebirthday");
            entity.Property(e => e.TypeArea)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("type_area");
            entity.Property(e => e.Vid)
                .HasMaxLength(8)
                .HasColumnName("vid");
            entity.Property(e => e.WorkAddr)
                .HasMaxLength(230)
                .HasColumnName("work_addr");
            entity.Property(e => e.Workaddr)
                .HasMaxLength(50)
                .HasColumnName("workaddr");
            entity.Property(e => e.Worktel)
                .HasMaxLength(20)
                .HasColumnName("worktel");
        });

        modelBuilder.Entity<PatientImage>(entity =>
        {
            entity.HasKey(e => new { e.Hn, e.ImageName })
                .HasName("PRIMARY")
                .HasAnnotation("MySql:IndexPrefixLength", new[] { 0, 0 });

            entity.ToTable("patient_image");

            entity.HasIndex(e => e.Hn, "hn");

            entity.HasIndex(e => e.ImageName, "image_name");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasDefaultValueSql("''")
                .HasColumnName("hn");
            entity.Property(e => e.ImageName)
                .HasMaxLength(150)
                .HasDefaultValueSql("''")
                .HasColumnName("image_name");
            entity.Property(e => e.CaptureDate)
                .HasColumnType("datetime")
                .HasColumnName("capture_date");
            entity.Property(e => e.Height)
                .HasColumnType("int(11)")
                .HasColumnName("height");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.Image).HasColumnName("image");
            entity.Property(e => e.Width)
                .HasColumnType("int(11)")
                .HasColumnName("width");
        });

        modelBuilder.Entity<Ptcardno>(entity =>
        {
            entity.HasKey(e => new { e.Hn, e.Cardtype })
                .HasName("PRIMARY")
                .HasAnnotation("MySql:IndexPrefixLength", new[] { 0, 0 });

            entity.ToTable("ptcardno");

            entity.HasIndex(e => e.Cardno, "ix_cardno");

            entity.HasIndex(e => e.Cardtype, "ix_cardtype");

            entity.HasIndex(e => e.Hn, "ix_hn");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.Cardtype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("cardtype");
            entity.Property(e => e.Cardno)
                .HasMaxLength(20)
                .HasColumnName("cardno");
            entity.Property(e => e.Expiredate).HasColumnName("expiredate");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
        });

        modelBuilder.Entity<Ptnote>(entity =>
        {
            entity.HasKey(e => e.PtnoteId).HasName("PRIMARY");

            entity.ToTable("ptnote");

            entity.HasIndex(e => e.Hn, "ix_hn");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.NoteStaff, "ix_note_staff");

            entity.Property(e => e.PtnoteId)
                .ValueGeneratedNever()
                .HasColumnType("int(11)")
                .HasColumnName("ptnote_id");
            entity.Property(e => e.CheckGroup)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("check_group");
            entity.Property(e => e.EncryptNote)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("encrypt_note");
            entity.Property(e => e.ExpireDate).HasColumnName("expire_date");
            entity.Property(e => e.Groupname)
                .HasMaxLength(250)
                .HasColumnName("groupname");
            entity.Property(e => e.HasExpired)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("has_expired");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.NoteDatetime)
                .HasColumnType("datetime")
                .HasColumnName("note_datetime");
            entity.Property(e => e.NoteStaff)
                .HasMaxLength(25)
                .HasColumnName("note_staff");
            entity.Property(e => e.Noteflag)
                .HasMaxLength(250)
                .HasColumnName("noteflag");
            entity.Property(e => e.PlainText)
                .HasColumnType("text")
                .HasColumnName("plain_text");
            entity.Property(e => e.PrscNoteText)
                .HasMaxLength(250)
                .HasColumnName("prsc_note_text");
            entity.Property(e => e.Ptnote1)
                .HasColumnType("text")
                .HasColumnName("ptnote");
            entity.Property(e => e.PtnoteHtml)
                .HasColumnType("text")
                .HasColumnName("ptnote_html");
            entity.Property(e => e.PublicNote)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("public_note");
            entity.Property(e => e.ShowAllDep)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("show_all_dep");
            entity.Property(e => e.UsernameList)
                .HasColumnType("text")
                .HasColumnName("username_list");
            entity.Property(e => e.Vstdate).HasColumnName("vstdate");
            entity.Property(e => e.Vsttime)
                .HasColumnType("time")
                .HasColumnName("vsttime");
        });

        modelBuilder.Entity<Pttype>(entity =>
        {
            entity.HasKey(e => e.Pttype1).HasName("PRIMARY");

            entity.ToTable("pttype");

            entity.HasIndex(e => e.Isuse, "ix_isuse");

            entity.HasIndex(e => e.HipdataCode, "ix_ix_hipdata_code");

            entity.HasIndex(e => e.NhsoCode, "ix_nhso_code");

            entity.HasIndex(e => e.NhsoSubinscl, "ix_nhso_subinscl");

            entity.HasIndex(e => e.Pcode, "ix_pcode");

            entity.HasIndex(e => e.PttypeGuid, "ix_pttype_guid");

            entity.HasIndex(e => e.PttypeSppId, "ix_pttype_spp_id");

            entity.HasIndex(e => e.Name, "name");

            entity.Property(e => e.Pttype1)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pttype");
            entity.Property(e => e.AdmitFeeCode)
                .HasMaxLength(7)
                .HasColumnName("admit_fee_code");
            entity.Property(e => e.AllowFinanceEdit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("allow_finance_edit");
            entity.Property(e => e.BillSss)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("bill_sss");
            entity.Property(e => e.BillType)
                .HasColumnType("int(11)")
                .HasColumnName("bill_type");
            entity.Property(e => e.CalcDiscount)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("calc_discount");
            entity.Property(e => e.ChargeDfPerday)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("charge_df_perday");
            entity.Property(e => e.CheckNhsoAuth)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("check_nhso_auth");
            entity.Property(e => e.Contract)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("contract");
            entity.Property(e => e.DebtDueDay)
                .HasColumnType("int(11)")
                .HasColumnName("debt_due_day");
            entity.Property(e => e.DebtFinanceLimit)
                .HasColumnType("double(15,3)")
                .HasColumnName("debt_finance_limit");
            entity.Property(e => e.DebtFinancePttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("debt_finance_pttype");
            entity.Property(e => e.Debtor)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("debtor");
            entity.Property(e => e.Discount)
                .HasColumnType("int(11)")
                .HasColumnName("discount");
            entity.Property(e => e.DoctorFee)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("doctor_fee");
            entity.Property(e => e.Editmask)
                .HasMaxLength(100)
                .HasColumnName("editmask");
            entity.Property(e => e.EmpFinancial)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("emp_financial");
            entity.Property(e => e.EmpPrivilege)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("emp_privilege");
            entity.Property(e => e.ExportEclaim)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("export_eclaim");
            entity.Property(e => e.FeeCode)
                .HasMaxLength(7)
                .HasColumnName("fee_code");
            entity.Property(e => e.FeeCode2)
                .HasMaxLength(7)
                .HasColumnName("fee_code2");
            entity.Property(e => e.FeeCode2Paidst)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("fee_code2_paidst");
            entity.Property(e => e.FeeCodePaidst)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("fee_code_paidst");
            entity.Property(e => e.FinanceRoundMoney)
                .HasMaxLength(1)
                .HasColumnName("finance_round_money");
            entity.Property(e => e.GrouperRelease)
                .HasMaxLength(5)
                .HasColumnName("grouper_release");
            entity.Property(e => e.GrouperVersion)
                .HasColumnType("int(11)")
                .HasColumnName("grouper_version");
            entity.Property(e => e.HipdataCode)
                .HasMaxLength(6)
                .HasColumnName("hipdata_code");
            entity.Property(e => e.HipdataPttype)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("hipdata_pttype");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HospmainList)
                .HasMaxLength(100)
                .HasColumnName("hospmain_list");
            entity.Property(e => e.InRegion)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("in_region");
            entity.Property(e => e.IncRoundMoney)
                .HasMaxLength(1)
                .HasColumnName("inc_round_money");
            entity.Property(e => e.IpdBedcharge24)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ipd_bedcharge_24");
            entity.Property(e => e.IpdHourCut)
                .HasColumnType("int(11)")
                .HasColumnName("ipd_hour_cut");
            entity.Property(e => e.IsPttypePlan)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("is_pttype_plan");
            entity.Property(e => e.Isuse)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("isuse");
            entity.Property(e => e.MaxAge)
                .HasColumnType("int(11)")
                .HasColumnName("max_age");
            entity.Property(e => e.MaxDebtMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("max_debt_money");
            entity.Property(e => e.MinAge)
                .HasColumnType("int(11)")
                .HasColumnName("min_age");
            entity.Property(e => e.Name)
                .HasMaxLength(250)
                .HasColumnName("name");
            entity.Property(e => e.NhsoCode)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("nhso_code");
            entity.Property(e => e.NhsoSubinscl)
                .HasMaxLength(3)
                .HasColumnName("nhso_subinscl");
            entity.Property(e => e.Noexpire)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("noexpire");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(5)
                .HasColumnName("oldcode");
            entity.Property(e => e.OpbkkTypeCode)
                .HasMaxLength(2)
                .HasColumnName("opbkk_type_code");
            entity.Property(e => e.Paidst)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("paidst");
            entity.Property(e => e.Pcode)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pcode");
            entity.Property(e => e.PriceType)
                .HasColumnType("int(11)")
                .HasColumnName("price_type");
            entity.Property(e => e.PrintCsmbStatement)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_csmb_statement");
            entity.Property(e => e.PrintPrescNed)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_presc_ned");
            entity.Property(e => e.PttypeEclaimId)
                .HasMaxLength(2)
                .HasColumnName("pttype_eclaim_id");
            entity.Property(e => e.PttypeGroup1)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("pttype_group1");
            entity.Property(e => e.PttypeGroup2)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("pttype_group2");
            entity.Property(e => e.PttypeGuid)
                .HasMaxLength(38)
                .HasColumnName("pttype_guid");
            entity.Property(e => e.PttypeInformation).HasColumnName("pttype_information");
            entity.Property(e => e.PttypePriceGroupId)
                .HasColumnType("int(11)")
                .HasColumnName("pttype_price_group_id");
            entity.Property(e => e.PttypePricePolicyTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("pttype_price_policy_type_id");
            entity.Property(e => e.PttypeSppId)
                .HasColumnType("int(11)")
                .HasColumnName("pttype_spp_id");
            entity.Property(e => e.PttypeStdCode)
                .HasMaxLength(4)
                .IsFixedLength()
                .HasColumnName("pttype_std_code");
            entity.Property(e => e.PttypeUppTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("pttype_upp_type_id");
            entity.Property(e => e.RcpBookno)
                .HasColumnType("int(11)")
                .HasColumnName("rcp_bookno");
            entity.Property(e => e.RegionType)
                .HasColumnType("int(11)")
                .HasColumnName("region_type");
            entity.Property(e => e.RequireHcode)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("require_hcode");
            entity.Property(e => e.Requirecode)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("requirecode");
            entity.Property(e => e.RoundMoney)
                .HasMaxLength(1)
                .HasColumnName("round_money");
            entity.Property(e => e.RxPayDebitTr)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("rx_pay_debit_tr");
            entity.Property(e => e.RxQueueGroupId)
                .HasColumnType("int(11)")
                .HasColumnName("rx_queue_group_id");
            entity.Property(e => e.SeparateDebtId)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("separate_debt_id");
            entity.Property(e => e.SeparateRcpno)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("separate_rcpno");
            entity.Property(e => e.SkhHipdataCode)
                .HasMaxLength(6)
                .HasColumnName("skh_hipdata_code");
            entity.Property(e => e.SksBenefitPlanTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("sks_benefit_plan_type_id");
            entity.Property(e => e.Uc)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("uc");
            entity.Property(e => e.UseContractId)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_contract_id");
            entity.Property(e => e.UsePackage)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_package");
            entity.Property(e => e.YearlyCharge)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("yearly_charge");
            entity.Property(e => e.YearlyChargeIcode1)
                .HasMaxLength(7)
                .HasColumnName("yearly_charge_icode1");
            entity.Property(e => e.YearlyChargeIcode2)
                .HasMaxLength(7)
                .HasColumnName("yearly_charge_icode2");
        });

        modelBuilder.Entity<Pttypeno>(entity =>
        {
            entity.HasKey(e => new { e.Hn, e.Pttype })
                .HasName("PRIMARY")
                .HasAnnotation("MySql:IndexPrefixLength", new[] { 0, 0 });

            entity.ToTable("pttypeno");

            entity.HasIndex(e => e.Hn, "hn");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.Pttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pttype");
            entity.Property(e => e.Begindate).HasColumnName("begindate");
            entity.Property(e => e.Expiredate).HasColumnName("expiredate");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.Hospmain)
                .HasMaxLength(5)
                .HasColumnName("hospmain");
            entity.Property(e => e.Hospsub)
                .HasMaxLength(5)
                .HasColumnName("hospsub");
            entity.Property(e => e.Pttypeno1)
                .HasMaxLength(20)
                .HasColumnName("pttypeno");
        });

        modelBuilder.Entity<SDrugitem>(entity =>
        {
            entity.HasKey(e => e.Icode).HasName("PRIMARY");

            entity.ToTable("s_drugitems");

            entity.HasIndex(e => e.AccessLevel, "ix_access");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => e.IcodeGuid, "ix_icode_guid");

            entity.HasIndex(e => e.MustPaid, "ix_must_paid");

            entity.HasIndex(e => e.Name, "ix_name");

            entity.HasIndex(e => new { e.Name, e.Strength, e.Units }, "ix_nsu");

            entity.HasIndex(e => e.Paidst, "ix_paidst");

            entity.HasIndex(e => new { e.IsMedication, e.Istatus, e.Name, e.Icode }, "ix_search1");

            entity.HasIndex(e => e.Istatus, "ix_status");

            entity.Property(e => e.Icode)
                .HasMaxLength(7)
                .HasColumnName("icode");
            entity.Property(e => e.AccessLevel)
                .HasColumnType("int(11)")
                .HasColumnName("access_level");
            entity.Property(e => e.Billcode)
                .HasMaxLength(10)
                .HasColumnName("billcode");
            entity.Property(e => e.Billnumber)
                .HasMaxLength(15)
                .HasColumnName("billnumber");
            entity.Property(e => e.Cost)
                .HasColumnType("double(15,3)")
                .HasColumnName("cost");
            entity.Property(e => e.CsmbsClaimCat)
                .HasMaxLength(1)
                .HasColumnName("csmbs_claim_cat");
            entity.Property(e => e.Did)
                .HasMaxLength(30)
                .HasColumnName("did");
            entity.Property(e => e.Displaycolor)
                .HasColumnType("int(11)")
                .HasColumnName("displaycolor");
            entity.Property(e => e.DisplaycolorFocus)
                .HasColumnType("int(11)")
                .HasColumnName("displaycolor_focus");
            entity.Property(e => e.Dosageform)
                .HasMaxLength(100)
                .HasColumnName("dosageform");
            entity.Property(e => e.DrgChrgitemId)
                .HasMaxLength(2)
                .HasColumnName("drg_chrgitem_id");
            entity.Property(e => e.Drugaccount)
                .HasMaxLength(2)
                .HasColumnName("drugaccount");
            entity.Property(e => e.Drugcategory)
                .HasMaxLength(150)
                .HasColumnName("drugcategory");
            entity.Property(e => e.Drugnote)
                .HasMaxLength(150)
                .HasColumnName("drugnote");
            entity.Property(e => e.Ename)
                .HasMaxLength(150)
                .HasColumnName("ename");
            entity.Property(e => e.ExportIpd)
                .HasMaxLength(1)
                .HasColumnName("export_ipd");
            entity.Property(e => e.ExportOpd)
                .HasMaxLength(1)
                .HasColumnName("export_opd");
            entity.Property(e => e.Highcost)
                .HasMaxLength(1)
                .HasDefaultValueSql("'N'")
                .HasColumnName("highcost");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.Icd9cm)
                .HasMaxLength(7)
                .HasColumnName("icd9cm");
            entity.Property(e => e.IcodeGuid)
                .HasMaxLength(38)
                .HasColumnName("icode_guid");
            entity.Property(e => e.Income)
                .HasMaxLength(2)
                .HasColumnName("income");
            entity.Property(e => e.IpdPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("ipd_price");
            entity.Property(e => e.IsMedication)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("is_medication");
            entity.Property(e => e.IsMedsupply)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("is_medsupply");
            entity.Property(e => e.Istatus)
                .HasMaxLength(1)
                .HasDefaultValueSql("'Y'")
                .HasColumnName("istatus");
            entity.Property(e => e.MaxQty)
                .HasColumnType("int(11)")
                .HasColumnName("max_qty");
            entity.Property(e => e.MaxQtyIpd)
                .HasColumnType("int(11)")
                .HasColumnName("max_qty_ipd");
            entity.Property(e => e.MustPaid)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("must_paid");
            entity.Property(e => e.Name)
                .HasMaxLength(200)
                .HasColumnName("name");
            entity.Property(e => e.NhsoAdpCode)
                .HasMaxLength(15)
                .HasColumnName("nhso_adp_code");
            entity.Property(e => e.NhsoAdpTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("nhso_adp_type_id");
            entity.Property(e => e.Note)
                .HasMaxLength(100)
                .HasColumnName("note");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(25)
                .HasColumnName("oldcode");
            entity.Property(e => e.Paidst)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("paidst");
            entity.Property(e => e.PriceLock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("price_lock");
            entity.Property(e => e.SksDfsCode)
                .HasMaxLength(50)
                .HasColumnName("sks_dfs_code");
            entity.Property(e => e.SksDrugCode)
                .HasMaxLength(25)
                .HasColumnName("sks_drug_code");
            entity.Property(e => e.SksProductCategoryId)
                .HasColumnType("int(11)")
                .HasColumnName("sks_product_category_id");
            entity.Property(e => e.Stdprice)
                .HasColumnType("double(15,3)")
                .HasColumnName("stdprice");
            entity.Property(e => e.Strength)
                .HasMaxLength(50)
                .HasColumnName("strength");
            entity.Property(e => e.SubIncome)
                .HasMaxLength(3)
                .HasColumnName("sub_income");
            entity.Property(e => e.TpuCodeList)
                .HasMaxLength(200)
                .HasColumnName("tpu_code_list");
            entity.Property(e => e.Unitprice)
                .HasColumnType("double(15,3)")
                .HasColumnName("unitprice");
            entity.Property(e => e.Units)
                .HasMaxLength(50)
                .HasColumnName("units");
            entity.Property(e => e.UsePaidst)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_paidst");
            entity.Property(e => e.UseRight)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("use_right");
        });

        modelBuilder.Entity<Serial>(entity =>
        {
            entity.HasKey(e => e.Name).HasName("PRIMARY");

            entity.ToTable("serial");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.Property(e => e.Name)
                .HasMaxLength(50)
                .HasColumnName("name");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.NodeId)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("node_id");
            entity.Property(e => e.SerialNo)
                .HasColumnType("int(11)")
                .HasColumnName("serial_no");
        });

        modelBuilder.Entity<SpUse>(entity =>
        {
            entity.HasKey(e => e.SpUse1).HasName("PRIMARY");

            entity.ToTable("sp_use");

            entity.HasIndex(e => e.HosGuidExt, "ix_hos_guid_ext");

            entity.HasIndex(e => e.Oldcode, "ix_ix_oldcode");

            entity.HasIndex(e => new { e.Name1, e.Name2, e.Name3 }, "ix_name1_name3_name2");

            entity.HasIndex(e => e.SpUseGuid, "ix_sp_use_guid");

            entity.HasIndex(e => e.SpName, "sp_name");

            entity.HasIndex(e => e.User, "user");

            entity.Property(e => e.SpUse1)
                .HasMaxLength(7)
                .HasColumnName("sp_use");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.HosGuidExt)
                .HasMaxLength(64)
                .HasColumnName("hos_guid_ext");
            entity.Property(e => e.Name1)
                .HasMaxLength(150)
                .HasColumnName("name1");
            entity.Property(e => e.Name2)
                .HasMaxLength(150)
                .HasColumnName("name2");
            entity.Property(e => e.Name3)
                .HasMaxLength(150)
                .HasColumnName("name3");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(20)
                .HasColumnName("oldcode");
            entity.Property(e => e.SpName)
                .HasMaxLength(150)
                .HasColumnName("sp_name");
            entity.Property(e => e.SpUseGuid)
                .HasMaxLength(38)
                .HasColumnName("sp_use_guid");
            entity.Property(e => e.User)
                .HasMaxLength(15)
                .HasColumnName("user");
        });

        modelBuilder.Entity<Spclty>(entity =>
        {
            entity.HasKey(e => e.Spclty1).HasName("PRIMARY");

            entity.ToTable("spclty");

            entity.HasIndex(e => e.Oldcode, "ix_oldcode");

            entity.HasIndex(e => e.SpcltyGuid, "ix_spclty_guid");

            entity.HasIndex(e => e.Spname, "ix_spname");

            entity.HasIndex(e => e.Name, "name");

            entity.Property(e => e.Spclty1)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.ActiveStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("active_status");
            entity.Property(e => e.AllowOnlineAppointment)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("allow_online_appointment");
            entity.Property(e => e.Depcode)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("depcode");
            entity.Property(e => e.DisplayOrder)
                .HasColumnType("int(11)")
                .HasColumnName("display_order");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.IllVisit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ill_visit");
            entity.Property(e => e.Name)
                .HasMaxLength(150)
                .HasColumnName("name");
            entity.Property(e => e.NhsoCode)
                .HasMaxLength(5)
                .HasColumnName("nhso_code");
            entity.Property(e => e.NoExport43)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_export_43");
            entity.Property(e => e.NoServiceCharge)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_service_charge");
            entity.Property(e => e.NoServiceChargeDoctor)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_service_charge_doctor");
            entity.Property(e => e.Oldcode)
                .HasMaxLength(20)
                .HasColumnName("oldcode");
            entity.Property(e => e.Operation)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("operation");
            entity.Property(e => e.Ovstostlink)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("ovstostlink");
            entity.Property(e => e.ProvisCode)
                .HasMaxLength(10)
                .HasColumnName("provis_code");
            entity.Property(e => e.Shortname)
                .HasMaxLength(10)
                .HasColumnName("shortname");
            entity.Property(e => e.SpcltyGuid)
                .HasMaxLength(38)
                .HasColumnName("spclty_guid");
            entity.Property(e => e.Spname)
                .HasMaxLength(150)
                .HasColumnName("spname");
        });

        modelBuilder.Entity<VnStat>(entity =>
        {
            entity.HasKey(e => e.Vn).HasName("PRIMARY");

            entity.ToTable("vn_stat");

            entity.HasIndex(e => e.Cid, "ix_cid");

            entity.HasIndex(e => e.Hn, "ix_hn");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.Hospmain, "ix_hospmain");

            entity.HasIndex(e => e.Hospsub, "ix_hospsub");

            entity.HasIndex(e => e.Pcode, "ix_pcode");

            entity.HasIndex(e => e.Pttype, "ix_pttype");

            entity.HasIndex(e => e.Vstdate, "ix_vstdate");

            entity.HasIndex(e => e.Ym, "ix_ym");

            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.AccidentCode)
                .HasMaxLength(6)
                .HasColumnName("accident_code");
            entity.Property(e => e.AgeD)
                .HasColumnType("smallint(6)")
                .HasColumnName("age_d");
            entity.Property(e => e.AgeM)
                .HasColumnType("smallint(6)")
                .HasColumnName("age_m");
            entity.Property(e => e.AgeY)
                .HasColumnType("smallint(6)")
                .HasColumnName("age_y");
            entity.Property(e => e.Aid)
                .HasMaxLength(6)
                .HasColumnName("aid");
            entity.Property(e => e.ArTransferDatetimeSk)
                .HasColumnType("datetime")
                .HasColumnName("ar_transfer_datetime_sk");
            entity.Property(e => e.ArTransferSk)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ar_transfer_sk");
            entity.Property(e => e.ArTransferStaffSk)
                .HasMaxLength(25)
                .HasColumnName("ar_transfer_staff_sk");
            entity.Property(e => e.Cid)
                .HasMaxLength(13)
                .HasColumnName("cid");
            entity.Property(e => e.CountInDay)
                .HasColumnType("tinyint(4)")
                .HasColumnName("count_in_day");
            entity.Property(e => e.CountInMonth)
                .HasColumnType("smallint(6)")
                .HasColumnName("count_in_month");
            entity.Property(e => e.CountInYear)
                .HasColumnType("smallint(6)")
                .HasColumnName("count_in_year");
            entity.Property(e => e.Dba)
                .HasColumnType("tinyint(4)")
                .HasColumnName("dba");
            entity.Property(e => e.DebtIdList)
                .HasMaxLength(50)
                .HasColumnName("debt_id_list");
            entity.Property(e => e.DiscountMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("discount_money");
            entity.Property(e => e.Dx0)
                .HasMaxLength(6)
                .HasColumnName("dx0");
            entity.Property(e => e.Dx1)
                .HasMaxLength(6)
                .HasColumnName("dx1");
            entity.Property(e => e.Dx2)
                .HasMaxLength(6)
                .HasColumnName("dx2");
            entity.Property(e => e.Dx3)
                .HasMaxLength(6)
                .HasColumnName("dx3");
            entity.Property(e => e.Dx4)
                .HasMaxLength(6)
                .HasColumnName("dx4");
            entity.Property(e => e.Dx5)
                .HasMaxLength(6)
                .HasColumnName("dx5");
            entity.Property(e => e.DxDoctor)
                .HasMaxLength(7)
                .HasColumnName("dx_doctor");
            entity.Property(e => e.Gr504)
                .HasColumnType("smallint(6)")
                .HasColumnName("gr504");
            entity.Property(e => e.Hcode)
                .HasMaxLength(5)
                .HasColumnName("hcode");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.Hospmain)
                .HasMaxLength(5)
                .HasColumnName("hospmain");
            entity.Property(e => e.Hospsub)
                .HasMaxLength(5)
                .HasColumnName("hospsub");
            entity.Property(e => e.IllVisit)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ill_visit");
            entity.Property(e => e.Inc01)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc01");
            entity.Property(e => e.Inc02)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc02");
            entity.Property(e => e.Inc03)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc03");
            entity.Property(e => e.Inc04)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc04");
            entity.Property(e => e.Inc05)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc05");
            entity.Property(e => e.Inc06)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc06");
            entity.Property(e => e.Inc07)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc07");
            entity.Property(e => e.Inc08)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc08");
            entity.Property(e => e.Inc09)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc09");
            entity.Property(e => e.Inc10)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc10");
            entity.Property(e => e.Inc11)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc11");
            entity.Property(e => e.Inc12)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc12");
            entity.Property(e => e.Inc13)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc13");
            entity.Property(e => e.Inc14)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc14");
            entity.Property(e => e.Inc15)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc15");
            entity.Property(e => e.Inc16)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc16");
            entity.Property(e => e.Inc17)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc17");
            entity.Property(e => e.IncDrug)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc_drug");
            entity.Property(e => e.IncNondrug)
                .HasColumnType("double(15,3)")
                .HasColumnName("inc_nondrug");
            entity.Property(e => e.Income)
                .HasColumnType("double(15,3)")
                .HasColumnName("income");
            entity.Property(e => e.ItemMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("item_money");
            entity.Property(e => e.LabPaidOk)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("lab_paid_ok");
            entity.Property(e => e.Lastvisit)
                .HasColumnType("smallint(6)")
                .HasColumnName("lastvisit");
            entity.Property(e => e.LastvisitHour)
                .HasColumnType("int(11)")
                .HasColumnName("lastvisit_hour");
            entity.Property(e => e.LastvisitVn)
                .HasMaxLength(13)
                .HasColumnName("lastvisit_vn");
            entity.Property(e => e.MainPdx)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("main_pdx");
            entity.Property(e => e.Moopart)
                .HasMaxLength(5)
                .HasColumnName("moopart");
            entity.Property(e => e.NodeId)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("node_id");
            entity.Property(e => e.OldDiagnosis)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("old_diagnosis");
            entity.Property(e => e.Op0)
                .HasMaxLength(6)
                .HasColumnName("op0");
            entity.Property(e => e.Op1)
                .HasMaxLength(6)
                .HasColumnName("op1");
            entity.Property(e => e.Op2)
                .HasMaxLength(6)
                .HasColumnName("op2");
            entity.Property(e => e.Op3)
                .HasMaxLength(6)
                .HasColumnName("op3");
            entity.Property(e => e.Op4)
                .HasMaxLength(6)
                .HasColumnName("op4");
            entity.Property(e => e.Op5)
                .HasMaxLength(6)
                .HasColumnName("op5");
            entity.Property(e => e.PaidMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("paid_money");
            entity.Property(e => e.Pcode)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pcode");
            entity.Property(e => e.Pdx)
                .HasMaxLength(6)
                .HasColumnName("pdx");
            entity.Property(e => e.PrintCount)
                .HasColumnType("tinyint(4)")
                .HasColumnName("print_count");
            entity.Property(e => e.PrintDone)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_done");
            entity.Property(e => e.PtSubtype)
                .HasColumnType("tinyint(4)")
                .HasColumnName("pt_subtype");
            entity.Property(e => e.Pttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pttype");
            entity.Property(e => e.PttypeBegin).HasColumnName("pttype_begin");
            entity.Property(e => e.PttypeExpire).HasColumnName("pttype_expire");
            entity.Property(e => e.PttypeInChwpart)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pttype_in_chwpart");
            entity.Property(e => e.PttypeInRegion)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pttype_in_region");
            entity.Property(e => e.Pttypeno)
                .HasMaxLength(20)
                .HasColumnName("pttypeno");
            entity.Property(e => e.RcpNo)
                .HasMaxLength(10)
                .HasColumnName("rcp_no");
            entity.Property(e => e.RcpnoList)
                .HasMaxLength(100)
                .HasColumnName("rcpno_list");
            entity.Property(e => e.RcptMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("rcpt_money");
            entity.Property(e => e.RemainMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("remain_money");
            entity.Property(e => e.RxLicenseNo)
                .HasMaxLength(15)
                .HasColumnName("rx_license_no");
            entity.Property(e => e.Sex)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("sex");
            entity.Property(e => e.Spclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.UcMoney)
                .HasColumnType("double(15,3)")
                .HasColumnName("uc_money");
            entity.Property(e => e.VnGuid)
                .HasMaxLength(38)
                .HasColumnName("vn_guid");
            entity.Property(e => e.Vstdate).HasColumnName("vstdate");
            entity.Property(e => e.XrayPaidOk)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("xray_paid_ok");
            entity.Property(e => e.Ym)
                .HasMaxLength(7)
                .HasColumnName("ym");
        });

        modelBuilder.Entity<Ward>(entity =>
        {
            entity.HasKey(e => e.Ward1).HasName("PRIMARY");

            entity.ToTable("ward");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.OldCode, "ix_old_code");

            entity.HasIndex(e => e.Name, "name");

            entity.Property(e => e.Ward1)
                .HasMaxLength(4)
                .HasColumnName("ward");
            entity.Property(e => e.Bedcount)
                .HasColumnType("int(11)")
                .HasColumnName("bedcount");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.IpKey)
                .HasMaxLength(50)
                .HasColumnName("ip_key");
            entity.Property(e => e.IpdRxShiftTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("ipd_rx_shift_type_id");
            entity.Property(e => e.Name)
                .HasMaxLength(250)
                .HasColumnName("name");
            entity.Property(e => e.NameOldSk)
                .HasMaxLength(250)
                .HasColumnName("name_old_sk");
            entity.Property(e => e.OldCode)
                .HasMaxLength(15)
                .HasColumnName("old_code");
            entity.Property(e => e.SelectBednoFromLayout)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("select_bedno_from_layout");
            entity.Property(e => e.ShortName)
                .HasMaxLength(100)
                .HasColumnName("short_name");
            entity.Property(e => e.ShortNameBarcode)
                .HasMaxLength(100)
                .HasColumnName("short_name_barcode");
            entity.Property(e => e.Shortname)
                .HasMaxLength(20)
                .HasColumnName("shortname");
            entity.Property(e => e.Spclty)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("spclty");
            entity.Property(e => e.SssCode)
                .HasMaxLength(10)
                .HasColumnName("sss_code");
            entity.Property(e => e.StrictAccess)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("strict_access");
            entity.Property(e => e.WardActive)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ward_active");
            entity.Property(e => e.WardExportCode)
                .HasMaxLength(50)
                .HasColumnName("ward_export_code");
        });

        modelBuilder.Entity<XrayHead>(entity =>
        {
            entity.HasKey(e => e.Vn).HasName("PRIMARY");

            entity.ToTable("xray_head");

            entity.HasIndex(e => e.Hn, "hn");

            entity.HasIndex(e => e.ConfirmReport, "ix_confirm_report");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.OrderDate, "ix_order_date");

            entity.HasIndex(e => e.OrderDateTime, "ix_order_date_time");

            entity.HasIndex(e => e.XrayOrderNumber, "ix_xray_order_number_unique").IsUnique();

            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.AgeY)
                .HasColumnType("int(11)")
                .HasColumnName("age_y");
            entity.Property(e => e.AiAbnormal)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ai_abnormal");
            entity.Property(e => e.AiAvailable)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ai_available");
            entity.Property(e => e.BeginDate).HasColumnName("begin_date");
            entity.Property(e => e.BeginTime)
                .HasColumnType("time")
                .HasColumnName("begin_time");
            entity.Property(e => e.ConfirmAll)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm_all");
            entity.Property(e => e.ConfirmRadiologyAll)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm_radiology_all");
            entity.Property(e => e.ConfirmReadFilm)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm_read_film");
            entity.Property(e => e.ConfirmReport)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm_report");
            entity.Property(e => e.Department)
                .HasMaxLength(3)
                .IsFixedLength()
                .HasColumnName("department");
            entity.Property(e => e.DepartmentCode)
                .HasMaxLength(4)
                .HasColumnName("department_code");
            entity.Property(e => e.DepartmentList)
                .HasColumnType("text")
                .HasColumnName("department_list");
            entity.Property(e => e.DepartmentName)
                .HasMaxLength(100)
                .HasColumnName("department_name");
            entity.Property(e => e.DoctorCode)
                .HasMaxLength(4)
                .HasColumnName("doctor_code");
            entity.Property(e => e.DoctorList)
                .HasColumnType("text")
                .HasColumnName("doctor_list");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.IsPregnant)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("is_pregnant");
            entity.Property(e => e.OrderDate).HasColumnName("order_date");
            entity.Property(e => e.OrderDateTime)
                .HasColumnType("datetime")
                .HasColumnName("order_date_time");
            entity.Property(e => e.OrderTime)
                .HasColumnType("time")
                .HasColumnName("order_time");
            entity.Property(e => e.PartName)
                .HasMaxLength(150)
                .HasColumnName("part_name");
            entity.Property(e => e.PrintStatus)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("print_status");
            entity.Property(e => e.PtXn)
                .HasColumnType("int(11)")
                .HasColumnName("pt_xn");
            entity.Property(e => e.Pttype)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("pttype");
            entity.Property(e => e.ReceiveNo)
                .HasColumnType("int(11)")
                .HasColumnName("receive_no");
            entity.Property(e => e.ReportDate).HasColumnName("report_date");
            entity.Property(e => e.ReportTime)
                .HasColumnType("time")
                .HasColumnName("report_time");
            entity.Property(e => e.ReporterName)
                .HasMaxLength(100)
                .HasColumnName("reporter_name");
            entity.Property(e => e.ResultNote)
                .HasColumnType("text")
                .HasColumnName("result_note");
            entity.Property(e => e.ServiceTypeNameList)
                .HasMaxLength(200)
                .HasColumnName("service_type_name_list");
            entity.Property(e => e.TotalInyearRequest)
                .HasColumnType("int(11)")
                .HasColumnName("total_inyear_request");
            entity.Property(e => e.TotalPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("total_price");
            entity.Property(e => e.TotalRequest)
                .HasColumnType("int(11)")
                .HasColumnName("total_request");
            entity.Property(e => e.WaitXrayList)
                .HasColumnType("text")
                .HasColumnName("wait_xray_list");
            entity.Property(e => e.Ward)
                .HasMaxLength(2)
                .IsFixedLength()
                .HasColumnName("ward");
            entity.Property(e => e.XrayConfirmFlag)
                .HasColumnType("int(11)")
                .HasColumnName("xray_confirm_flag");
            entity.Property(e => e.XrayFilmFlag)
                .HasColumnType("int(11)")
                .HasColumnName("xray_film_flag");
            entity.Property(e => e.XrayHeadFlag)
                .HasColumnType("int(11)")
                .HasColumnName("xray_head_flag");
            entity.Property(e => e.XrayHeadRemark)
                .HasMaxLength(250)
                .HasColumnName("xray_head_remark");
            entity.Property(e => e.XrayList)
                .HasColumnType("text")
                .HasColumnName("xray_list");
            entity.Property(e => e.XrayOrderNumber)
                .HasColumnType("int(11)")
                .HasColumnName("xray_order_number");
            entity.Property(e => e.XrayPrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("xray_price");
            entity.Property(e => e.XrayPriorityId)
                .HasColumnType("int(11)")
                .HasColumnName("xray_priority_id");
        });

        modelBuilder.Entity<XrayReport>(entity =>
        {
            entity.HasKey(e => e.Xn).HasName("PRIMARY");

            entity.ToTable("xray_report");

            entity.HasIndex(e => e.XrayFormOrderId, "ix_form_order_id");

            entity.HasIndex(e => e.HosGuid, "ix_hos_guid");

            entity.HasIndex(e => e.An, "ix_ix_an");

            entity.HasIndex(e => e.Hn, "ix_ix_hn");

            entity.HasIndex(e => e.ReportDate, "ix_ix_report_date");

            entity.HasIndex(e => e.RequestDate, "ix_ix_request_date");

            entity.HasIndex(e => e.Vn, "ix_ix_vn");

            entity.HasIndex(e => e.OpitemreceGuid, "ix_opiguid");

            entity.HasIndex(e => e.RefOrderCode, "ix_ref_order_code");

            entity.HasIndex(e => new { e.PacsFlag, e.RequestDate, e.ConfirmRadiology }, "ix_rf");

            entity.HasIndex(e => new { e.ReportDate, e.PacsFlag }, "ix_rp");

            entity.HasIndex(e => new { e.Xn, e.XrayItemsCode }, "ix_xn_xray_items_code");

            entity.Property(e => e.Xn)
                .ValueGeneratedNever()
                .HasColumnType("int(11)")
                .HasColumnName("xn");
            entity.Property(e => e.AcceptDate).HasColumnName("accept_date");
            entity.Property(e => e.AcceptTime)
                .HasColumnType("time")
                .HasColumnName("accept_time");
            entity.Property(e => e.AiAvailable)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("ai_available");
            entity.Property(e => e.AiReportTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("ai_report_type_id");
            entity.Property(e => e.An)
                .HasMaxLength(9)
                .HasColumnName("an");
            entity.Property(e => e.ClinicalDiagnosis)
                .HasColumnType("text")
                .HasColumnName("clinical_diagnosis");
            entity.Property(e => e.ClinicalInformation)
                .HasColumnType("text")
                .HasColumnName("clinical_information");
            entity.Property(e => e.Confirm)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm");
            entity.Property(e => e.ConfirmRadiology)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm_radiology");
            entity.Property(e => e.ConfirmRadiologyDate).HasColumnName("confirm_radiology_date");
            entity.Property(e => e.ConfirmRadiologyTime)
                .HasColumnType("time")
                .HasColumnName("confirm_radiology_time");
            entity.Property(e => e.ConfirmReadDate).HasColumnName("confirm_read_date");
            entity.Property(e => e.ConfirmReadFilm)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("confirm_read_film");
            entity.Property(e => e.ConfirmReadTime)
                .HasColumnType("time")
                .HasColumnName("confirm_read_time");
            entity.Property(e => e.ConsultFlag)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("consult_flag");
            entity.Property(e => e.ConsultText)
                .HasColumnType("text")
                .HasColumnName("consult_text");
            entity.Property(e => e.Doctor)
                .HasMaxLength(6)
                .HasColumnName("doctor");
            entity.Property(e => e.DoctorFee)
                .HasColumnType("double(15,3)")
                .HasColumnName("doctor_fee");
            entity.Property(e => e.ExaminedDate).HasColumnName("examined_date");
            entity.Property(e => e.ExaminedTime)
                .HasColumnType("time")
                .HasColumnName("examined_time");
            entity.Property(e => e.FinalReadFilm)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("final_read_film");
            entity.Property(e => e.FinanceLock)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("finance_lock");
            entity.Property(e => e.FinanceXrayClear)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("finance_xray_clear");
            entity.Property(e => e.Hn)
                .HasMaxLength(9)
                .HasColumnName("hn");
            entity.Property(e => e.HosGuid)
                .HasMaxLength(38)
                .HasColumnName("hos_guid");
            entity.Property(e => e.NoRadiologist)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("no_radiologist");
            entity.Property(e => e.Normal)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("normal");
            entity.Property(e => e.OpitemreceGuid)
                .HasMaxLength(38)
                .HasColumnName("opitemrece_guid");
            entity.Property(e => e.OrderDatetime)
                .HasColumnType("datetime")
                .HasColumnName("order_datetime");
            entity.Property(e => e.PacEmergencyCode)
                .HasColumnType("int(11)")
                .HasColumnName("pac_emergency_code");
            entity.Property(e => e.PacRequest)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pac_request");
            entity.Property(e => e.PacRequestDatetime)
                .HasColumnType("datetime")
                .HasColumnName("pac_request_datetime");
            entity.Property(e => e.PacRequestMessage)
                .HasMaxLength(150)
                .HasColumnName("pac_request_message");
            entity.Property(e => e.PackStatus)
                .HasMaxLength(1)
                .HasColumnName("pack_status");
            entity.Property(e => e.PacsFlag)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pacs_flag");
            entity.Property(e => e.Pregnant)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("pregnant");
            entity.Property(e => e.PtClassXn)
                .HasColumnType("int(11)")
                .HasColumnName("pt_class_xn");
            entity.Property(e => e.PtXn)
                .HasColumnType("int(11)")
                .HasColumnName("pt_xn");
            entity.Property(e => e.RadiologistReportResult)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("radiologist_report_result");
            entity.Property(e => e.RefOrderCode)
                .HasMaxLength(50)
                .HasColumnName("ref_order_code");
            entity.Property(e => e.ReportDate).HasColumnName("report_date");
            entity.Property(e => e.ReportDoctor)
                .HasMaxLength(10)
                .HasColumnName("report_doctor");
            entity.Property(e => e.ReportRtf).HasColumnName("report_rtf");
            entity.Property(e => e.ReportText).HasColumnName("report_text");
            entity.Property(e => e.ReportTime)
                .HasColumnType("time")
                .HasColumnName("report_time");
            entity.Property(e => e.RequestCountInYear)
                .HasColumnType("int(11)")
                .HasColumnName("request_count_in_year");
            entity.Property(e => e.RequestDate).HasColumnName("request_date");
            entity.Property(e => e.RequestDepcode)
                .HasMaxLength(3)
                .HasColumnName("request_depcode");
            entity.Property(e => e.RequestDoctor)
                .HasMaxLength(6)
                .HasColumnName("request_doctor");
            entity.Property(e => e.RequestNote)
                .HasColumnType("text")
                .HasColumnName("request_note");
            entity.Property(e => e.RequestStaff)
                .HasMaxLength(25)
                .HasColumnName("request_staff");
            entity.Property(e => e.RequestTime)
                .HasColumnType("time")
                .HasColumnName("request_time");
            entity.Property(e => e.ServicePrice)
                .HasColumnType("double(15,3)")
                .HasColumnName("service_price");
            entity.Property(e => e.Staff)
                .HasMaxLength(25)
                .HasColumnName("staff");
            entity.Property(e => e.VisitTimeTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("visit_time_type_id");
            entity.Property(e => e.Vn)
                .HasMaxLength(13)
                .HasColumnName("vn");
            entity.Property(e => e.WalkId)
                .HasColumnType("int(11)")
                .HasColumnName("walk_id");
            entity.Property(e => e.WriteData)
                .HasMaxLength(1)
                .IsFixedLength()
                .HasColumnName("write_data");
            entity.Property(e => e.XrayFormOrderId)
                .HasColumnType("int(11)")
                .HasColumnName("xray_form_order_id");
            entity.Property(e => e.XrayIcd10)
                .HasMaxLength(7)
                .HasColumnName("xray_icd10");
            entity.Property(e => e.XrayIcd102)
                .HasMaxLength(7)
                .HasColumnName("xray_icd10_2");
            entity.Property(e => e.XrayIcd103)
                .HasMaxLength(10)
                .HasColumnName("xray_icd10_3");
            entity.Property(e => e.XrayIcd104)
                .HasMaxLength(10)
                .HasColumnName("xray_icd10_4");
            entity.Property(e => e.XrayItemsCode)
                .HasColumnType("int(11)")
                .HasColumnName("xray_items_code");
            entity.Property(e => e.XrayItemsCountInYear)
                .HasColumnType("int(11)")
                .HasColumnName("xray_items_count_in_year");
            entity.Property(e => e.XrayItemsGroupCountInYear)
                .HasColumnType("int(11)")
                .HasColumnName("xray_items_group_count_in_year");
            entity.Property(e => e.XrayItemsGroupTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("xray_items_group_type_id");
            entity.Property(e => e.XrayItemsServiceTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("xray_items_service_type_id");
            entity.Property(e => e.XrayKeeperId)
                .HasColumnType("int(11)")
                .HasColumnName("xray_keeper_id");
            entity.Property(e => e.XrayNote)
                .HasColumnType("text")
                .HasColumnName("xray_note");
            entity.Property(e => e.XrayPriorityId)
                .HasColumnType("int(11)")
                .HasColumnName("xray_priority_id");
            entity.Property(e => e.XrayPtStatus)
                .HasColumnType("int(11)")
                .HasColumnName("xray_pt_status");
            entity.Property(e => e.XrayReportStatusId)
                .HasColumnType("int(11)")
                .HasColumnName("xray_report_status_id");
            entity.Property(e => e.XrayRoomId)
                .HasColumnType("int(11)")
                .HasColumnName("xray_room_id");
            entity.Property(e => e.XraySide)
                .HasColumnType("int(11)")
                .HasColumnName("xray_side");
            entity.Property(e => e.XrayStatusId)
                .HasColumnType("int(11)")
                .HasColumnName("xray_status_id");
            entity.Property(e => e.XrayTimeTypeId)
                .HasColumnType("int(11)")
                .HasColumnName("xray_time_type_id");
            entity.Property(e => e.XrayTryCount)
                .HasColumnType("int(11)")
                .HasColumnName("xray_try_count");
            entity.Property(e => e.XrayType)
                .HasColumnType("int(11)")
                .HasColumnName("xray_type");
        });

        OnModelCreatingPartial(modelBuilder);
    }

    partial void OnModelCreatingPartial(ModelBuilder modelBuilder);
}
