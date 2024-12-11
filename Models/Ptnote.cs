using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class Ptnote
{
    public int PtnoteId { get; set; }

    public string? Hn { get; set; }

    public string? Noteflag { get; set; }

    public string? Ptnote1 { get; set; }

    public DateOnly? Vstdate { get; set; }

    public TimeOnly? Vsttime { get; set; }

    public string? Groupname { get; set; }

    public string? PlainText { get; set; }

    public string? PrscNoteText { get; set; }

    public DateTime? NoteDatetime { get; set; }

    public string? NoteStaff { get; set; }

    public string? UsernameList { get; set; }

    public string? HasExpired { get; set; }

    public DateOnly? ExpireDate { get; set; }

    public string? PublicNote { get; set; }

    public string? EncryptNote { get; set; }

    public string? HosGuid { get; set; }

    public string? ShowAllDep { get; set; }

    public string? CheckGroup { get; set; }

    public string? PtnoteHtml { get; set; }
}
