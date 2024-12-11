using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class PatientImage
{
    public string Hn { get; set; } = null!;

    public string ImageName { get; set; } = null!;

    public byte[]? Image { get; set; }

    public int? Width { get; set; }

    public int? Height { get; set; }

    public DateTime? CaptureDate { get; set; }

    public string? HosGuid { get; set; }

    public string? HosGuidExt { get; set; }
}
