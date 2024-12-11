using System;
using System.Collections.Generic;

namespace RealtimeDataApp.Models;

public partial class LabOrderImage
{
    public int LabOrderNumber { get; set; }

    public byte[]? Image1 { get; set; }

    public string? Image1Note { get; set; }

    public byte[]? Image2 { get; set; }

    public string? Image2Note { get; set; }

    public byte[]? Image3 { get; set; }

    public string? Image3Note { get; set; }

    public byte[]? Image4 { get; set; }

    public string? Image4Note { get; set; }

    public byte[]? Image5 { get; set; }

    public string? Image5Note { get; set; }

    public string? HosGuid { get; set; }
}
