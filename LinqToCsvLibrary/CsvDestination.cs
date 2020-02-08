using System;
using System.IO;

namespace LinqToCsvLibrary
{
  public class CsvDestination
  {
    public StreamWriter StreamWriter;

    public static implicit operator CsvDestination(string path)
    {
      return new CsvDestination(path);
    }
    private CsvDestination(StreamWriter streamWriter)
    {
      StreamWriter = streamWriter;
    }

    private CsvDestination(Stream stream)
    {
      StreamWriter = new StreamWriter(stream);
    }

    public CsvDestination(string fullName)
    {
      FixCsvFileName(ref fullName);
      StreamWriter = new StreamWriter(fullName);
    }

    private static void FixCsvFileName(ref string fullName)
    {
      fullName = Path.GetFullPath(fullName);
      var path = Path.GetDirectoryName(fullName);
      if (path != null && !Directory.Exists(path))
      {
        Directory.CreateDirectory(path);
      }

      if (!string.Equals(Path.GetExtension(fullName), ".csv"))
      {
        fullName += ".csv";
      }
    }
  }
}