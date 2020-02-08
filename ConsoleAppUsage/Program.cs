using LinqToCsvLibrary;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Diagnostics;

namespace ConsoleAppUsage
{
  class Program
  {
    static void Main(string[] args)
    {
      Action<string> display = Console.WriteLine;
      CsvFile.UseLambdas = true;
      CsvFile.UseTasks = false;
      CsvFile.FastIndexOfAny = true;
      CreateRandomClientsCsvFile();
      QueryClients();

      display("Press any key to exit:");
      Console.ReadKey();
    }

    private static string QueryClients()
    {
      string result = "Clients with JFK initials";

      foreach (var c in from client in CsvFile.Read<Client>("clients.csv")
                        where client.FirstName.StartsWith("J")
                            && client.MiddleName.StartsWith("F")
                            && client.LastName.StartsWith("K")
                        select client)
      {
        result +=string.Format("Client #{0}: {1} ({2} in {3})",
            c.ClientId,
            c.FirstName + (string.IsNullOrEmpty(c.MiddleName) ? " " : " " + c.MiddleName + " ") + c.LastName,
            c.Occupation,
            c.City);
      }

      return result;
    }

    private static void SortClientsByCity()
    {
      Console.WriteLine();
      Console.WriteLine("Sorting clients into a file for each City");
      int processed = 0;

      var files = new Dictionary<string, LinqToCsvLibrary.CsvFile<Client>>();

      foreach (var c in CsvFile.Read<Client>("clients.csv"))
      {
        processed++;
        if (processed % 1000 == 0)
        {
          Console.Write(string.Format("\r{0} clients sorted.", processed));
        }

        CsvFile<Client> csvFile;
        if (!files.TryGetValue(c.City ?? "Blank", out csvFile))
        {
          csvFile = new CsvFile<Client>(c.City);
          files[c.City ?? "Blank"] = csvFile;
        }

        csvFile.Append(c);
      }

      foreach (var f in files.Values)
      {
        f.Dispose();
      }

      Console.WriteLine();
    }

    private static void Top5BiggestCsvFiles()
    {
      Console.WriteLine();
      Console.WriteLine("Getting 5 biggest CSV files...");
      new DirectoryInfo(Directory.GetCurrentDirectory()).GetFiles("*.csv", SearchOption.AllDirectories)
          .OrderByDescending(fi => fi.Length)
          .Take(5)
          .ToCsv("Top5CsvFiles.csv", new CsvDefinition()
          {
            FieldSeparator = '\t',
            Columns = new String[] { "Name", "Length" }
          });

      Console.WriteLine(File.ReadAllText("Top5CsvFiles.csv"));
    }

    static Random rnd = new Random(38452847);

    private static void CreateRandomClientsCsvFile()
    {
      Console.WriteLine();
      Console.WriteLine("Creating Random Clients...");
      using (var csvFile = new CsvFile<Client>("clients.csv"))
      {
        for (int i = 0; i < 1000000; i++)
        {
          var user = Client.RandomClient();
          user.ClientId = i + 100;
          csvFile.Append(user);

          if ((i + 1) % 1000 == 0)
          {
            Console.Write(string.Format("Writing {0} ({1}/{2})\r", "clients.csv", i + 1, 1000000));
          }
        }
      }

      Console.WriteLine();
    }

    private static void dummy()
    {
      using (var csvFile = new CsvFile<Client>("clients.csv"))
      {
        for (int i = 0; i < 1000000; i++)
        {
          var user = Client.RandomClient();
          csvFile.Append(user);
        }
      }
    }
  }
}
