using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace LinqToCsvLibrary
{
  public class CsvFile : IDisposable
  {
    #region Static Members
    public static CsvDefinition DefaultCsvDefinition { get; set; }
    public static bool UseLambdas { get; set; }
    public static bool UseTasks { get; set; }
    public static bool FastIndexOfAny { get; set; }

    static CsvFile()
    {
      // Choosing default Field Separator is a hard decision
      // In theory the C of CSV means Comma separated 
      // In Windows though many people will try to open the csv with Excel which is horrid with real CSV.
      // As the target for this library is Windows we go for Semi Colon.
      DefaultCsvDefinition = new CsvDefinition
      {
        EndOfLine = "\r\n",
        FieldSeparator = ';',
        TextQualifier = '"'
      };

      UseLambdas = true;
      UseTasks = true;
      FastIndexOfAny = true;
    }

    #endregion

    internal protected Stream BaseStream;
    protected static DateTime DateTimeZero = new DateTime();


    public static IEnumerable<T> Read<T>(CsvSource csvSource) where T : new()
    {
      var csvFileReader = new CsvFileReader<T>(csvSource);
      return (IEnumerable<T>)csvFileReader;
    }

    public char FieldSeparator { get; private set; }
    public char TextQualifier { get; private set; }
    public IEnumerable<String> Columns { get; private set; }

    public void Dispose()
    {
      Dispose(true);
    }

    protected virtual void Dispose(bool disposing)
    {
      // overriden in derived classes
    }
  }

  public class CsvFile<T> : CsvFile
  {
    private readonly char fieldSeparator;
    private readonly string fieldSeparatorAsString;
    private readonly char[] invalidCharsInFields;
    private readonly StreamWriter streamWriter;
    private readonly char textQualifier;
    private readonly String[] columns;
    private Func<T, object>[] getters;
    readonly bool[] isInvalidCharInFields;
#if !NET_3_5
    private int linesToWrite;
    private readonly BlockingCollection<string> csvLinesToWrite = new BlockingCollection<string>(5000);
    private readonly Thread writeCsvLinesTask;
    private Task addAsyncTask;
#endif

    public CsvFile(CsvDestination csvDestination)
        : this(csvDestination, null)
    {
    }

    public CsvFile()
    {
    }

    public CsvFile(CsvDestination csvDestination, CsvDefinition csvDefinition)
    {
      if (csvDefinition == null)
      {
        csvDefinition = DefaultCsvDefinition;
      }

      columns = (csvDefinition.Columns ?? InferColumns(typeof(T))).ToArray();
      fieldSeparator = csvDefinition.FieldSeparator;
      fieldSeparatorAsString = fieldSeparator.ToString(CultureInfo.InvariantCulture);
      textQualifier = csvDefinition.TextQualifier;
      streamWriter = csvDestination.StreamWriter;

      invalidCharsInFields = new[] { '\r', '\n', textQualifier, fieldSeparator };
      isInvalidCharInFields = new bool[256];

      foreach (var c in invalidCharsInFields)
      {
        isInvalidCharInFields[c] = true;
      }

      WriteHeader();
      CreateGetters();
#if !NET_3_5
      if (CsvFile.UseTasks)
      {
        writeCsvLinesTask = new Thread((o) => WriteCsvLines());
        writeCsvLinesTask.Start();
      }

      addAsyncTask = Task.Factory.StartNew(() => { });
#endif
    }

    protected override void Dispose(bool disposing)
    {
      if (disposing)
      {
        // free managed resources
#if !NET_3_5
        addAsyncTask.Wait();
        if (csvLinesToWrite != null)
        {
          csvLinesToWrite.CompleteAdding();
        }

        if (writeCsvLinesTask != null)
        {
          writeCsvLinesTask.Join();
        }
#endif
        streamWriter.Close();
      }
    }

    protected static IEnumerable<string> InferColumns(Type recordType)
    {
      var columns = recordType
          .GetProperties(BindingFlags.Public | BindingFlags.Instance)
          .Where(pi => pi.GetIndexParameters().Length == 0
              && pi.GetSetMethod() != null
              && !Attribute.IsDefined(pi, typeof(CsvIgnorePropertyAttribute)))
          .Select(pi => pi.Name)
          .Concat(recordType
              .GetFields(BindingFlags.Public | BindingFlags.Instance)
              .Where(fi => !Attribute.IsDefined(fi, typeof(CsvIgnorePropertyAttribute)))
              .Select(fi => fi.Name))
          .ToList();
      return columns;
    }

#if !NET_3_5
    private void WriteCsvLines()
    {
      int written = 0;
      foreach (var csvLine in csvLinesToWrite.GetConsumingEnumerable())
      {
        streamWriter.WriteLine(csvLine);
        written++;
      }

      Interlocked.Add(ref linesToWrite, -written);
    }
#endif

    public void Append(T record)
    {
      if (CsvFile.UseTasks)
      {
#if !NET_3_5

        var linesWaiting = Interlocked.Increment(ref linesToWrite);
        Action<Task> addRecord = (t) =>
        {
          var csvLine = ToCsv(record);
          csvLinesToWrite.Add(csvLine);
        };

        if (linesWaiting < 10000)
        {
          addAsyncTask = addAsyncTask.ContinueWith(addRecord);
        }
        else
        {
          addRecord(null);
        }
#else
                throw new NotImplementedException("Tasks");
#endif
      }
      else
      {
        var csvLine = ToCsv(record);
        streamWriter.WriteLine(csvLine);
      }
    }

    private static Func<T, object> FindGetter(string c, bool staticMember)
    {
      var flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase | (staticMember ? BindingFlags.Static : BindingFlags.Instance);
      Func<T, object> func = null;
      PropertyInfo propertyInfo = typeof(T).GetProperty(c, flags);
      FieldInfo fi = typeof(T).GetField(c, flags);

      if (UseLambdas)
      {
        Expression expr = null;
        ParameterExpression parameter = Expression.Parameter(typeof(T), "r");
        Type type = null;

        if (propertyInfo != null)
        {
          type = propertyInfo.PropertyType;
          expr = Expression.Property(parameter, propertyInfo.Name);
        }
        else if (fi != null)
        {
          type = fi.FieldType;
          expr = Expression.Field(parameter, fi.Name);
        }
        if (expr != null)
        {
          Expression<Func<T, object>> lambda;
          if (type.IsValueType)
          {
            lambda = Expression.Lambda<Func<T, object>>(Expression.TypeAs(expr, typeof(object)), parameter);
          }
          else
          {
            lambda = Expression.Lambda<Func<T, object>>(expr, parameter);
          }
          func = lambda.Compile();
        }
      }
      else
      {
        if (propertyInfo != null)
        {
          func = o => propertyInfo.GetValue(o, null);
        }
        else if (fi != null)
        {
          func = o => fi.GetValue(o);
        }
      }

      return func;
    }

    private void CreateGetters()
    {
      var list = new List<Func<T, object>>();

      foreach (var columnName in columns)
      {
        Func<T, Object> func = null;
        var propertyName = (columnName.IndexOf(' ') < 0 ? columnName : columnName.Replace(" ", ""));
        func = FindGetter(columnName, false) ?? FindGetter(columnName, true);
        list.Add(func);
      }

      getters = list.ToArray();
    }

    public string ToCsv(T record)
    {
      if (record == null)
      {
        throw new ArgumentException("Cannot be null", "record");
      }

      string[] csvStrings = new string[getters.Length];

      for (int i = 0; i < getters.Length; i++)
      {
        var getter = getters[i];
        object fieldValue = getter == null ? null : getter(record);
        csvStrings[i] = ToCsvString(fieldValue);
      }

      return string.Join(fieldSeparatorAsString, csvStrings);
    }

    private string ToCsvString(object oneObject)
    {
      if (oneObject != null)
      {
        string valueString = oneObject as string ?? Convert.ToString(oneObject, CultureInfo.CurrentUICulture);
        if (RequiresQuotes(valueString))
        {
          var csvLine = new StringBuilder();
          csvLine.Append(textQualifier);
          foreach (char c in valueString)
          {
            if (c == textQualifier)
            {
              csvLine.Append(c); // double the double quotes
            }

            csvLine.Append(c);
          }

          csvLine.Append(textQualifier);
          return csvLine.ToString();
        }
        else
        {
          return valueString;
        }
      }

      return string.Empty;
    }

    private bool RequiresQuotes(string valueString)
    {
      if (CsvFile.FastIndexOfAny)
      {
        var len = valueString.Length;
        for (int i = 0; i < len; i++)
        {
          char c = valueString[i];
          if (c <= 255 && isInvalidCharInFields[c])
          {
            return true;
          }
        }

        return false;
      }
      else
      {
        return valueString.IndexOfAny(invalidCharsInFields) >= 0;
      }
    }

    private void WriteHeader()
    {
      var csvLine = new StringBuilder();
      for (int i = 0; i < columns.Length; i++)
      {
        if (i > 0)
        {
          csvLine.Append(fieldSeparator);
        }

        csvLine.Append(ToCsvString(columns[i]));
      }

      streamWriter.WriteLine(csvLine.ToString());
    }
  }

}
