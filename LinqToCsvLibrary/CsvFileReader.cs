﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace LinqToCsvLibrary
{
  public class CsvFileReader<T> : CsvFile, IEnumerable<T>, IEnumerator<T>
   where T : new()
  {
    private readonly Dictionary<Type, List<Action<T, String>>> allSetters = new Dictionary<Type, List<Action<T, String>>>();
    private string[] columns;
    private char curChar;
    private int len;
    private string line;
    private int pos;
    private T record;
    private readonly char fieldSeparator;
    private readonly TextReader textReader;
    private readonly char textQualifier;
    private readonly StringBuilder parseFieldResult = new StringBuilder();

    public CsvFileReader(CsvSource csvSource)
        : this(csvSource, null)
    {
    }

    public CsvFileReader(CsvSource csvSource, CsvDefinition csvDefinition)
    {
      var streamReader = csvSource.TextReader as StreamReader;
      if (streamReader != null)
      {
        BaseStream = streamReader.BaseStream;
      }

      if (csvDefinition == null)
      {
        csvDefinition = DefaultCsvDefinition;
      }

      fieldSeparator = csvDefinition.FieldSeparator;
      textQualifier = csvDefinition.TextQualifier;

      textReader = csvSource.TextReader;// new FileStream(csvSource.TextReader, FileMode.Open);
      ReadHeader(csvDefinition.Header);
    }

    public T Current
    {
      get { return record; }
    }

    public bool Eof
    {
      get { return line == null; }
    }

    object IEnumerator.Current
    {
      get { return Current; }
    }

    protected override void Dispose(bool disposing)
    {
      if (disposing)
      {
        // free managed resources
        textReader.Dispose();
      }
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
      return GetEnumerator();
    }

    public IEnumerator<T> GetEnumerator()
    {
      return this;
    }

    public bool MoveNext()
    {
      ReadNextLine();
      if (line == null && (line = textReader.ReadLine()) == null)
      {
        record = default(T);
      }
      else
      {
        record = new T();
        Type recordType = typeof(T);
        List<Action<T, String>> setters;
        if (!allSetters.TryGetValue(recordType, out setters))
        {
          setters = CreateSetters();
          allSetters[recordType] = setters;
        }

        var fieldValues = new string[setters.Count];
        for (int i = 0; i < setters.Count; i++)
        {
          fieldValues[i] = ParseField();
          if (curChar == fieldSeparator)
          {
            NextChar();
          }
          else
          {
            break;
          }
        }
        for (int i = 0; i < setters.Count; i++)
        {
          var setter = setters[i];
          if (setter != null)
          {
            setter(record, fieldValues[i]);
          }
        }
      }

      return (record != null);
    }

    public void Reset()
    {
      throw new NotImplementedException("Cannot reset CsvFileReader enumeration.");
    }

    private static Action<T, string> EmitSetValueAction(MemberInfo mi, Func<string, object> func)
    {
      ParameterExpression paramExpObj = Expression.Parameter(typeof(object), "obj");
      ParameterExpression paramExpT = Expression.Parameter(typeof(T), "instance");
      {
        var pi = mi as PropertyInfo;
        if (pi != null)
        {
#if !NET_3_5
          if (CsvFile.UseLambdas)
          {
            var callExpr = Expression.Call(
                paramExpT,
                pi.GetSetMethod(),
                Expression.ConvertChecked(paramExpObj, pi.PropertyType));
            var setter = Expression.Lambda<Action<T, object>>(
                callExpr,
                paramExpT,
                paramExpObj).Compile();
            return (o, s) => setter(o, func(s));
          }
#endif
          return (o, v) => pi.SetValue(o, (object)func(v), null);
        }
      }
      {
        var fi = mi as FieldInfo;
        if (fi != null)
        {
#if !NET_3_5
          if (CsvFile.UseLambdas)
          {
            //ParameterExpression valueExp = Expression.Parameter(typeof(string), "value");
            var valueExp = Expression.ConvertChecked(paramExpObj, fi.FieldType);

            // Expression.Property can be used here as well
            MemberExpression fieldExp = Expression.Field(paramExpT, fi);
            BinaryExpression assignExp = Expression.Assign(fieldExp, valueExp);

            var setter = Expression.Lambda<Action<T, object>>
                (assignExp, paramExpT, paramExpObj).Compile();

            return (o, s) => setter(o, func(s));
          }
#endif
          return ((o, v) => fi.SetValue(o, func(v)));
        }
      }

      throw new NotImplementedException();
    }

    private static Action<T, string> FindSetter(string c, bool staticMember)
    {
      var flags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.IgnoreCase | (staticMember ? BindingFlags.Static : BindingFlags.Instance);
      Action<T, string> action = null;
      PropertyInfo pi = typeof(T).GetProperty(c, flags);
      if (pi != null)
      {
        var pFunc = StringToObject(pi.PropertyType);
        action = EmitSetValueAction(pi, pFunc);
      }

      FieldInfo fi = typeof(T).GetField(c, flags);
      if (fi != null)
      {
        var fFunc = StringToObject(fi.FieldType);
        action = EmitSetValueAction(fi, fFunc);
      }

      return action;
    }

    private static Func<string, object> StringToObject(Type propertyType)
    {
      if (propertyType == typeof(string))
      {
        return (s) => s ?? String.Empty;
      }
      else if (propertyType == typeof(Int32))
      {
        return (s) => String.IsNullOrEmpty(s) ? 0 : Int32.Parse(s);
      }

      if (propertyType == typeof(DateTime))
      {
        return (s) => String.IsNullOrEmpty(s) ? DateTimeZero : DateTime.Parse(s);
      }
      else
      {
        throw new NotImplementedException();
      }
    }

    private List<Action<T, string>> CreateSetters()
    {
      var list = new List<Action<T, string>>();
      for (int i = 0; i < columns.Length; i++)
      {
        string columnName = columns[i];
        Action<T, string> action = null;
        if (columnName.IndexOf(' ') >= 0)
        {
          columnName = columnName.Replace(" ", "");
        }

        action = FindSetter(columnName, false) ?? FindSetter(columnName, true);

        list.Add(action);
      }

      return list;
    }

    private void NextChar()
    {
      if (pos < len)
      {
        pos++;
        curChar = pos < len ? line[pos] : '\0';
      }
    }

    private void ParseEndOfLine()
    {
      throw new NotImplementedException();
    }

    private string ParseField()
    {
      parseFieldResult.Length = 0;
      if (line == null || pos >= len)
      {
        return null;
      }

      while (curChar == ' ' || curChar == '\t')
      {
        NextChar();
      }

      if (curChar == textQualifier)
      {
        NextChar();
        while (curChar != 0)
        {
          if (curChar == textQualifier)
          {
            NextChar();
            if (curChar == textQualifier)
            {
              NextChar();
              parseFieldResult.Append(textQualifier);
            }
            else
            {
              return parseFieldResult.ToString();
            }
          }
          else if (curChar == '\0')
          {
            if (line == null)
            {
              return parseFieldResult.ToString();
            }

            ReadNextLine();
          }
          else
          {
            parseFieldResult.Append(curChar);
            NextChar();
          }
        }
      }
      else
      {
        while (curChar != 0 && curChar != fieldSeparator && curChar != '\r' && curChar != '\n')
        {
          parseFieldResult.Append(curChar);
          NextChar();
        }
      }

      return parseFieldResult.ToString();
    }

    private void ReadHeader(string header)
    {
      if (header == null)
      {
        ReadNextLine();
      }
      else
      {
        // we read the first line from the given header
        line = header;
        pos = -1;
        len = line.Length;
        NextChar();
      }

      var readColumns = new List<string>();
      string columnName;
      while ((columnName = ParseField()) != null)
      {
        readColumns.Add(columnName);
        if (curChar == fieldSeparator)
        {
          NextChar();
        }
        else
        {
          break;
        }
      }

      columns = readColumns.ToArray();
    }

    private void ReadNextLine()
    {
      line = textReader.ReadLine();
      pos = -1;
      if (line == null)
      {
        len = 0;
        curChar = '\0';
      }
      else
      {
        len = line.Length;
        NextChar();
      }
    }
  }
}