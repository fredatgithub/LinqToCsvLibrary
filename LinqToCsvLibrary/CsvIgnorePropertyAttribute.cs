using System;

namespace LinqToCsvLibrary
{
  public class CsvIgnorePropertyAttribute : Attribute
  {
    public override string ToString()
    {
      return "Ignore Property";
    }
  }
}