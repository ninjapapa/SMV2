# Smv Schema

Smv Schema is specifically for CSV file read and write.

## Schema Definition
Because CSV files do not describe the data, the user must supply a schema definition that describes the set of columns and their type.  The schema file consists of CSV attributes and field definitions with one field definition per line.  The field definition consists of the field name and the field type.  The file may also contain blank lines and comments that start with "//" or "#".
For example:
```
# CSV attributes
@has-header = true
@delimiter = |
# schema for input
acct_id: String;  # this is the id
user_id: String;
amt: Double;  // transaction amount!
income: Decimal[10];
```

## CSV attributes
The schema file can specify the CSV attributes (delimiter, quote char, and header).  All three attributes are optional and will default to (',', '"', true) respectively.
<table>
<tr>
<th>Key</th>
<th>Default</th>
<th>Description</th>
</tr>
<tr>
<td>has-header</td>
<td>false</td>
<td>Determine if CSV file has header.  Can only contain true/false</td>
</tr>
<tr>
<td>delimiter</td>
<td>,</td>
<td>CSV field delimiter/separator. For tab separated files, specify \t as the separator; For ";"
separated files, use "semicolon"</td>
</tr>
<tr>
<td>quote-char</td>
<td>"</td>
<td>character used to quote fields (only used if field contains characters that would confuse the parser). For NO-quote-char case use \0</td>
</tr>
<tr>
<td>timestampFormat</td>
<td>yyyy-MM-dd HH:mm:ss</td>
<td>File level timestamp format. Spark can't specify different timestamp format for different fields. If that is the case, need to read in as String and convert in code. </td>
</tr>
<tr>
<td>dateFormat</td>
<td>yyyy-MM-dd</td>
<td>File level Date format. Spark can't specify different date format for different fields. If that is the case, need to read in as String and convert in code. </td>
</tr>
</table>

Example schema file with special characters:

```
# CSV attributes
@has-header = true
@quote-char = \0
@delimiter = \t
# schema for input
acct_id: String;  # this is the id
user_id: String;
```

## userSchema
Alternatively, the schema can be specified by overriding the `userSchema` method, for example:
```Python
class acct_demo(smv.iomod.SmvCsvInputFile):
  ...
  def userSchema(self):
    return "acct_id:String;user_id:String;store_id:String;amt:Double;income:Decimal[10]"
```

## Supported schema types
### Native types
`Integer`, `Long`, `Float`, `Double`, `Boolean`, and `String` types correspond to their corresponding JVM type.

### Decimal type
The `Decimal` type can be used to hold a `BigDecimal` field value.  An optional precision and scale values can also supplied.  They default to 10 and 0 respectively if not defined (same as `BigDecimal`).
```
income: Decimal;
amt: Decimal[7,2];
other: Decimal[10];
```

### Timestamp type
The `Timestamp` type can be used to hold a date/timestamp field value.
An optional format string can be used when defining a field of type `timestamp`.
The field format is the standard [`java.text.SimpleDateFormat`](https://docs.oracle.com/javase/8/docs/api/java/text/SimpleDateFormat.html)

If a format string is not specified, it defaults to `"yyyy-MM-dd HH:mm:ss"`.
Please note the difference between `HH`(Hour in day (0-23)) and `hh`(Hour in am/pm (1-12))
```
@timestampFormat = yyyy-MM-dd HH:mm:ss
std_date: Timestamp;
```

### Date type
The `Date` type is similar to `Timestamp` without the time part.
An optional format string can be used.
If a format string is not specified, it defaults to `"yyyy-MM-dd"`
```
@dateFormat = yyyy-MM-dd
std_date: Date
evt_date: Date
```

