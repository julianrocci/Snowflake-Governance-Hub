Extraction:
EXTRACT(<part> FROM <date>) or DATE_PART(...): Pulls a specific component ( Day of week, Year).

Truncation:
DATE_TRUNC('<part>', <date>): Rounds the date down to the beginning of the specified part ( truncating to MONTH returns the 1st of that month).

Conversion:
TO_DATE() / TO_TIMESTAMP(): Converts strings to temporal types.

Formatting:
TRY_TO_DATE(): Safer version that returns NULL instead of failing if the format is incorrect.

Date Arithmetic (Add/Diff)

DATEADD(<part>, <value>, <date>): Adds an interval to a date.

DATEDIFF(<part>, <start>, <end>): Calculates the difference between two dates.

<part>: Can be YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, etc.