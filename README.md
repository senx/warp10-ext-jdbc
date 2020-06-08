# JDBC WarpScript Extension

The JDBC WarpScript extension allows to retrieve data from a database for which a [`JDBC`]() Driver exists.

Updating data in the database is not possible using this extension.

# Conversion from SQL results to Geo Time Series

Geo Time Series have metadata and data points. For each row, the `SQLEXEC` function will extract the timestamp, classes and labels from the various columns of each rows.

The way columns are interpreted is determined by the parameters passed to `SQLEXEC`.

Please refer to the documentation of the `SQLEXEC` function for detailed information on its usage.