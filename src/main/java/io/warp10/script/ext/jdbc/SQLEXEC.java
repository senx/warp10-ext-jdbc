//
//   Copyright 2018-2021  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
package io.warp10.script.ext.jdbc;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.Constants;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Executes an SQL statement and creates Geo Time Series with the results.
 */
public class SQLEXEC extends NamedWarpScriptFunction implements WarpScriptStackFunction {

  // Allows the convertion of SQL timestamps without TZ to consider them as being UTC instead of local TZ.
  // Not static because non-thread safe.
  private final Calendar gmtCalendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

  public SQLEXEC(String name) {
    super(name);
  }

  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    //
    // List of column names for automatic GTS conversion
    //
    Object top = stack.pop();

    if (null != top && !(top instanceof List)) {
      throw new WarpScriptException(getName() + " expected a list of value fields or NULL.");
    }

    List<Object> valueNames = (List<Object>) top;

    //
    // SQL statement
    //
    top = stack.pop();

    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a SQL statement.");
    }

    String sql = (String) top;

    //
    // Optional list of list of values for the prepared statement
    //
    top = stack.pop();

    List prepValues = null;

    if (top instanceof List) {
      prepValues = (List) top;
      top = stack.pop();
    }

    //
    // Map of connection properties
    //
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expected a property map before the SQL statement.");
    }

    Properties props = new Properties();

    for (Entry<Object, Object> entry: ((Map<Object, Object>) top).entrySet()) {
      if (!(entry.getKey() instanceof String) || !(entry.getValue() instanceof String)) {
        throw new WarpScriptException(getName() + " expects the properties MAP to have STRING keys and STRING values.");
      }
      props.setProperty((String) entry.getKey(), (String) entry.getValue());
    }

    //
    // URL of the DB
    //
    String url = stack.pop().toString();

    //
    // JDBC objects
    //

    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;

    //
    // Do the magic
    //

    try {
      conn = DriverManager.getConnection(url, props);

      //
      // Simply execute if there is no value to insert (or they are in the statement) or use a prepared statement
      // and add all the values to this statement.
      //

      if (null != prepValues) {
        stmt = conn.prepareStatement(sql);
        PreparedStatement pstmt = (PreparedStatement) stmt;

        for (Object val: prepValues) {
          if (!(val instanceof List)) {
            throw new WarpScriptException(getName() + " expects values to be inserted to be LISTs.");
          }
          List tuple = (List) val;
          for (int i = 0; i < tuple.size(); i++) {
            pstmt.setObject(i + 1, tuple.get(i));
          }
          pstmt.addBatch();
        }

        long[] counts = pstmt.executeLargeBatch();

        ArrayList<Object> countsList = new ArrayList<Object>(counts.length);
        for (long c: counts) {
          countsList.add(c);
        }

        stack.push(countsList);
      } else {
        stmt = conn.createStatement();

        boolean isResultSet = stmt.execute(sql);
        long updateCount = stmt.getLargeUpdateCount();

        if (isResultSet) {
          rs = stmt.getResultSet();

          ResultSetMetaData rsmd = rs.getMetaData();

          int n = rsmd.getColumnCount();

          if (null == valueNames) {
            int[] types = new int[n + 1];
            for (int i = 1; i <= n; i++) {
              types[i] = rsmd.getColumnType(i);
            }


            ArrayList<ArrayList<Object>> result = new ArrayList<ArrayList<Object>>();

            while (rs.next()) {
              ArrayList<Object> row = new ArrayList<>(n);
              for (int i = 1; i <= n; i++) {
                try {
                  Object o = sqlToWarpScript(types[i], rs, i, gmtCalendar);
                  row.add(o);
                } catch (WarpScriptException | SQLException e) {
                  throw new WarpScriptException(getName() + " couldn't convert the data.", e);
                }
              }
              result.add(row);
            }

            stack.push(result);
          } else {
            Map<String, Integer> colidx = new HashMap<String, Integer>();
            Map<String, Integer> labelidx = new HashMap<String, Integer>();

            int tsidx = -1;

            for (int i = 1; i <= n; i++) {
              if (valueNames.get(0).equals(rsmd.getColumnLabel(i))) {
                tsidx = i;
              } else if (valueNames.contains(rsmd.getColumnLabel(i))) {
                colidx.put(rsmd.getColumnLabel(i), i);
              } else {
                labelidx.put(rsmd.getColumnLabel(i), i);
              }
            }

            if (-1 == tsidx) {
              throw new WarpScriptException(getName() + " did not find timestamp column '" + valueNames.get(0) + "'.");
            }

            int tsType = rsmd.getColumnType(tsidx);

            Map<Metadata, GeoTimeSerie> mgts = new HashMap<Metadata, GeoTimeSerie>();

            Map<String, String> labels = new HashMap<String, String>();

            while (rs.next()) {
              // Populate labels
              labels.clear();
              for (Entry<String, Integer> entry: labelidx.entrySet()) {
                String val = rs.getString(entry.getValue());
                if (!rs.wasNull()) {
                  labels.put(entry.getKey(), val);
                }
              }

              Object o;

              try {
                o = sqlToWarpScript(tsType, rs, tsidx, gmtCalendar);
              } catch (WarpScriptException | SQLException e) {
                throw new WarpScriptException(getName() + " is given an invalid column type for timestamp.", e);
              }

              if (!(o instanceof Number)) {
                throw new WarpScriptException(getName() + " is given an invalid column type for timestamp.");
              }

              long ts = ((Number) o).longValue();

              // Skip rows with no timestamp
              if (rs.wasNull()) {
                continue;
              }

              // Now read values
              for (Entry<String, Integer> entry: colidx.entrySet()) {

                Object val = rs.getObject(entry.getValue());

                if (rs.wasNull()) {
                  continue;
                }

                Metadata meta = new Metadata();
                meta.setLabels(new HashMap<String, String>(labels));
                meta.setName(entry.getKey());

                GeoTimeSerie gts = mgts.get(meta);
                if (null == gts) {
                  gts = new GeoTimeSerie();
                  gts.setMetadata(meta);
                  mgts.put(meta, gts);
                }

                GTSHelper.setValue(gts, ts, val);
              }
            }

            List<GeoTimeSerie> lgts = new ArrayList<GeoTimeSerie>();

            lgts.addAll(mgts.values());

            stack.push(lgts);
          }
        } else {
          // Encapsulate the result in a List for all the result to be of type List.
          ArrayList<Object> count = new ArrayList<Object>(1);
          count.add(updateCount);

          stack.push(count);
        }
      }
    } catch (SQLException sqle) {
      throw new WarpScriptException(getName() + " caught a SQL Exception.", sqle);
    } finally {
      if (null != rs) {
        try {
          rs.close();
        } catch (SQLException sqle) {
        }
      }
      if (null != stmt) {
        try {
          stmt.close();
        } catch (SQLException sqle) {
        }
      }
      if (null != conn) {
        try {
          conn.close();
        } catch (SQLException sqle) {
          throw new WarpScriptException(getName() + " caught a SQL Exception while closing the connection.", sqle);
        }
      }
    }

    return stack;
  }

  /**
   * Converts a SQL data type in a ResultSet to a WarpScript data type.
   *
   * @param type        The SQL type, list can be found in java.sql.Types.
   * @param rs          The ResultSet to get the data from.
   * @param colIndex    The column to get the data from, 1-indexed.
   * @param gmtCalendar A Calendar with GMT timezone to convert to GMT times/timestamps without timezone.
   * @return An object which is accepted in WarpScript (Long, Double, String, Boolean, byte[], BigDecimal, null).
   * Times and timestamps are converted to long timestamps in platform time unit.
   * @throws WarpScriptException If the type cannot be converted.
   * @throws SQLException        If there is a problem during conversion.
   */
  private static Object sqlToWarpScript(int type, ResultSet rs, int colIndex, Calendar gmtCalendar) throws WarpScriptException, SQLException {
    Object o;

    if (null == rs.getObject(colIndex)) {
      return null;
    }

    switch (type) {
      case Types.BIT:
      case Types.BOOLEAN:
        o = rs.getBoolean(colIndex);
        break;
      case Types.TINYINT:
      case Types.SMALLINT:
      case Types.INTEGER:
        o = rs.getLong(colIndex);
        break;
      case Types.BIGINT:
        o = rs.getBigDecimal(colIndex);
        break;
      case Types.FLOAT:
      case Types.REAL:
      case Types.DOUBLE:
      case Types.NUMERIC:
      case Types.DECIMAL:
        o = rs.getDouble(colIndex);
        break;
      case Types.CHAR:
      case Types.VARCHAR:
      case Types.LONGVARCHAR:
      case Types.NCHAR:
      case Types.NVARCHAR:
      case Types.LONGNVARCHAR:
      case Types.CLOB:
        o = rs.getString(colIndex);
        break;
      case Types.TIMESTAMP:
        Timestamp timestamp = rs.getTimestamp(colIndex, gmtCalendar);
        o = (timestamp.getTime() / 1000) * Constants.TIME_UNITS_PER_S + timestamp.getNanos() / Constants.NS_PER_TIME_UNIT;
        break;
      case Types.TIMESTAMP_WITH_TIMEZONE:
        Timestamp timestampTZ = rs.getTimestamp(colIndex);
        o = (timestampTZ.getTime() / 1000) * Constants.TIME_UNITS_PER_S + timestampTZ.getNanos() / Constants.NS_PER_TIME_UNIT;
        break;
      case Types.DATE:
        Date date = rs.getDate(colIndex, gmtCalendar);
        o = (date.getTime() / 1000) * Constants.TIME_UNITS_PER_S;
        break;
      case Types.TIME:
        Time time = rs.getTime(colIndex, gmtCalendar);
        o = (time.getTime() / 1000) * Constants.TIME_UNITS_PER_S;
        break;
      case Types.TIME_WITH_TIMEZONE:
        Time timeTZ = rs.getTime(colIndex);
        o = (timeTZ.getTime() / 1000) * Constants.TIME_UNITS_PER_S;
        break;
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        o = rs.getBytes(colIndex);
        break;
      case Types.NULL:
        o = null;
        break;
      // Not handled: Types.OTHER, Types.JAVA_OBJECT, Types.DISTINCT, Types.STRUCT, Types.ARRAY, Types.REF, Types.DATALINK, Types.ROWID, Types.NCLOB, Types.SQLXML, Types.REF_CURSOR = 2012;
      default:
        throw new WarpScriptException("Got unhandled type " + type + ".");
    }

    return o;
  }
}
