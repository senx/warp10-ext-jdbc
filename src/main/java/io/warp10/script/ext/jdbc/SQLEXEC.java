//
//   Copyright 2018-2020  SenX S.A.S.
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.gts.GeoTimeSerie;
import io.warp10.continuum.store.thrift.data.Metadata;
import io.warp10.script.NamedWarpScriptFunction;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptStack;
import io.warp10.script.WarpScriptStackFunction;

/**
 * Executes an SQL statement and creates Geo Time Series with the results.
 *
 * @param url URL to use to connect to the DB
 * @param properties Map of key/value with properties to pass to the driver
 * @param sql SQL statement to execute
 * @param[TOP] List of timestamp + value fields, the others will be considered labels. The first field is the timestamp.
 */
public class SQLEXEC extends NamedWarpScriptFunction implements WarpScriptStackFunction {
  
  public SQLEXEC(String name) {
    super(name);
  }
  
  @Override
  public Object apply(WarpScriptStack stack) throws WarpScriptException {
    Object top = stack.pop();
    
    if (!(top instanceof List)) {
      throw new WarpScriptException(getName()+ " expected a list of value fields.");
    }
    
    List<Object> values = (List<Object>) top;
    
    top = stack.pop();
        
    if (!(top instanceof String)) {
      throw new WarpScriptException(getName() + " expects a SQL statement.");
    }
    
    String sql = (String) top;
    
    top = stack.pop();
    
    if (!(top instanceof Map)) {
      throw new WarpScriptException(getName() + " expected a property map before the SQL statement.");
    }
    
    Properties props = new Properties();
    
    for(Entry<Object,Object> entry: ((Map<Object,Object>) top).entrySet()) {
      props.put(entry.getKey(), entry.getValue());
    }
    
    String url = stack.pop().toString();
    
    Connection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    
    try {
      conn = DriverManager.getConnection(url, props);
      
      stmt = conn.createStatement();  

      rs = stmt.executeQuery(sql);
      ResultSetMetaData rsmd = rs.getMetaData();

      int n = rsmd.getColumnCount();
      
      Map<String,Integer> colidx = new HashMap<String,Integer>();
      Map<String,Integer> labelidx = new HashMap<String,Integer>();

      int tsidx = -1;
      
      for (int i = 1; i <= n; i++) {
        if (values.get(0).equals(rsmd.getColumnLabel(i))) {
          tsidx = i;
        } else if (values.contains(rsmd.getColumnLabel(i))) {
          colidx.put(rsmd.getColumnLabel(i), i);
        } else {
          labelidx.put(rsmd.getColumnLabel(i), i);
        }
      }
      
      if (-1 == tsidx) {
        throw new WarpScriptException(getName() + " did not find timestamp column '" + values.get(0) + "'.");
      }
            
      Map<Metadata,GeoTimeSerie> gts = new HashMap<Metadata, GeoTimeSerie>();
      
      Map<String,String> labels = new HashMap<String,String>();
      
      while(rs.next()) {
        // Populate labels
        labels.clear();
        for (Entry<String,Integer> entry: labelidx.entrySet()) {
          String val = rs.getString(entry.getValue());
          if (!rs.wasNull()) {
            labels.put(entry.getKey(), val);
          }
        }
        
        // Now read timestamp
        long ts = rs.getLong(tsidx);
      
        // Skip rows with no timestamp
        if (rs.wasNull()) {
          continue;
        }
        
        // Now read values
        for (Entry<String,Integer> entry: colidx.entrySet()) {
          
          Object val = rs.getObject(entry.getValue());
          
          if (rs.wasNull()) {
            continue;
          }
          
          Metadata meta = new Metadata();
          meta.setLabels(new HashMap<String,String>(labels));
          meta.setName(entry.getKey());
          
          GeoTimeSerie g = gts.get(meta);
          if (null == g) {
            g = new GeoTimeSerie();
            g.setMetadata(meta);
            gts.put(meta, g);
          }
          
          GTSHelper.setValue(g, ts, val);
        }
      }
     
      List<GeoTimeSerie> results = new ArrayList<GeoTimeSerie>();
      
      results.addAll(gts.values());
      
      stack.push(results);
    } catch (SQLException sqle) {
      throw new WarpScriptException(getName() + " caught a SQL Exception.", sqle);
    } finally {
      if (null != rs) {
        try { rs.close(); } catch (SQLException sqle) {}
      }
      if (null != stmt) {
        try { stmt.close(); } catch (SQLException sqle) {}
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
}
