//
//   Copyright 2018  SenX S.A.S.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import io.warp10.WarpConfig;
import io.warp10.warp.sdk.WarpScriptExtension;

public class JDBCWarpScriptExtension extends WarpScriptExtension {
  
  private static final Map<String,Object> functions;
  
  private static final String JDBC_DRIVER_PREFIX = "jdbc.driver.";
  
  static {
    //
    // Load JDBC drivers from configuration
    //
    
    Properties props = WarpConfig.getProperties();

    for (Object prop: props.keySet()) {
      if (prop.toString().startsWith(JDBC_DRIVER_PREFIX)) {
        try {
          Class.forName(props.getProperty(prop.toString()));
        } catch (ClassNotFoundException cnfe) {
          throw new RuntimeException("Unable to load JDBC driver '" + prop + "'.", cnfe);
        }
      }
    }
    
    functions = new HashMap<String, Object>();
    
    functions.put("SQLEXEC", new SQLEXEC("SQLEXEC"));
  }
  
  @Override
  public Map<String, Object> getFunctions() {
    return functions;
  }
}
