/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.yahoo.ycsb.measurements.Measurements;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class DBWrapper extends DB
{
    DB _db;
    Measurements _measurements;
    String readConcern;
    String readPreference;
    String writeConcern;
    String operationType;
    Properties props;


    public DBWrapper(DB db)
    {
        _db=db;
        props = _db.getProperties();
        _measurements=Measurements.getMeasurements();
        _measurements.init();
    }

    /**
     * Set the properties for this DB.
     */
    public void setProperties(Properties p)
    {
        _db.setProperties(p);
    }

    /**
     * Get the set of properties for this DB.
     */
    public Properties getProperties()
    {
        return _db.getProperties();
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws DBException
    {
        _db.init();
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws DBException
    {
        long st=System.nanoTime();
        _db.cleanup();
        long en=System.nanoTime();
        _measurements.measure("CLEANUP", (int)((en-st)/1000));
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error
     */
    public int read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result)
    {
        long st=System.nanoTime();
        int res=_db.read(table,key,fields,result);
        long en=System.nanoTime();
        readPreference = props.getProperty("mongodb.readPreference", "primary").toLowerCase();
        readConcern = props.getProperty("mongodb.readConcern", "local").toLowerCase();
//        if (readPreference == null) {
//            // Example: Setting a default, or you can choose to skip handling
//            readPreference = "primary"; // Default readPreference if not specified
//        }
//        switch (readPreference) {
//            case "primary":
//                _measurements.measure("READ CONSISTENT",(int)((en-st)/1000));
//                _measurements.reportReturnCode("READ CONSISTENT",res);
//                break;
//            case "secondary":
//                _measurements.measure("READ REPLICA",(int)((en-st)/1000));
//                _measurements.reportReturnCode("READ REPLICA",res);
//                break;
//            case "majority":
//                _measurements.measure("READ MAJORITY",(int)((en-st)/1000));
//                _measurements.reportReturnCode("READ MAJORITY",res);
//                break;
//            default:
//                System.err.println("ERROR: Invalid readPreference: '"
//                        + readPreference
//                        + "'. Must be [ primary | secondary | majority ]");
//                System.exit(1);
//        }
//
//        return res;
//    }
        if ("primary".equals(readPreference)) {
            operationType = "READ PRIMARY";
            if ("majority".equals(readConcern)) {
                operationType = "READ MAJORITY";
            }
        } else if ("secondary".equals(readPreference)) {
            operationType = "READ SECONDARY";
        } else {
            System.err.println("ERROR: Invalid readPreference/readConcern: '" + readPreference + "'. Must be [ primary | secondary ] '" + readConcern + "'. Must be [ local | majority ]");
            System.exit(1);
        }

        // Measure and report based on operation type
        _measurements.measure(operationType, (int) ((en - st) / 1000));
        _measurements.reportReturnCode(operationType, res);

        return res;
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result)
    {
        long st=System.nanoTime();
        int res=_db.scan(table,startkey,recordcount,fields,result);
        long en=System.nanoTime();

        readPreference = props.getProperty("mongodb.readPreference", "primary").toLowerCase();
        readConcern = props.getProperty("mongodb.readConcern", "local").toLowerCase();
//        switch (readPreference) {
//            case "primary":
//                _measurements.measure("SCAN CONSISTENT", (int) ((en - st) / 1000));
//                _measurements.reportReturnCode("SCAN CONSISTENT", res);
//                break;
//            case "secondary":
//                _measurements.measure("SCAN REPLICA", (int) ((en - st) / 1000));
//                _measurements.reportReturnCode("SCAN REPLICA", res);
//                break;
//            case "majority":
//                _measurements.measure("SCAN MAJORITY", (int) ((en - st) / 1000));
//                _measurements.reportReturnCode("SCAN MAJORITY", res);
//                break;
//            default:
//                System.err.println("ERROR: Invalid readPreference: '"
//                        + readPreference
//                        + "'. Must be [ primary | secondary | majority ]");
//                System.exit(1);
//        }
//        return res;

        if ("primary".equals(readPreference)) {
            operationType = "SCAN PRIMARY";
            if ("majority".equals(readConcern)) {
                operationType = "SCAN MAJORITY";
            }
        } else if ("secondary".equals(readPreference)) {
            operationType = "SCAN SECONDARY";
        } else {
            System.err.println("ERROR: Invalid readPreference/readConcern: '" + readPreference + "'. Must be [ primary | secondary ] '" + readConcern + "'. Must be [ local | majority ]");
            System.exit(1);
        }

        // Measure and report based on operation type
        _measurements.measure(operationType, (int) ((en - st) / 1000));
        _measurements.reportReturnCode(operationType, res);

        return res;
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int update(String table, String key, HashMap<String,ByteIterator> values)
    {
        long st=System.nanoTime();
        int res=_db.update(table,key,values);
        long en=System.nanoTime();

        writeConcern = props.getProperty("mongodb.writeConcern");
        if (writeConcern == null) {
            // If writeConcern is not specified, use a default value or skip setting.
            // Example: Assuming 'acknowledged' as default:
            writeConcern = "acknowledged"; // Or simply skip setting if that's preferable
        }
        switch (writeConcern) {
            case "unacknowledged":
                _measurements.measure("UPDATE NONE",(int)((en-st)/1000));
                _measurements.reportReturnCode("UPDATE NONE",res);
                break;
            case "acknowledged":
                _measurements.measure("UPDATE ONE",(int)((en-st)/1000));
                _measurements.reportReturnCode("UPDATE ONE",res);
                break;
            case "majority":
                _measurements.measure("UPDATE MAJORITY",(int)((en-st)/1000));
                _measurements.reportReturnCode("UPDATE MAJORITY",res);
                break;
            case "replica_acknowledged":
                _measurements.measure("UPDATE ALL",(int)((en-st)/1000));
                _measurements.reportReturnCode("UPDATE ALL",res);
                break;
            default:
                System.err.println("ERROR: Invalid writeConcern: '"
                        + writeConcern
                        + "'. Must be [ unacknowledged | acknowledged | majority | replica_acknowledged ]");
                System.exit(1);
        }
        return res;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error
     */
    public int insert(String table, String key, HashMap<String,ByteIterator> values)
    {
        long st=System.nanoTime();
        int res=_db.insert(table,key,values);
        long en=System.nanoTime();
        if (props == null) {
            System.err.println("Properties (props) is not initialized.");
            return res; // Or consider throwing an exception or initializing props here
        }
        writeConcern = props.getProperty("mongodb.writeConcern");
        // Ensure writeConcern is not null
        if (writeConcern == null) {
            System.err.println("writeConcern property is not set.");
            return res; // Handle the error as appropriate
        }
        switch (writeConcern) {
            case "unacknowledged":
                _measurements.measure("INSERT NONE", (int) ((en - st) / 1000));
                _measurements.reportReturnCode("INSERT NONE", res);
                break;
            case "acknowledged":
                _measurements.measure("INSERT ONE", (int) ((en - st) / 1000));
                _measurements.reportReturnCode("INSERT ONE", res);
                break;
            case "majority":
                _measurements.measure("INSERT MAJORITY", (int) ((en - st) / 1000));
                _measurements.reportReturnCode("INSERT MAJORITY", res);
                break;
            case "replica_acknowledged":
                _measurements.measure("INSERT ALL", (int) ((en - st) / 1000));
                _measurements.reportReturnCode("INSERT ALL", res);
                break;
            default:
                System.err.println("ERROR: Invalid writeConcern: '"
                        + writeConcern
                        + "'. Must be [ unacknowledged | acknowledged | majority | replica_acknowledged ]");
                System.exit(1);
        }
        return res;
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
    public int delete(String table, String key)
    {
        long st=System.nanoTime();
        int res=_db.delete(table,key);
        long en=System.nanoTime();
        writeConcern = props.getProperty("mongodb.writeConcern");
        switch (writeConcern) {
            case "unacknowledged":
                _measurements.measure("DELETE NONE", (int) ((en - st) / 1000));
                _measurements.reportReturnCode("DELETE NONE", res);
                break;
            case "acknowledged":
                _measurements.measure("DELETE ONE", (int) ((en - st) / 1000));
                _measurements.reportReturnCode("DELETE ONE", res);
                break;
            case "majority":
                _measurements.measure("DELETE MAJORITY", (int) ((en - st) / 1000));
                _measurements.reportReturnCode("DELETE MAJORITY", res);
                break;
            case "replica_acknowledged":
                _measurements.measure("DELETE ALL", (int) ((en - st) / 1000));
                _measurements.reportReturnCode("DELETE ALL", res);
                break;
            default:
                System.err.println("ERROR: Invalid writeConcern: '"
                        + writeConcern
                        + "'. Must be [ unacknowledged | acknowledged | majority | replica_acknowledged ]");
                System.exit(1);
        }
        return res;
    }
}
