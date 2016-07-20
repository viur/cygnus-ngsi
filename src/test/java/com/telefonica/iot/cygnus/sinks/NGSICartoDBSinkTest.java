/**
 * Copyright 2016 Telefonica Investigación y Desarrollo, S.A.U
 *
 * This file is part of fiware-cygnus (FI-WARE project).
 *
 * fiware-cygnus is free software: you can redistribute it and/or modify it under the terms of the GNU Affero
 * General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your
 * option) any later version.
 * fiware-cygnus is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with fiware-cygnus. If not, see
 * http://www.gnu.org/licenses/.
 *
 * For those usages not covered by the GNU Affero General Public License please contact with iot_support at tid dot es
 */
package com.telefonica.iot.cygnus.sinks;

import com.google.gson.JsonPrimitive;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextAttribute;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextElement;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest.ContextMetadata;
import com.telefonica.iot.cygnus.sinks.NGSICartoDBSink.CartoDBAggregator;
import static com.telefonica.iot.cygnus.utils.CommonUtilsForTests.getTestTraceHead;
import com.telefonica.iot.cygnus.utils.NGSIConstants;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import org.apache.flume.Context;
import org.apache.flume.channel.MemoryChannel;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 *
 * @author frb
 */
public class NGSICartoDBSinkTest {
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    
    /**
     * Constructor.
     */
    public NGSICartoDBSinkTest() {
        LogManager.getRootLogger().setLevel(Level.FATAL);
    } // NGSICartoDBSinkTest
    
    /**
     * [NGSICartoDBSink.configure] -------- Independently of the configured value, enable_lowercase is always 'true'
     * by default.
     */
    @Test
    public void testConfigureEnableLowercaseAlwaysTrue() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                + "-------- Independently of the configured value, enable_lowercase is always 'true' by default");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = "false";
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        try {
            assertTrue(sink.enableLowercase);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "-  OK  - 'enable_lowercase=false' was configured, nevertheless it is always true by default");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "- FAIL - 'enable_lowercase=false' was configured and it is really false");
            throw e;
        } // try catch
    } // testConfigureEnableLowercaseAlwaysTrue
    
    /**
     * [NGSICartoDBSink.configure] -------- Configured 'flip_coordinates' cannot be different than 'true' or 'false'.
     */
    @Test
    public void testConfigureFlipCoordinatesOK() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                + "-------- Configured 'flip_coordinates' cannot be different than 'true' or 'false'");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = "falso"; // wrong value
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        try {
            assertTrue(sink.enableLowercase);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "-  OK  - 'flip_coordinates=falso' was detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "- FAIL - 'flip_coordinates=falso' was not detected");
            throw e;
        } // try catch
    } // testConfigureFlipCoordinatesOK
    
    /**
     * [NGSICartoDBSink.configure] -------- Configured 'enable_raw' cannot be different than 'true' or 'false'.
     */
    @Test
    public void testConfigureEnableRawOK() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                + "-------- Configured 'enable_raw' cannot be different than 'true' or 'false'");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default value
        String enableRaw = "falso"; // wrong value
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        try {
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "-  OK  - 'enable_raw=falso' was detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "- FAIL - 'enable_raw=falso' was not detected");
            throw e;
        } // try catch
    } // testConfigureEnableRawOK
    
    /**
     * [NGSICartoDBSink.configure] -------- Configured 'enable_distance' cannot be different than 'true' or 'false'.
     */
    @Test
    public void testConfigureEnableDistanceOK() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                + "-------- Configured 'enable_distance' cannot be different than 'true' or 'false'");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String keysConfFile = "/keys.conf";
        String enableDistance = "falso"; // wrong value
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        try {
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "-  OK  - 'enable_distance=falso' was detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "- FAIL - 'enable_distance=falso' was not detected");
            throw e;
        } // try catch
    } // testConfigureEnableDistanceOK
    
    /**
     * [NGSICartoDBSink.configure] -------- Configured 'keys_conf_file' cannot be empty.
     */
    @Test
    public void testConfigureKeysConfFileOK() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                + "-------- Configured 'keys_conf_file' cannot be empty");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default_one
        String keysConfFile = null; // empty file
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        try {
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "-  OK  - Empty 'keys_conf_file' was detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "- FAIL - Empty 'keys_conf_file' was not detected");
            throw e;
        } // try catch
    } // testConfigureKeysConfFileOK
    
    /**
     * [NGSICartoDBSink.start] -------- When started, a CartoDB backend is created from a valid keys file.
     */
    @Test
    public void testStartBackendCreated() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                + "-------- When started, a CartoDB backend is created from a valid keys file");
        File file;
        
        try {
            file = folder.newFile("keys.conf");
            PrintWriter out = new PrintWriter(file);
            out.println("{\"cartodb_keys\":[{\"username\":\"frb\",\"endpoint\":\"http://frb.com\",\"key\":\"xxx\"}]}");
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - There was some problem when mocking the keys file");
            throw new AssertionError(e.getMessage());
        } // try catch
        
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = file.getAbsolutePath();
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        sink.setChannel(new MemoryChannel());
        sink.start();
        
        try {
            assertTrue(sink.getBackends() != null);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "-  OK  - A CartoDB backend has been created");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - A CartoDB backend has not been created");
            throw e;
        } // try catch
    } // testStartBackendCreated
    
    /**
     * [NGSICartoDBSink.start] -------- Username field must appear within an entry in the keys file.
     */
    @Test
    public void testStartUsernameNotNullKeysFile() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                + "-------- Username field must appear within an entry in the keys file");
        File file;
        
        try {
            file = folder.newFile("keys.conf");
            PrintWriter out = new PrintWriter(file);
            out.println("{\"cartodb_keys\":[{\"endpoint\":\"http://frb.com\",\"key\":\"xxx\"}]}");
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - There was some problem when mocking the keys file");
            throw new AssertionError(e.getMessage());
        } // try catch
        
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = file.getAbsolutePath();
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        sink.setChannel(new MemoryChannel());
        sink.start();
        
        try {
            assertEquals(null, sink.getBackends());
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "-  OK  - Null username has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - Null username has not been detected");
            throw e;
        } // try catch
    } // testStartUsernameNotNullKeysFile
    
    /**
     * [NGSICartoDBSink.start] -------- Username within an entry in the keys file cannot be empty.
     */
    @Test
    public void testStartUsernameNotEmptyKeysFile() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                + "-------- Username within an entry in the keys file cannot be empty");
        File file;
        
        try {
            file = folder.newFile("keys.conf");
            PrintWriter out = new PrintWriter(file);
            out.println("{\"cartodb_keys\":[{\"username\":\"\",\"endpoint\":\"http://frb.com\",\"key\":\"xxx\"}]}");
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - There was some problem when mocking the keys file");
            throw new AssertionError(e.getMessage());
        } // try catch
        
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = file.getAbsolutePath();
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        sink.setChannel(new MemoryChannel());
        sink.start();
        
        try {
            assertEquals(null, sink.getBackends());
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "-  OK  - Empty username has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - Empty username has not been detected");
            throw e;
        } // try catch
    } // testStartUsernameNotEmptyKeysFile
    
    /**
     * [NGSICartoDBSink.configure] -------- Configured `data_model` cannot be different than `dm-by-service-path`
     * or `dm-by-entity`.
     */
    @Test
    public void testConfigureDataModelOK() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                + "-------- Configured `data_model` cannot be different than `dm-by-service-path` or `dm-by-entity`");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-service";
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        try {
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "-  OK  - 'data_model=dm-by-service' was detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "- FAIL - 'data_model=dm-by-service' was not detected");
            throw e;
        } // try catch
        
        dataModel = "dm-by-attribute";
        sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        try {
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "-  OK  - 'data_model=dm-by-attribute' was detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.configure]")
                    + "- FAIL - 'data_model=dm-by-attribute' was not detected");
            throw e;
        } // try catch
    } // testConfigureDataModelOK
    
    /**
     * [NGSICartoDBSink.start] -------- When started, a CartoDB backend is created.
     */
    @Test
    public void testStartEndpointNotNullKeysFile() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                + "-------- Endpoint field must appear within an entry in the keys file");
        File file;
        
        try {
            file = folder.newFile("keys.conf");
            PrintWriter out = new PrintWriter(file);
            out.println("{\"cartodb_keys\":[{\"username\":\"frb\",\"key\":\"xxx\"}]}");
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - There was some problem when mocking the keys file");
            throw new AssertionError(e.getMessage());
        } // try catch
        
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = file.getAbsolutePath();
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        sink.setChannel(new MemoryChannel());
        sink.start();
        
        try {
            assertEquals(null, sink.getBackends());
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "-  OK  - Null endpoint has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - Null endpoint has not been detected");
            throw e;
        } // try catch
    } // testStartEndpointNotNullKeysFile
    
    /**
     * [NGSICartoDBSink.start] -------- Endpoint within an entry in the keys file cannot be empty.
     */
    @Test
    public void testStartEndpointNotEmptyKeysFile() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                + "-------- Endpoint within an entry in the keys file cannot be empty");
        File file;
        
        try {
            file = folder.newFile("keys.conf");
            PrintWriter out = new PrintWriter(file);
            out.println("{\"cartodb_keys\":[{\"username\":\"frb\",\"endpoint\":\"\",\"key\":\"xxx\"}]}");
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - There was some problem when mocking the keys file");
            throw new AssertionError(e.getMessage());
        } // try catch
        
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = file.getAbsolutePath();
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        sink.setChannel(new MemoryChannel());
        sink.start();
        
        try {
            assertEquals(null, sink.getBackends());
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "-  OK  - Empty endpoint has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - Empty endpoint has not been detected");
            throw e;
        } // try catch
    } // testStartEndpointNotEmptyKeysFile
    
    /**
     * [NGSICartoDBSink.start] -------- Endpoint within an entry in the keys file must be a URI using the http or
     * https schema.
     */
    @Test
    public void testStartEndpointSchemaOKKeysFile() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                + "-------- Endpoint within an entry in the keys file must be a URI using the http or https schema");
        File file;
        
        try {
            file = folder.newFile("keys.conf");
            PrintWriter out = new PrintWriter(file);
            out.println("{\"cartodb_keys\":[{\"username\":\"frb\",\"endpoint\":\"htt://frb.com\",\"key\":\"xxx\"}]}");
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - There was some problem when mocking the keys file");
            throw new AssertionError(e.getMessage());
        } // try catch
        
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = file.getAbsolutePath();
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        sink.setChannel(new MemoryChannel());
        sink.start();
        
        try {
            assertEquals(null, sink.getBackends());
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "-  OK  - Invalid 'htt' schema in the endpoint has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - Invalid 'htt' schema in the endpoint has not been detected");
            throw e;
        } // try catch
    } // testStartEndpointSchemaOKKeysFile
    
    /**
     * [NGSICartoDBSink.start] -------- Key field must appear within an entry in the keys file.
     */
    @Test
    public void testStartKeyNotNullKeysFile() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                + "-------- Key field must appear within an entry in the keys file");
        File file;
        
        try {
            file = folder.newFile("keys.conf");
            PrintWriter out = new PrintWriter(file);
            out.println("{\"cartodb_keys\":[{\"username\":\"frb\",\"endpoint\":\"http://frb.com\"}]}");
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - There was some problem when mocking the keys file");
            throw new AssertionError(e.getMessage());
        } // try catch
        
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = file.getAbsolutePath();
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        sink.setChannel(new MemoryChannel());
        sink.start();
        
        try {
            assertEquals(null, sink.getBackends());
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "-  OK  - Null key has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - Null key has not been detected");
            throw e;
        } // try catch
    } // testStartKeyNotNullKeysFile
    
    /**
     * [NGSICartoDBSink.start] -------- Key within an entry in the keys file cannot be empty.
     */
    @Test
    public void testStartKeyNotEmptyKeysFile() {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                + "-------- Key within an entry in the keys file cannot be empty");
        File file;
        
        try {
            file = folder.newFile("keys.conf");
            PrintWriter out = new PrintWriter(file);
            out.println("{\"cartodb_keys\":[{\"username\":\"frb\",\"endpoint\":\"http://frb.com\",\"key\":\"\"}]}");
            out.flush();
            out.close();
        } catch (IOException e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - There was some problem when mocking the keys file");
            throw new AssertionError(e.getMessage());
        } // try catch
        
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = file.getAbsolutePath();
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        sink.setChannel(new MemoryChannel());
        sink.start();
        
        try {
            assertEquals(null, sink.getBackends());
            assertTrue(sink.invalidConfiguration);
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "-  OK  - Empty key has been detected");
        } catch (AssertionError e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.start]")
                    + "- FAIL - Empty key has not been detected");
            throw e;
        } // try catch
    } // testStartKeyNotEmptyKeysFile
    
    /**
     * [NGSICartoDBSink.buildSchemaName] -------- The schema name is equals to the notified/defaulted service.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildSchemaName() throws Exception {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.buildSchemaName]")
                + "-------- The schema name is equals to the notified/defaulted service");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = null; // default one
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        String service = "someService";
        
        try {
            String builtSchemaName = sink.buildSchemaName(service);
        
            try {
                assertEquals("someservice", builtSchemaName);
                System.out.println(getTestTraceHead("[NGSICartoDBSink.buildSchemaName]")
                        + "-  OK  - '" + builtSchemaName + "' is equals to the lower case of <service>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICartoDBSink.buildSchemaName]")
                        + "- FAIL - '" + builtSchemaName + "' is not equals to the lower case of <service>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.buildSchemaName]")
                    + "- FAIL - There was some problem when building the schema name");
            throw e;
        } // try catch
    } // testBuildSchemaName

    /**
     * [NGSICartoDBSink.buildTableName] -------- When a non root service-path is notified/defaulted and
     * data_model is 'dm-by-service-path' the CartoDB table name is the lower case of x002f<service-path>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildDBNameNonRootServicePathDataModelByServicePath() throws Exception {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                + "-------- When a non root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the CartoDB table name is the lower case of of x002f<service-path>");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-service-path";
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        String servicePath = "/somePath";
        String entity = null; // irrelevant for this test
        String attribute = null; // irrelevant for this test
        
        try {
            String builtTableName = sink.buildTableName(servicePath, entity, attribute);
        
            try {
                assertEquals("x002fsomepath", builtTableName);
                System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                        + "-  OK  - '" + builtTableName + "' is equals to the lower case of x002f<service-path>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                        + "- FAIL - '" + builtTableName + "' is not equals to the lower case of x002f<service-path>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildDBNameNonRootServicePathDataModelByServicePath
    
    /**
     * [NGSICartoDBSink.buildTableName] -------- When a non root service-path is notified/defaulted
     * and data_model is 'dm-by-entity' the CartoDB table name is the lower case of
     * x002f\<service-path\>x0000\<entity_id\>x0000\<entity_type\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildDBNameNonRootServicePathDataModelByEntity() throws Exception {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                + "-------- When a non root service-path is notified/defaulted and data_model is 'dm-by-entity' "
                + "the CartoDB table name is the lower case of x002f<servicePath>x0000<entityId>x0000<entityType>");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-entity";
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        String servicePath = "/somePath";
        String entity = "someId=someType"; // using the internal concatenator
        String attribute = null; // irrelevant for this test
        
        try {
            String builtTableName = sink.buildTableName(servicePath, entity, attribute);
        
            try {
                assertEquals("x002fsomepathx0000someidx0000sometype", builtTableName);
                System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                        + "-  OK  - '" + builtTableName + "' is equals to the lower case of "
                        + "x002f<servicePath>x0000<entityId>x0000<entityType>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                        + "- FAIL - '" + builtTableName + "' is not equals to the lower case of "
                        + "x002f<servicePath>x0000<entityId>x0000<entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildDBNameNonRootServicePathDataModelByEntity
    
    /**
     * [NGSICartoDBSink.buildTableName] -------- When a root service-path is notified/defaulted and
     * data_model is 'dm-by-service-path' the CartoDB table name is x002f.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildDBNameRootServicePathDataModelByServicePath() throws Exception {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                + "-------- When a root service-path is notified/defaulted and data_model is "
                + "'dm-by-service-path' the CartoDB table name is x002f");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-service-path";
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        String servicePath = "/";
        String entity = null; // irrelevant for this test
        String attribute = null; // irrelevant for this test
        
        try {
            String builtTableName = sink.buildTableName(servicePath, entity, attribute);
            
            try {
                assertEquals("x002f", builtTableName);
                System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                        + "-  OK  - '" + builtTableName + "' is equals to x002f");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                        + "- FAIL - '" + builtTableName + "' is not equals to x002f");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildDBNameRootServicePathDataModelByServicePath
    
    /**
     * [NGSICartoDBSink.buildTableName] -------- When a root service-path is notified/defaulted
     * and data_model is 'dm-by-entity' the CartoDB table name is the lower case of
     * x002f\<service-path\>_\<entity_id\>_\<entity_type\>.
     * @throws java.lang.Exception
     */
    @Test
    public void testBuildDBNameRootServicePathDataModelByEntity() throws Exception {
        System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                + "-------- When a non service-path is notified/defaulted and data_model is 'dm-by-entity' "
                + "the CartoDB table name is the lower case of x002fx0000<entityId>x0000<entityType>");
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-entity";
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        String servicePath = "/";
        String entity = "someId=someType";
        String attribute = null; // irrelevant for this test
        
        try {
            String builtTableName = sink.buildTableName(servicePath, entity, attribute);
        
            try {
                assertEquals("x002fx0000someidx0000sometype", builtTableName);
                System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                        + "-  OK  - '" + builtTableName + "' is equals to the lower case of "
                        + "x002fx0000<entityId>x0000<entityType>");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                        + "- FAIL - '" + builtTableName + "' is not equals to the lower case of "
                        + "x002fx0000<entityId>x0000<entityType>");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[NGSICartoDBSink.buildTableName]")
                    + "- FAIL - There was some problem when building the table name");
            throw e;
        } // try catch
    } // testBuildDBNameRootServicePathDataModelByEntity

    /**
     * [CartoDBAggregator.initialize] -------- When initializing through an initial geolocated event, a table
     * name is created.
     * @throws java.lang.Exception
     */
    @Test
    public void testInitializeBuildTable() throws Exception {
        System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                + "-------- When initializing through an initial geolocated event, a table name is created");
        
        // Create a NGSICartoDBSink
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-entity";
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        // Create a CartoDBAggregator
        CartoDBAggregator aggregator = sink.new CartoDBAggregator();
        
        // Create a NGSIEvent
        long recvTimeTs = 1461136795801L;
        String service = "someService";
        String servicePath = "somePath";
        String entity = "someId_someType";
        String attribute = null; // irrelevant for this test
        ContextElement contextElement = createContextElement();
        NGSIEvent event = new NGSIEvent(recvTimeTs, service, servicePath, entity, attribute, contextElement);
        
        try {
            aggregator.initialize(event);
        
            try {
                assertTrue(aggregator.getTableName() != null);
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "-  OK  - A table name has been created");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "- FAIL - A table name has not been created");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                    + "- FAIL - There was some problem when initializing CartoDBAggregator");
            throw e;
        } // try catch
    } // testInitializeBuildTable
    
    /**
     * [CartoDBAggregator.initialize] -------- When initializing through an initial geolocated event, the
     * aggregation fields string contains a field and a metadata field for each attribute in the event except
     * for the geolocation attribute, which is added as a specific field ('the_geom').
     * @throws java.lang.Exception
     */
    @Test
    public void testInitializeFieldsOK() throws Exception {
        System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                + "-------- When initializing through an initial geolocated event, the aggregation fields "
                + "string contains a field and a metadata field for each attribute in the event except for "
                + "the geolocation attribute, which is added as a specific field ('the_geom')");
        
        // Create a NGSICartoDBSink
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-entity";
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        // Create a CartoDBAggregator
        CartoDBAggregator aggregator = sink.new CartoDBAggregator();
        
        // Create a NGSIEvent
        long recvTimeTs = 1461136795801L;
        String service = "someService";
        String servicePath = "somePath";
        String entity = "someId_someType";
        String attribute = null; // irrelevant for this test
        ContextElement contextElement = createContextElement();
        NGSIEvent event = new NGSIEvent(recvTimeTs, service, servicePath, entity, attribute, contextElement);
        
        try {
            aggregator.initialize(event);
            String fields = aggregator.getFields();
        
            try {
                assertTrue(!fields.contains("somename1") && !fields.contains("somname1_md"));
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "-  OK  - 'somename1' and 'somename1_md' are not in the fields '" + fields + "'");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "- FAIL - 'somename1' and 'somename1_md' are in the fields '" + fields + "'");
                throw e;
            } // try catch
            
            try {
                assertTrue(fields.contains("somename2") && fields.contains("somename2_md"));
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "-  OK  - 'somename2' and 'somename2_md' are in the fields '" + fields + "'");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "- FAIL - 'somename2' and 'somename2_md' are not in the fields '" + fields + "'");
                throw e;
            } // try catch
            
            try {
                assertTrue(fields.contains(NGSIConstants.THE_GEOM));
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "-  OK  - '" + NGSIConstants.THE_GEOM + "' is in the fields '" + fields + "'");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "- FAIL - '" + NGSIConstants.THE_GEOM + "' is not in the fields '" + fields + "'");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                    + "- FAIL - There was some problem when initializing CartoDBAggregator");
            throw e;
        } // try catch
    } // testInitializeFieldsOK
    
    /**
     * [CartoDBAggregator.initialize] -------- When initializing through an initial geolocated event,
     * the aggregation fields string is lower case, starts with '(' and finishes with ')'.
     * @throws java.lang.Exception
     */
    @Test
    public void testInitializeFieldsStringOK() throws Exception {
        System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                + "-------- When initializing through an initial geolocated event, the aggregation fields "
                + "string is lower case, starts with '(' and finishes with ')'");
        
        // Create a NGSICartoDBSink
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-entity";
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        // Create a CartoDBAggregator
        CartoDBAggregator aggregator = sink.new CartoDBAggregator();
        
        // Create a NGSIEvent
        long recvTimeTs = 1461136795801L;
        String service = "someService";
        String servicePath = "somePath";
        String entity = "someId_someType";
        String attribute = null; // irrelevant for this test
        ContextElement contextElement = createContextElement();
        NGSIEvent event = new NGSIEvent(recvTimeTs, service, servicePath, entity, attribute, contextElement);
        
        try {
            aggregator.initialize(event);
            String fields = aggregator.getFields();
        
            try {
                assertEquals(fields, fields.toLowerCase());
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "-  OK  - '" + fields + "' is lower case");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "- FAIL - '" + fields + "' is not lower case");
                throw e;
            } // try catch
            
            try {
                assertTrue(fields.startsWith("("));
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "-  OK  - '" + fields + "' starts with '('");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "- FAIL - '" + fields + "' does not start with '('");
                throw e;
            } // try catch
            
            try {
                assertTrue(fields.endsWith(")"));
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "-  OK  - '" + fields + "' ends with ')'");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "- FAIL - '" + fields + "' does not end with ')'");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                    + "- FAIL - There was some problem when initializing CartoDBAggregator");
            throw e;
        } // try catch
    } // testInitializeFieldsStringOK
    
    /**
     * [CartoDBAggregator.aggregate] -------- When aggregating a single geolocated event, the aggregation
     * values string contains a value and a metadata value for each attribute in the event except for the
     * geolocation attribute, which is added as a specific value (a point).
     * @throws java.lang.Exception
     */
    @Test
    public void testAggregateValuesOK() throws Exception {
        System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                + "-------- When aggregating a single geolocated event, the aggregation values string "
                + "contains a value and a metadata value for each attribute in the event except for the "
                + "geolocation attribute, which is added as a specific value (a point)");
        
        // Create a NGSICartoDBSink
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-entity";
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        // Create a CartoDBAggregator
        CartoDBAggregator aggregator = sink.new CartoDBAggregator();
        
        // Create a NGSIEvent
        long recvTimeTs = 1461136795801L;
        String service = "someService";
        String servicePath = "somePath";
        String entity = "someId_someType";
        String attribute = null; // irrelevant for this test
        ContextElement contextElement = createContextElement();
        NGSIEvent event = new NGSIEvent(recvTimeTs, service, servicePath, entity, attribute, contextElement);
        
        try {
            aggregator.initialize(event);
            aggregator.aggregate(event);
            String rows = aggregator.getRows();
            
            try {
                assertTrue(!rows.contains("'-3.7167, 40.3833'")
                        && !rows.contains("'{\"name\":\"location\",\"type\":\"string\",\"value\":\"WGS84\"}'"));
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "-  OK  - '-3.7167, 40.3833' and "
                        + "'{\"name\":\"location\",\"type\":\"string\",\"value\":\"WGS84\"}' "
                        + "are not in the rows '" + rows + "'");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "- FAIL - '-3.7167, 40.3833' and "
                        + "'{\"name\":\"location\",\"type\":\"string\",\"value\":\"WGS84\"}' "
                        + "are in the rows '" + rows + "'");
                throw e;
            } // try catch
            
            try {
                assertTrue(rows.contains("'someValue2'") && rows.contains("'[]'"));
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "-  OK  - 'someValue2' and '[]' are in the rows '" + rows + "'");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "- FAIL - 'someValue2' and '[]' are not in the rows '" + rows + "'");
                throw e;
            } // try catch
            
            try {
                assertTrue(rows.contains("ST_SetSRID(ST_MakePoint(-3.7167,40.3833), 4326)"));
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "-  OK  - 'ST_SetSRID(ST_MakePoint(-3.7167, 40.3833), 4326)' is in the rows '" + rows
                        + "'");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                        + "- FAIL - 'ST_SetSRID(ST_MakePoint(-3.7167, 40.3833), 4326)' is not in the rows '"
                        + rows + "'");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[CartoDBAggregator.initialize]")
                    + "- FAIL - There was some problem when initializing CartoDBAggregator");
            throw e;
        } // try catch
    } // testAggregateValuesOK
    
    /**
     * [CartoDBAggregator.aggregate] -------- When aggregating a single geolocated event, the aggregation
     * values string starts with '(' and finishes with ')'.
     * @throws java.lang.Exception
     */
    @Test
    public void testAggregateValuesStringOK() throws Exception {
        System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                + "-------- When aggregating a single geolocated event, the aggregation values string starts "
                + "with '(' and finishes with ')'");
        
        // Create a NGSICartoDBSink
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-entity";
        String enableLowercase = null; // default one
        String flipCoordinates = null; // default one
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        // Create a CartoDBAggregator
        CartoDBAggregator aggregator = sink.new CartoDBAggregator();
        
        // Create a NGSIEvent
        long recvTimeTs = 1461136795801L;
        String service = "someService";
        String servicePath = "somePath";
        String entity = "someId_someType";
        String attribute = null; // irrelevant for this test
        ContextElement contextElement = createContextElement();
        NGSIEvent event = new NGSIEvent(recvTimeTs, service, servicePath, entity, attribute, contextElement);
        
        try {
            aggregator.initialize(event);
            aggregator.aggregate(event);
            String rows = aggregator.getRows();
            
            try {
                assertTrue(rows.startsWith("("));
                System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                        + "-  OK  - '" + rows + "' starts with '('");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                        + "- FAIL - '" + rows + "' does not start with '('");
                throw e;
            } // try catch
            
            try {
                assertTrue(rows.endsWith(")"));
                System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                        + "-  OK  - '" + rows + "' ends with ')'");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                        + "- FAIL - '" + rows + "' does not end with ')'");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                    + "- FAIL - There was some problem when aggregating in CartoDBAggregator");
            throw e;
        } // try catch
    } // testAggregateValuesStringOK
    
    /**
     * [CartoDBAggregator.aggregate] -------- When aggregating a single geolocated event, if flip_coordinates=true
     * then the_geom field contains a point with exchanged latitude and longitude.
     * @throws java.lang.Exception
     */
    @Test
    public void testAggregateCoordinatesAreFlipped() throws Exception {
        System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                + "-------- When aggregating a single geolocated event, if flip_coordinates=true then the_geom "
                + "field contains a point with exchanged latitude and longitude.");
        
        // Create a NGSICartoDBSink
        String endpoint = "https://localhost";
        String apiKey = "1234567890abcdef";
        String dataModel = "dm-by-entity";
        String enableLowercase = null; // default one
        String flipCoordinates = "true";
        String enableRaw = null; // default one
        String enableDistance = null; // default one
        String keysConfFile = "/keys.conf";
        NGSICartoDBSink sink = new NGSICartoDBSink();
        sink.configure(createContext(endpoint, apiKey, dataModel, enableLowercase, flipCoordinates, enableRaw,
                enableDistance, keysConfFile));
        
        // Create a CartoDBAggregator
        CartoDBAggregator aggregator = sink.new CartoDBAggregator();
        
        // Create a NGSIEvent
        long recvTimeTs = 1461136795801L;
        String service = "someService";
        String servicePath = "somePath";
        String entity = "someId_someType";
        String attribute = null; // irrelevant for this test
        ContextElement contextElement = createContextElement();
        NGSIEvent event = new NGSIEvent(recvTimeTs, service, servicePath, entity, attribute, contextElement);
        
        try {
            aggregator.initialize(event);
            aggregator.aggregate(event);
            String rows = aggregator.getRows();
            
            try {
                assertTrue(rows.contains("40.3833,-3.7167"));
                System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                        + "-  OK  - '" + rows + "' contains the coordinates '-3.7167, 40.3833' flipped");
            } catch (AssertionError e) {
                System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                        + "- FAIL - '" + rows + "' done not contain the coordinates '-3.7167, 40.3833' flipped");
                throw e;
            } // try catch
        } catch (Exception e) {
            System.out.println(getTestTraceHead("[CartoDBAggregator.aggregate]")
                    + "- FAIL - There was some problem when aggregating in CartoDBAggregator");
            throw e;
        } // try catch
    } // testAggregateCoordinatesAreFlipped
    
    private Context createContext(String endpoint, String apiKey, String dataModel, String enableLowercase,
            String flipCoordinates, String enableRaw, String enableDistance, String keysConfFile) {
        Context context = new Context();
        context.put("api_key", apiKey);
        context.put("batch_size", "100");
        context.put("batch_timeout", "30");
        context.put("batch_ttl", "10");
        context.put("data_model", dataModel);
        context.put("enable_distance", enableDistance);
        context.put("enable_grouping", "false");
        context.put("enable_lowercase", enableLowercase);
        context.put("enable_raw", enableRaw);
        context.put("endpoint", endpoint);
        context.put("flip_coordinates", flipCoordinates);
        context.put("keys_conf_file", keysConfFile);
        return context;
    } // createContext
    
    private ContextElement createContextElement() {
        NotifyContextRequest notifyContextRequest = new NotifyContextRequest();
        ContextMetadata contextMetadata = notifyContextRequest.new ContextMetadata();
        contextMetadata.setName("location");
        contextMetadata.setType("string");
        contextMetadata.setContextMetadata(new JsonPrimitive("WGS84"));
        ArrayList<ContextMetadata> metadata = new ArrayList<ContextMetadata>();
        metadata.add(contextMetadata);
        ContextAttribute contextAttribute1 = notifyContextRequest.new ContextAttribute();
        contextAttribute1.setName("someName1");
        contextAttribute1.setType("someType1");
        contextAttribute1.setContextValue(new JsonPrimitive("-3.7167, 40.3833"));
        contextAttribute1.setContextMetadata(metadata);
        ContextAttribute contextAttribute2 = notifyContextRequest.new ContextAttribute();
        contextAttribute2.setName("someName2");
        contextAttribute2.setType("someType2");
        contextAttribute2.setContextValue(new JsonPrimitive("someValue2"));
        contextAttribute2.setContextMetadata(null);
        ArrayList<ContextAttribute> attributes = new ArrayList<ContextAttribute>();
        attributes.add(contextAttribute1);
        attributes.add(contextAttribute2);
        ContextElement contextElement = notifyContextRequest.new ContextElement();
        contextElement.setId("someId");
        contextElement.setType("someType");
        contextElement.setIsPattern("false");
        contextElement.setAttributes(attributes);
        return contextElement;
    } // createContextElement
    
} // NGSICartoDBSinkTest
