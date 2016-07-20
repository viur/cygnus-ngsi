package com.telefonica.iot.cygnus.sinks;

import com.telefonica.iot.cygnus.backends.postgresql.PostgreSQLBackendImpl;
import com.telefonica.iot.cygnus.containers.NotifyContextRequest;
import com.telefonica.iot.cygnus.containers.NotifyContextRequestV2;
import com.telefonica.iot.cygnus.errors.CygnusBadConfiguration;
import com.telefonica.iot.cygnus.log.CygnusLogger;
import com.telefonica.iot.cygnus.utils.CommonConstants;
import com.telefonica.iot.cygnus.utils.CommonUtils;
import com.telefonica.iot.cygnus.utils.NGSIConstants;
import com.telefonica.iot.cygnus.utils.NGSIUtils;
import org.apache.flume.Context;

import java.util.*;

/**
 * Created by ultrix on 24/06/16.
 * Viur uses a Postgres Database for now
 */
public class NGSIViurSink extends NGSISink {


    private static final CygnusLogger LOGGER = new CygnusLogger(NGSIViurSink.class);
    private String host;
    private String port;
    private String database;
    private String username;
    private String password;
    private PostgreSQLBackendImpl persistenceBackend;

    /**
     * Constructor.
     */
    public NGSIViurSink() {
        super();
    } // NGSIViurSink


    @Override
    public void configure(Context context) {

        host = context.getString("host", "localhost");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (host=" + host + ")");
        port = context.getString("port", "5432");
        int intPort = Integer.parseInt(port);

        if ((intPort <= 0) || (intPort > 65535)) {
            invalidConfiguration = true;
            LOGGER.debug("[" + this.getName() + "] Invalid configuration (port=" + port + ")"
                    + " -- Must be between 0 and 65535");
        } else {
            LOGGER.debug("[" + this.getName() + "] Reading configuration (port=" + port + ")");
        }  // if else

        database = context.getString("database", "postgres");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (database=" + database + ")");
        username = context.getString("username", "postgres");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (username=" + username + ")");
        // FIXME: password should be read as a SHA1 and decoded here
        password = context.getString("password", "");
        LOGGER.debug("[" + this.getName() + "] Reading configuration (password=" + password + ")");

        super.configure(context);

        // CKAN requires all the names written in lower case
        enableLowercase = true;
    } // configure


    @Override
    public void start() {
        try {
            LOGGER.debug("[" + this.getName() + "] Viur persistence backend created");
            persistenceBackend = new PostgreSQLBackendImpl(host, port, database,
                    username, password);
        } catch (Exception e) {
            LOGGER.error("Error while creating the Viur persistence backend. Details="
                    + e.getMessage());
        } // try catch

        super.start();
        LOGGER.info("[" + this.getName() + "] Startup completed");
    } // start


    @Override
    void persistBatch(NGSIBatch batch) throws Exception {
        if (batch == null) {
            LOGGER.debug("[" + this.getName() + "] Null batch, nothing to do");
            return;
        } // if

        // iterate on the destinations, for each one a single create / append will be performed
        for (String destination : batch.getDestinations()) {
            LOGGER.debug("[" + this.getName() + "] Processing sub-batch regarding the " + destination
                    + " destination");

            // get the sub-batch for this destination
            ArrayList<NGSIEvent> subBatch = batch.getEvents(destination);

            // init aggregator for this destination
            ViurAggregator aggregator = new ViurAggregator();
            aggregator.initialize(subBatch.get(0));

            for (NGSIEvent cygnusEvent : subBatch) {
                aggregator.aggregate(cygnusEvent);
            } // for

            // persist the fieldValues
            persistAggregation(aggregator);
            batch.setPersisted(destination);
        } // for
    } // persistBatch


    /**
     * Class for aggregating fieldValues.
     */
    private class ViurAggregator {

        // string containing the data fieldValues
        protected LinkedHashMap<String, Column> aggregation;

        protected Table table;

        protected String service;
        protected String servicePath;
        protected String entity;
        protected String attribute;

        public ViurAggregator() {
            aggregation = new LinkedHashMap<String, Column>();
        } // ViurAggregator

        public String getSchema() {
            return table.getSchema();
        } // getDbName

        public String getTableName() {
            return table.getName();
        } // getTableName

        public String getFieldsForCreate() {
            return table.getFieldsForCreate();
        } // getFieldsForCreate

        public String getFieldsForInsert() {
            return table.getFieldsForInsert();
        } // getFieldsForInsert

        public String getValuesForInsert() {
            return table.getValuesForInsert();
        }

        public void initialize(NGSIEvent cygnusEvent) throws Exception {

            service = cygnusEvent.getService();
            servicePath = cygnusEvent.getServicePath();
            entity = cygnusEvent.getEntity();
            attribute = cygnusEvent.getAttribute();

            String schemaName = buildSchemaName();
            String tableName = buildTableName();

            table = new Table(schemaName, tableName);

            // particular initialization

            table.createColumn(NGSIConstants.RECV_TIME, DataType.DATE_TIME);
            table.createColumn(NGSIConstants.FIWARE_SERVICE_PATH, DataType.STRING);
            table.createColumn(NGSIConstants.ENTITY_ID, DataType.STRING);
            table.createColumn(NGSIConstants.ENTITY_TYPE, DataType.STRING);

            // iterate on all this context element attributes, if there are attributes
            ArrayList<NotifyContextRequest.ContextAttribute> contextAttributes = cygnusEvent.getContextElement().getAttributes();

            if (contextAttributes == null || contextAttributes.isEmpty()) {
                return;
            } // if

            for (NotifyContextRequest.ContextAttribute contextAttribute : contextAttributes) {
                table.createColumn(contextAttribute);
            } // for

        } // initialize

        private String buildSchemaName() throws Exception {
            String name = NGSIUtils.encode(service, false, true);

            if (name.length() > CommonConstants.MAX_NAME_LEN) {
                throw new CygnusBadConfiguration("Building schema name '" + name
                        + "' and its length is greater than " + CommonConstants.MAX_NAME_LEN);
            } // if

            return name;
        } // buildSchemaName

        private String buildTableName() throws Exception {
            String name;

            switch (dataModel) {
                case DMBYSERVICEPATH:
                    if (servicePath.equals("/")) {
                        throw new CygnusBadConfiguration("Default service path '/' cannot be used with "
                                + "dm-by-service-path data model");
                    } // if

                    name = NGSIUtils.encode(servicePath, true, false);
                    break;
                case DMBYENTITY:
                    String truncatedServicePath = NGSIUtils.encode(servicePath, true, false);
                    name = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_')
                            + NGSIUtils.encode(entity, false, true);
                    break;
                case DMBYATTRIBUTE:
                    truncatedServicePath = NGSIUtils.encode(servicePath, true, false);
                    name = (truncatedServicePath.isEmpty() ? "" : truncatedServicePath + '_')
                            + NGSIUtils.encode(entity, false, true)
                            + '_' + NGSIUtils.encode(attribute, false, true);
                    break;
                default:
                    throw new CygnusBadConfiguration("Unknown data model '" + dataModel.toString()
                            + "'. Please, use DMBYSERVICEPATH, DMBYENTITY or DMBYATTRIBUTE");
            } // switch

            if (name.length() > CommonConstants.MAX_NAME_LEN) {
                throw new CygnusBadConfiguration("Building table name '" + name
                        + "' and its length is greater than " + CommonConstants.MAX_NAME_LEN);
            } // if

            return name;
        } // buildTableName


        public void aggregate(NGSIEvent cygnusEvent) throws Exception {

            // get the event headers
            long recvTimeTs = cygnusEvent.getRecvTimeTs();
            String recvTime = CommonUtils.getHumanReadable(recvTimeTs, true);

            // get the event body
            NotifyContextRequest.ContextElement contextElement = cygnusEvent.getContextElement();
            String entityId = contextElement.getId();
            String entityType = contextElement.getType();
            LOGGER.debug("[" + getName() + "] Processing context element (id=" + entityId + ", type="
                    + entityType + ")");

            // iterate on all this context element attributes, if there are attributes
            ArrayList<NotifyContextRequest.ContextAttribute> contextAttributes = contextElement.getAttributes();

            if (contextAttributes == null || contextAttributes.isEmpty()) {
                LOGGER.warn("No attributes within the notified entity, nothing is done (id=" + entityId
                        + ", type=" + entityType + ")");
                return;
            } // if

            table.getColumn(NGSIConstants.RECV_TIME).addValue(recvTime);
            table.getColumn(NGSIConstants.FIWARE_SERVICE_PATH).addValue(servicePath);
            table.getColumn(NGSIConstants.ENTITY_ID).addValue(entityId);
            table.getColumn(NGSIConstants.ENTITY_TYPE).addValue(entityType);

            for (NotifyContextRequest.ContextAttribute contextAttribute : contextAttributes) {

                String attrName = contextAttribute.getName();
                String attrType = contextAttribute.getType();
                table.addValue(contextAttribute);

                LOGGER.debug("[" + getName() + "] Processing context attribute (name=" + attrName + ", type="
                        + attrType + ")");
            } // for

        } // aggregate

        private class Table {

            private String schema;
            private String name;
            private LinkedHashMap<String, Column> columns;

            public Table(String schema, String name) {
                this.schema = schema.toLowerCase();
                this.name = name.toLowerCase();
                this.columns = new LinkedHashMap<String, Column>();
            }

            private String getSchema() {
                return schema;
            }

            private String getName() {
                return name;
            }

            private Column getColumn(String name) {
                return columns.get(name);
            }

            private void createColumn(String name, DataType type) {
                Column column = new Column(name, type);
                this.columns.put(name, column);
            }

            private void createColumn(NotifyContextRequest.ContextAttribute contextAttribute) {
                //TODO create _md column ?
                if (contextAttribute instanceof NotifyContextRequestV2.ContextAttributeV2) {

                    String name = contextAttribute.getName();
                    String type = contextAttribute.getType();
                    Object value = ((NotifyContextRequestV2.ContextAttributeV2) contextAttribute).getValue();
                    //TODO maybe read the metadata?

                    if (type.equalsIgnoreCase("geo:point")) {

                        this.columns.put(name + "_latitude", new Column(name + "_latitude", DataType.GEO_REFERENCE));
                        this.columns.put(name + "_longitude", new Column(name + "_longitude", DataType.GEO_REFERENCE));

                    } else {

                        DataType dataType = bestColumnDataType(value, type);
                        Column column = new Column(name, dataType);
                        this.columns.put(name, column);
                    }

                }
            }

            private void addValue(NotifyContextRequest.ContextAttribute contextAttribute) {
                //TODO
                if (contextAttribute instanceof NotifyContextRequestV2.ContextAttributeV2) {

                    String name = contextAttribute.getName();
                    String type = contextAttribute.getType();
                    Object value = ((NotifyContextRequestV2.ContextAttributeV2) contextAttribute).getValue();
                    //TODO maybe read the metadata?

                    if (type.equalsIgnoreCase("geo:point")) {
                        String geoReferences = (String) value;
                        String geo[] = geoReferences.split(",");

                        Double latitude = Double.parseDouble(geo[0].trim());
                        Double longitude = Double.parseDouble(geo[1].trim());

                        this.columns.get(name + "_latitude").addValue(latitude);
                        this.columns.get(name + "_longitude").addValue(longitude);
                    } else {
                        this.getColumn(name).addValue(value);
                    }
                }
            }

            public String getFieldsForCreate() {

                String fieldsForCreate = "(";
                boolean first = true;

                for (Column column : columns.values()) {

                    if (first) {
                        fieldsForCreate += column.getName() + " " + column.getDataType();
                        first = false;
                    } else {
                        fieldsForCreate += "," + column.getName() + " " + column.getDataType();
                    } // if else
                }

                return fieldsForCreate + ")";

            }

            public String getFieldsForInsert() {

                String fieldsForInsert = "(";
                boolean first = true;

                for (Column column : columns.values()) {

                    if (first) {
                        fieldsForInsert += column.getName();
                        first = false;
                    } else {
                        fieldsForInsert += "," + column.getName();
                    } // if else
                }

                return fieldsForInsert + ")";
            }

            public String getValuesForInsert() {

                String valuesForInsert = "";

                int rows = 0;
                for (Column column : columns.values()) {
                    rows = column.rows();
                    break;
                }

                for (int i = 0; i < rows; i++) {

                    if (i == 0) {
                        valuesForInsert += "(";
                    } else {
                        valuesForInsert += ",(";
                    } // if else

                    boolean first = true;
                    for (Column column : columns.values()) {
                        if (first) {
                            valuesForInsert += column.getValue(i);
                            first = false;
                        } else {
                            valuesForInsert += "," + column.getValue(i);
                        } // if else
                    } // while

                    valuesForInsert += ")";
                } // for

                return valuesForInsert;
            }
        }

        private class Column {

            private String name;
            private DataType dataType;
            private List<Object> values;

            private Column(String name, DataType dataType) {
                this.name = name;
                this.dataType = dataType;
                this.values = new ArrayList<Object>();
            }

            public String getName() {
                return name;
            }

            private String getDataType() {
                return dataType.dataType;
            }

            private void addValue(Object value) {
                this.values.add(value);
            }

            private String getValue(int i) {

                if (values.get(i) == null) {
                    return "null";
                }

                switch (dataType) {

                    case STRING:
                    case DATE_TIME:
                    case DATE:
                    case TIME:
                        return "'" + String.valueOf(values.get(i)) + "'";
                    case LONG:
                    case GEO_REFERENCE:
                    case BOOLEAN:
                        return String.valueOf(values.get(i));
                    case NUMBER:
                        return String.valueOf(values.get(i)).replace(",", ".");

                    default:
                        return "'" + String.valueOf(values.get(i)) + "'";
                }

            }

            private int rows() {
                return this.values.size();
            }

        }


    } // ViurAggregator

    private void persistAggregation(ViurAggregator aggregator) throws Exception {

        String schema = aggregator.getSchema();
        schema = "\"" + schema + "\"";
        String tableName = aggregator.getTableName();
        tableName = "\"" + tableName + "\"";

        String fieldsForCreate = aggregator.getFieldsForCreate();
        String fieldsForInsert = aggregator.getFieldsForInsert();
        String valuesForInsert = aggregator.getValuesForInsert();


        LOGGER.info("[" + this.getName() + "] Persisting data at ViurSink. Schema ("
                + schema + "), Table (" + tableName + "), Fields (" + fieldsForInsert + "), Values ("
                + valuesForInsert + ")");

        // creating the database and the table has only sense if working in row mode, in column node
        // everything must be provisioned in advance Why ??

        persistenceBackend.createSchema(schema);
        persistenceBackend.createTable(schema, tableName, fieldsForCreate);
        persistenceBackend.insertContextData(schema, tableName, fieldsForInsert, valuesForInsert);
    } // persistAggregation


    public enum DataType {

        STRING("text"),
        LONG("bigint"),
        NUMBER("double precision"),
        DATE_TIME("timestamp with time zone"),
        DATE("date"),
        TIME("time"),
        BOOLEAN("boolean"),
        GEO_REFERENCE("decimal(9,6)");

        private String dataType;

        DataType(String bdDataType) {
            this.dataType = bdDataType;
        }
    }

    private static DataType bestColumnDataType(Object value, String type) {

        if (value == null) {
            return DataType.STRING;
        }

        if (value instanceof String) {

            if (type == null || type.equalsIgnoreCase("")) {

                try {
                    Double.parseDouble((String) value);
                    return DataType.NUMBER;
                } catch (NumberFormatException ignore) {

                }

                return DataType.STRING;
            }

            if (type.equalsIgnoreCase("string") || type.equalsIgnoreCase("text")) {
                return DataType.STRING;
            }

            if (type.equalsIgnoreCase("int") || type.equalsIgnoreCase("integer")) {

                try {
                    Double.parseDouble((String) value);
                    return DataType.NUMBER;
                } catch (NumberFormatException ignore) {

                }

                return DataType.STRING;
            }

            if (type.equalsIgnoreCase("float") || type.equalsIgnoreCase("double")) {

                try {
                    Double.parseDouble((String) value);
                    return DataType.NUMBER;
                } catch (NumberFormatException ignore) {

                }

                return DataType.STRING;

            }

            //TODO maybe test against a mask passed through the metadata

            if (type.equalsIgnoreCase("datetime")) {
                return DataType.DATE_TIME;
            }

            if (type.equalsIgnoreCase("date")) {
                return DataType.DATE;
            }

            if (type.equalsIgnoreCase("time")) {
                return DataType.TIME;
            }

            return DataType.STRING;

        }

        if (value instanceof Number) {
            return DataType.NUMBER;
        }

        if (value instanceof Boolean) {
            return DataType.BOOLEAN;
        }


        return DataType.STRING;

    }
}
