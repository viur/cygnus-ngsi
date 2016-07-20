package com.telefonica.iot.cygnus.containers;

import com.google.gson.internal.LinkedTreeMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ultrix on 26/06/16.
 * Temporary Workaround to support NGSI v2 Subscription Notifications
 */
public class NotifyContextRequestV2 extends NotifyContextRequest {

    private List<Map<String, Object>> data;

    /**
     * Constructor for Gson, a Json parser.
     */
    public NotifyContextRequestV2() {
        this.data = new ArrayList<Map<String, Object>>();
    } // NotifyContextRequest

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }

    @Override
    public ArrayList<ContextElementResponse> getContextResponses() {

        if (this.contextResponses.size() == 0) {
            for (Map<String, Object> entity : data) {

                ContextElementResponse contextElementResponse = new ContextElementResponse();

                ContextElement contextElement = new ContextElement();
                ArrayList<ContextAttribute> contextAttributes = new ArrayList<ContextAttribute>();

                for (String param : entity.keySet()) {


                    if (param.equalsIgnoreCase("id")) {
                        contextElement.setId((String) entity.get("id"));
                    } else if (param.equalsIgnoreCase("type")) {
                        contextElement.setType((String) entity.get("type"));
                    } else if (param.equalsIgnoreCase("isPattern")) {
                        contextElement.setIsPattern((String) entity.get("isPattern"));
                    } else {

                        LinkedTreeMap<String, Object> attrValues = (LinkedTreeMap<String, Object>) entity.get(param);

                        ContextAttributeV2 contextAttribute = new ContextAttributeV2();
                        contextAttribute.setName(param);

                        if (attrValues.containsKey("type")) {
                            contextAttribute.setType((String) attrValues.get("type"));
                        }

                        if (attrValues.containsKey("value")) {
                            contextAttribute.setValue(attrValues.get("value"));
                        }

                        if (attrValues.containsKey("metadata")) {
                            contextAttribute.setMetadata(attrValues.get("metadata"));
                        }

                        contextAttributes.add(contextAttribute);
                    }
                }

                contextElement.setAttributes(contextAttributes);


                contextElementResponse.setContextElement(contextElement);
                contextElementResponse.setStatusCode(null);

                this.contextResponses.add(contextElementResponse);
            }
        }

        return this.contextResponses;
    }

    public class ContextAttributeV2 extends ContextAttribute {

        private Object value;
        private Object metadata;

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public void setMetadata(Object metadata) {
            this.metadata = metadata;
        }
    }
}
