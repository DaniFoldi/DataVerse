package com.danifoldi.dataverse.config;

public class JsonParser {
    private String jsonString;
    private int pointer;

    class JsonValue {

    }

    class JsonObject {

    }

    class JsonArray {

    }

    class JsonObjectValue {

    }

    class JsonString {

    }

    class JsonNumber {

    }

    class JsonBoolean {

    }

    class JsonComment {
        boolean isMultiline;
        String value;

        public static JsonComment find() {

        }

        private JsonComment(boolean isMultiline, String value) {

        }
    }

    public static JsonValue parse(String jsonString) {
        JsonParser parser = new JsonParser(jsonString);
    }

    public JsonParser(String jsonString) {
        this.jsonString = jsonString;
        pointer = 0;
    }
}

/*
Document:    Object | Array | ObjectValue
Array:       [ Element?(, Element)* ,?]
Object:      {Object}
ObjectValue: (Key: Element)?(, (Key: Element)?)* ,?
Element:     Object | Array | String | Number | Boolean
String:      ".*" | '.*'
Key:         [a-zA-Z0-9-_]+
Number:      -?\d*.\d*(e-?\d+)
Boolean:     true | false
Comment:     \/\/.*\n | \/\*.*\*\/
 */