package com.danifoldi.dataverse.config;

import com.danifoldi.dml.DmlParseException;
import com.danifoldi.dml.DmlParser;
import com.danifoldi.dml.type.DmlObject;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public class Config {

    public Map<String, String> getConfig(Path configFile) {

        Map<String, String> config = new LinkedHashMap<>();

        try {

            DmlObject dmlConfig = DmlParser.parse(configFile).asObject();
            dmlConfig.keys().forEach(k -> config.put(k.value(), dmlConfig.get(k).asString().value()));

        } catch (IOException | DmlParseException e) {

            System.err.println(e.getMessage());
            e.printStackTrace();
            config.put("error", "true");
        }

        config.put("file_name", configFile.getFileName().toString());
        config.put("file_path", configFile.getParent().toString());

        return config;
    }
}
