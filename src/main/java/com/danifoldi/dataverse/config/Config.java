package com.danifoldi.dataverse.config;

import java.nio.file.Path;
import java.util.Map;

public class Config {

    public Map<String, String> getConfig(Path configFile) {
        return Map.of("file_name", "db.json");
    }
}
