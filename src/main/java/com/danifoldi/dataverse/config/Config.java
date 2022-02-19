package com.danifoldi.dataverse.config;

import com.danifoldi.dataverse.DataVerse;
import com.danifoldi.dml.DmlParser;
import com.danifoldi.dml.exception.DmlParseException;
import com.danifoldi.dml.type.DmlObject;
import com.danifoldi.dml.type.DmlString;
import com.danifoldi.dml.type.DmlValue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

public class Config {

    public static Map<String, String> getConfig(Path configFile) {

        Map<String, String> config = new LinkedHashMap<>();

        try {

            DmlObject dmlConfig = DmlParser.parse(configFile).asObject();
            dmlConfig.keys().forEach(k -> {
                DmlValue value = dmlConfig.get(k);
                if (value instanceof DmlString) {

                    config.put(k.value(), value.asString().value());
                } else {

                    DmlObject object = value.asObject();
                    object.keys().forEach(k2 -> config.put("%s_%s".formatted(k.value(), k2.value()), object.getString(k2).value()));
                }
            });

        } catch (IOException | DmlParseException e) {

            System.err.println(e.getMessage());
            e.printStackTrace();
            config.put("error", "true");
        }

        config.put("file_name", configFile.getFileName().toString());
        config.put("file_path", configFile.getParent().toString());

        return config;
    }

    public static void ensureConfig(Path configFile) throws IOException {

        if (Files.exists(configFile)) {

            return;
        }

        if (!Files.exists(configFile.getParent())) {

            Files.createDirectories(configFile.getParent());
        }

        try (final InputStream stream = DataVerse.class.getResourceAsStream("/%s".formatted(configFile.getFileName()))) {

            if (stream == null) {

                throw new IOException("Could not find default config.dml");
            }
            Files.copy(stream, configFile);
        }
    }
}
