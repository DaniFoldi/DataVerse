package com.danifoldi.dataverse.data;

import com.google.gson.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;

public record FieldSpec(@NotNull String name, @NotNull TypeToken<?> type, @NotNull Field reflect) {}
