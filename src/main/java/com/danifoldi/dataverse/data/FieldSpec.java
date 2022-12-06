package com.danifoldi.dataverse.data;

import com.google.common.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;

import java.lang.reflect.Field;

@SuppressWarnings("UnstableApiUsage")
public record FieldSpec(@NotNull String name, @NotNull TypeToken<?> type, @NotNull Field reflect) {}
