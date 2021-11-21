package com.danifoldi.dataverse.util;

import java.sql.SQLException;

@FunctionalInterface
public interface SqlQuadConsumer<A, B, C, D> {

    void apply(A a, B b, C c, D d) throws SQLException;
}
