package com.danifoldi.dataverse.util;

import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class Pair<A, B> {

    private final @NotNull A first;
    private final @NotNull B second;

    private Pair(@NotNull A a, @NotNull B b) {

        this.first = a;
        this.second = b;
    }

    public A getFirst() {

        return first;
    }

    public B getSecond() {

        return second;
    }

    public static<A, B> @NotNull Pair<@NotNull A, @NotNull B> of(final @NotNull A a, final @NotNull B b) {

        return new Pair<>(a, b);
    }

    @Override
    public String toString() {

        return "Pair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return first.equals(pair.first) && second.equals(pair.second);
    }

    @Override
    public int hashCode() {

        return Objects.hash(first, second);
    }
}
