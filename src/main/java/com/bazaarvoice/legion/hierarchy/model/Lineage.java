package com.bazaarvoice.legion.hierarchy.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Lineage implements Serializable {

    public static final Lineage EMPTY = new Lineage(Collections.emptyList());
    
    private final List<String> _linage;

    public Lineage(String... lineage) {
        this(Arrays.asList(lineage));
    }
    
    public Lineage(List<String> linage) {
        _linage = new ArrayList<>(linage);
    }

    public List<String> getLinage() {
        return Collections.unmodifiableList(_linage);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Lineage)) {
            return false;
        }
        Lineage that = (Lineage) o;
        return Objects.equals(_linage, that._linage);
    }

    @Override
    public int hashCode() {

        return Objects.hash(_linage);
    }
}
