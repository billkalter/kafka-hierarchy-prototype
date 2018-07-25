package com.bazaarvoice.legion.hierarchy.model;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class LineageTransition implements Serializable {
    private final List<String> _parentLineage;
    private final Set<String> _children;

    public LineageTransition(List<String> parentLineage, Set<String> children) {
        _parentLineage = parentLineage;
        _children = children;
    }

    public List<String> getParentLineage() {
        return _parentLineage;
    }

    public Set<String> getChildren() {
        return _children;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LineageTransition)) {
            return false;
        }
        LineageTransition that = (LineageTransition) o;
        return Objects.equals(_parentLineage, that._parentLineage) &&
                Objects.equals(_children, that._children);
    }

    @Override
    public int hashCode() {

        return Objects.hash(_parentLineage, _children);
    }
}
