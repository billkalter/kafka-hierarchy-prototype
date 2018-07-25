package com.bazaarvoice.legion.hierarchy.model;

import java.io.Serializable;
import java.util.Objects;

public class ChildTransition implements Serializable {
    
    private final String _childId;
    private final boolean _add;

    public ChildTransition(String childId, boolean add) {
        _childId = childId;
        _add = add;
    }

    public String getChildId() {
        return _childId;
    }

    public boolean isAdd() {
        return _add;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ChildTransition)) {
            return false;
        }
        ChildTransition that = (ChildTransition) o;
        return _add == that._add &&
                Objects.equals(_childId, that._childId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(_childId, _add);
    }
}
