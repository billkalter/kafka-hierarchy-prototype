package com.bazaarvoice.legion.hierarchy.model;

import java.io.Serializable;
import java.util.Objects;

public class ParentTransition implements Serializable {

    private final String _oldParentId;
    private final String _newParentId;

    public ParentTransition(String oldParentId, String newParentId) {
        _oldParentId = oldParentId;
        _newParentId = newParentId;
    }

    public String getOldParentId() {
        return _oldParentId;
    }

    public String getNewParentId() {
        return _newParentId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ParentTransition)) {
            return false;
        }
        ParentTransition that = (ParentTransition) o;
        return Objects.equals(_oldParentId, that._oldParentId) &&
                Objects.equals(_newParentId, that._newParentId);
    }

    @Override
    public int hashCode() {

        return Objects.hash(_oldParentId, _newParentId);
    }
}
