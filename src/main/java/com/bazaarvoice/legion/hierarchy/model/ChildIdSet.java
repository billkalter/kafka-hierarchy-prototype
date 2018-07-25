package com.bazaarvoice.legion.hierarchy.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ChildIdSet implements Serializable {

    private final HashSet<String> _children;

    public static final ChildIdSet EMPTY = new ChildIdSet(new HashSet<>());

    public ChildIdSet create(Collection<String> children) {
        if (children.isEmpty()) {
            return EMPTY;
        } else {
            return new ChildIdSet(new HashSet<>(children));
        }
    }
    
    private ChildIdSet(HashSet<String> children) {
        _children = children;
    }

    public Set<String> children() {
        return Collections.unmodifiableSet(_children);
    }

    public ChildIdSet withUpdate(String child, boolean add) {
        return add ? withChild(child) : withoutChild(child);
    }

    public ChildIdSet withChild(String child) {
        if (_children.contains(child)) {
            return this;
        }
        HashSet<String> updatedChildren = new HashSet<>(_children);
        updatedChildren.add(child);
        return new ChildIdSet(updatedChildren);
    }

    public ChildIdSet withoutChild(String child) {
        if (!_children.contains(child)) {
            return this;
        }
        HashSet<String> updatedChildren = new HashSet<>(_children);
        updatedChildren.remove(child);
        return new ChildIdSet(updatedChildren);
    }
}
