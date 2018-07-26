package com.bazaarvoice.legion.hierarchy;

public class HierarchyStreamConfig {

    private String _sourceTopic;
    private String _parentTransitionTopic;
    private String _childTransitionTopic;
    private String _parentChildrenTopic;
    private String _destTopic;

    public HierarchyStreamConfig(String sourceTopic, String parentTransitionTopic, String childTransitionTopic, String parentChildrenTopic, String destTopic) {
        _sourceTopic = sourceTopic;
        _parentTransitionTopic = parentTransitionTopic;
        _childTransitionTopic = childTransitionTopic;
        _parentChildrenTopic = parentChildrenTopic;
        _destTopic = destTopic;
    }

    public HierarchyStreamConfig() {
        // Empty
    }

    public String getSourceTopic() {
        return _sourceTopic;
    }

    public void setSourceTopic(String sourceTopic) {
        _sourceTopic = sourceTopic;
    }

    public String getParentTransitionTopic() {
        return _parentTransitionTopic;
    }

    public void setParentTransitionTopic(String parentTransitionTopic) {
        _parentTransitionTopic = parentTransitionTopic;
    }

    public String getChildTransitionTopic() {
        return _childTransitionTopic;
    }

    public void setChildTransitionTopic(String childTransitionTopic) {
        _childTransitionTopic = childTransitionTopic;
    }

    public String getParentChildrenTopic() {
        return _parentChildrenTopic;
    }

    public void setParentChildrenTopic(String parentChildrenTopic) {
        _parentChildrenTopic = parentChildrenTopic;
    }

    public String getDestTopic() {
        return _destTopic;
    }

    public void setDestTopic(String destTopic) {
        _destTopic = destTopic;
    }
}
