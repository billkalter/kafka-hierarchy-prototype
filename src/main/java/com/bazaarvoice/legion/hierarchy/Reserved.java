package com.bazaarvoice.legion.hierarchy;

public interface Reserved {

    // Want to distinguish root categories from those where the parent is unknown (null)
    String ROOT = "__root__";

    // Used in instances where the parent is undefined.
    String UNDEFINED = "__undef__";
}
