package com.axiomine.largecollections.serdes;

import java.io.Serializable;

import com.google.common.base.Function;

public interface TurboSerializer<T> extends Function<T, byte[]>, Serializable {
}

