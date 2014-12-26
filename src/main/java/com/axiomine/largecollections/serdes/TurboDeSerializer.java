package com.axiomine.largecollections.serdes;

import java.io.Serializable;

import com.google.common.base.Function;

public interface TurboDeSerializer<T> extends Function<byte[],T>, Serializable {
}

