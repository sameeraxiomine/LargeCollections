/*
 * Copyright 2015 Axomine LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.axiomine.largecollections.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;

import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.google.common.base.Function;



public final class MapValueIterator<V> implements Iterator<V>,Closeable {

    private DBIterator iter = null;
    private TurboDeSerializer<? extends V> valDeSerFunc = null;
    public MapValueIterator(DB db,TurboDeSerializer<? extends V> valDeSerFunc) {
        ReadOptions ro = new ReadOptions();
        ro.fillCache(false);
        this.iter =db.iterator(ro);
        this.valDeSerFunc=valDeSerFunc;
        this.iter.seekToFirst();
    }

    public boolean hasNext() {
        return this.iter.hasNext();
    }

    public V next() {
        Entry<byte[], byte[]> entry = this.iter.next();
        return this.valDeSerFunc.apply(entry.getValue());
    }

    public void remove() {
        throw new UnsupportedOperationException("remove");
    }

    @Override
    public void close() throws IOException {
        this.iter.close();
    }

}