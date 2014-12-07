package com.axiomine.largecollections;

import java.util.Iterator;
import java.util.Map.Entry;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;

import com.google.common.base.Function;



/*
 * Copyright 2014 Sameer Wadkar
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
public final class MapValueIterator<V> implements Iterator<V> {

    private DBIterator iter = null;
    private Function<byte[],? extends V> valDeSerFunc = null;
    protected MapValueIterator(DB db,Function<byte[],? extends V> valDeSerFunc) {
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
        this.iter.remove();
    }

}