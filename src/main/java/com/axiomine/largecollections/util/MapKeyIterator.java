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
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.ReadOptions;

import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.google.common.base.Function;


public final class MapKeyIterator<K> implements Iterator<K>, Closeable {
    private LargeCollection lColl = null;
    private DBIterator iter = null;
    private TurboDeSerializer<? extends K> keyDeSerFunc = null;
    private Entry<byte[], byte[]> lastEntry = null;
    public MapKeyIterator(LargeCollection coll,TurboDeSerializer<? extends K> keyDeSerFunc) {
        ReadOptions ro = new ReadOptions();
        ro.fillCache(false);
        this.lColl = coll;
        this.iter = coll.getDB().iterator(ro);
        this.keyDeSerFunc=keyDeSerFunc;
        this.iter.seekToFirst();
    }

    public boolean hasNext() {
        // TODO Auto-generated method stub
        return this.iter.hasNext();
    }

    public K next() {
        // TODO Auto-generated method stub
        Entry<byte[], byte[]> entry = this.iter.next();
        lastEntry = entry;
        return this.keyDeSerFunc.apply(entry.getKey());
    }

    public void remove() {
        throw new UnsupportedOperationException("remove");
    }
    
    @Override
    public void close() throws IOException {
        this.iter.close();
        
    }


}