/*
 * Copyright 2014 Axiomine
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
package com.axiomine.largecollections.serdes;

import java.io.Serializable;

import com.google.common.base.Function;
import com.google.common.primitives.Longs;

public class SerializableSerDes {
    public static class SerFunction implements TurboSerializer<Serializable> {
        private static final long serialVersionUID = 10L;
        public byte[] apply(Serializable arg) {
            if(arg==null){
                return null;
            }
            else{
                byte[] ba = null;
                ba = org.apache.commons.lang.SerializationUtils
                        .serialize((Serializable) arg);
                return ba;
            }
        }
    }

    public static class DeSerFunction implements TurboDeSerializer<Serializable>{
        private static final long serialVersionUID = 10L;
        public Serializable apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return (Serializable) org.apache.commons.lang.SerializationUtils.deserialize(arg);    
            }            
        }    
    }

}
