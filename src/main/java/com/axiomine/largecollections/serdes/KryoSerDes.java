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

import java.io.ByteArrayOutputStream;

import com.axiomine.largecollections.utilities.KryoUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class KryoSerDes {
    public static class SerFunction<K> implements TurboSerializer<K> {        
        private static final long serialVersionUID = 7L;
        private final ThreadLocal<Kryo> kryos;        
        public SerFunction(){
            this.kryos = KryoUtils.getThreadLocalKryos();
        }       
        
        public byte[] apply(K arg) {
            if(arg==null){
                return null;
            }
            else{
                Kryo kryo = this.kryos.get();
                byte[] ba = null;
                Output output = null;
                try {
                    output = new Output(new ByteArrayOutputStream());
                    kryo.writeClassAndObject(output, arg);
                    ba = output.toBytes();
                } catch (Exception ex) {
                    Throwables.propagate(ex);
                } finally {
                    try {
                        if (output != null)
                            output.close();
                    } catch (Exception ex) {
                        Throwables.propagate(ex);
                    }
                }
                return ba;
            }
            
        }
    }
    
    public static class DeSerFunction<K> implements TurboDeSerializer<K>{
        private static final long serialVersionUID = 7L;
        private final ThreadLocal<Kryo> kryos; 
        public DeSerFunction(){
            this.kryos = KryoUtils.getThreadLocalKryos();
        }
        
        public K apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                Kryo kryo = kryos.get();
                Input input = new Input(arg);
                return (K)kryo.readClassAndObject(input);
            }
        }    
    }

}
