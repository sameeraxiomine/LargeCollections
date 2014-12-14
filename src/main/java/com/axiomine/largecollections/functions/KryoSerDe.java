package com.axiomine.largecollections.functions;

import java.io.ByteArrayOutputStream;

import com.axiomine.largecollections.utilities.KryoUtils;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class KryoSerDe {
    public static class SerFunction<K> implements Function<K, byte[]> {        
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
    
    public static class DeSerFunction<K> implements Function<byte[],K>{
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
