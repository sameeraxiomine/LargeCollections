package com.axiomine.largecollections.functions;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.axiomine.largecollections.utils.SerDeUtils;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class WritableSerDe {
    public static class WritableSerFunction implements Function<Writable,byte[]>{
        public byte[] apply(Writable arg) {
            if (arg == null) {
                return null;
            }
            else{
                return SerDeUtils.serializeWritable(arg);
            }
        }    
    }
    
    public static class WritableDeSerFunction implements Function<byte[],Writable>{
        private Class<? extends Writable> writableCls = null; 
        public WritableDeSerFunction(Class<? extends Writable> wCls){
            this.writableCls = wCls;
        }
        public Writable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                Writable w=null;
                try {
                    w = this.writableCls.newInstance();
                    return SerDeUtils.deserializeWritable(w,arg);
                } catch (InstantiationException ex) {
                    Throwables.propagate(ex);
                }   
                catch(IllegalAccessException ex){
                    Throwables.propagate(ex);
                }
            
                return null;
                
            }
        }    

    }
    
    public static class TextWritableDeSerFunction implements Function<byte[],Text>{
        public Text apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                Text w = new Text();
                return (Text) SerDeUtils.deserializeWritable(w, arg);
            }
        }    
    }
    
    public static class IntWritableDeSerFunction implements Function<byte[],IntWritable>{
        public IntWritable apply(byte[] arg) {
            if (arg == null) {
                return null;
            }
            else{
                IntWritable w = new IntWritable();
                return (IntWritable) SerDeUtils.deserializeWritable(w, arg);
            }
        }    
    }
}
