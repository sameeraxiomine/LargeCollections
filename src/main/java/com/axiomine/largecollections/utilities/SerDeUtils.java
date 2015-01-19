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

package com.axiomine.largecollections.utilities;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Externalizable;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.hadoop.io.Writable;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;

public class SerDeUtils {

    
    public static byte[] integerToByteArray(int o){
        return Ints.toByteArray(o);
    }
    public static int integerFromByteArray(byte[] bytes){
        return Ints.fromByteArray(bytes);
    }
    
    public static byte[] serializeExternalizable(Externalizable obj) {
        byte[] outBA = null;
        ByteArrayOutputStream out = null;
        ObjectOutputStream oos = null;
        try {
            out = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(out);
            oos.writeObject(obj);            
            outBA =  out.toByteArray();
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
        finally{
            if(outBA!=null){
                try{
                    oos.close();
                }
                catch(Exception ex){
                    //Nothing to do
                    ex.printStackTrace();
                }
            
            }
        }
        return outBA;
    }
    
    public static Externalizable deSerializeExternalizable(byte[] ba) {
        Externalizable obj = null;
        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(ba));
            obj = (Externalizable)ois.readObject();            
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
        finally{
            if(ois!=null){
                try{
                    ois.close();
                }
                catch(Exception ex){
                    //Nothing to do
                    ex.printStackTrace();
                }
            
            }
        }
        return obj;
    }
    
    public static byte[] serializeWritable(Writable writable) {
        byte[] outBA = null;
        ByteArrayOutputStream out = null;
        DataOutputStream dataOut = null;
        try {
            out = new ByteArrayOutputStream();
            dataOut = new DataOutputStream(out);
            writable.write(dataOut);            
            outBA =  out.toByteArray();
        } catch (Exception ex) {
            Throwables.propagate(ex);
        }
        finally{
            if(outBA!=null){
                try{
                    dataOut.close();
                }
                catch(Exception ex){
                    //Nothing to do
                    ex.printStackTrace();
                }
            
            }
        }
        return outBA;
    }
    
    public static Writable deserializeWritable(Writable writable, byte[] bytes) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            DataInputStream dataIn = new DataInputStream(in);
            writable.readFields(dataIn);
            dataIn.close();
            return writable;
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }
    
    public static Writable deserializeWritable(Class<Writable> wc, byte[] bytes) {
        try {
            Writable writable =  wc.newInstance();
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            DataInputStream dataIn = new DataInputStream(in);
            writable.readFields(dataIn);
            dataIn.close();
            return writable;
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

}
