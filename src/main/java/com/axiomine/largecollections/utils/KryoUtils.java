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

package com.axiomine.largecollections.utils;

import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.ExternalizableSerializer;
import com.google.common.base.Throwables;

public class KryoUtils {
    public static final String KRYO_REGISTRATION_PROP_FILE = "KRYO_REG_PROP_FILE";
    
    public static void registerKryoClasses(Kryo kryo){
        try{
            Map<String,String> m = new HashMap<String,String>();
            String propFile = System.getProperty(KRYO_REGISTRATION_PROP_FILE);
            if(StringUtils.isNotBlank(propFile)){
                FileReader fReader = new FileReader(new File(propFile));
                Properties props = new Properties();
                props.load(fReader);
                Set ks =  props.keySet();
                for(Object k:ks){
                    Class c = Class.forName((String)k);
                    Class s = Class.forName(props.getProperty((String)k));            
                    kryo.register(c, (Serializer)s.newInstance());
                }            
            }
        }
        catch(Exception ex){
            Throwables.propagate(ex);
        }
    }
    
    public static ThreadLocal<Kryo> getThreadLocalKryos() {
        ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
            protected Kryo initialValue() {                
                Kryo kryo = new Kryo();
                KryoUtils.registerKryoClasses(kryo);
                return kryo;
            };
        };
        return kryos;
    }
    
    public static void registerKryoSerializer(Kryo kryo, Class cls,
            Serializer serializer) {
        kryo.addDefaultSerializer(cls, serializer);
    }
    
    public static void registerKryoSerializer(Kryo kryo, Class cls,
            Serializer serializer, int id) {
        KryoUtils.registerKryoSerializer(kryo, cls, serializer);
        kryo.register(cls, id);
    }
    
    public void registerKryoExternalizableSerializer(Kryo kryo, Class cls) {
        kryo.addDefaultSerializer(cls, new ExternalizableSerializer());
    }
    
    public static void registerKryoExternalizableSerializer(Kryo kryo,
            Class cls, int id) {
        KryoUtils.registerKryoExternalizableSerializer(kryo, cls, id);
        kryo.register(cls, id);
    }
    
}
