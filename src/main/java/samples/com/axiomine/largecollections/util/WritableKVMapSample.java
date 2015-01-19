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
package samples.com.axiomine.largecollections.util;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.IntWritable;

import com.axiomine.largecollections.util.LargeCollection;
import com.axiomine.largecollections.util.WritableKVMap;
import com.axiomine.largecollections.utilities.FileSerDeUtils;
import com.google.common.base.Throwables;

public class WritableKVMapSample {

    public static void main(String[] args) {
        createWritableKVMap();
        System.out.println("Create Map overriding the dbName");  
        createWritableKVMap("KVMAP1");
        System.out.println("Create Map overriding the dbPath,dbName");
        createWritableKVMap(System.getProperty("java.io.tmpdir"),"KVMAP2");
        System.out.println("Create Map overriding the dbPath,dbName,cacheSize(in MB)");
        createWritableKVMap(System.getProperty("java.io.tmpdir"),"KVMAP3",50);
        System.out.println("Create Map overriding the dbPath,dbName,cacheSize(in MB),bloomFilterSize");
        createWritableKVMap(System.getProperty("java.io.tmpdir"),"KVMAP4",50,1000);
        System.out.println("Override the path and name at the time of Deserialization.Create Map overriding the dbPath,dbName,cacheSize(in MB),bloomFilterSize");
        createWritableKVMapOveridePathAndNameWhileDSer("c:/tmp","KVMAP",50,1000);
    }
    
    public static void printMapCharacteristics(LargeCollection m){
        System.out.println("DB Path="+m.getDBPath());
        System.out.println("DB Name="+m.getDBName());
        System.out.println("Cache Size="+m.getCacheSize() + "MB");
        System.out.println("Bloomfilter Size="+m.getBloomFilterSize());
    }
    
    
    public static void workOnKVMap(WritableKVMap<IntWritable,IntWritable> map){
        try {
            for (int i = 0; i < 10; i++) {
                IntWritable r = (IntWritable) map.put(new IntWritable(i), new IntWritable(i));
            }
            System.out.println("Size of map="+map.size());
            System.out.println("Value for key 0="+map.get(new IntWritable(0)));;
            System.out.println("Now remove key 0");
            IntWritable i = (IntWritable) map.remove(new IntWritable(0));
            System.out.println("Value for key 0(just removed)="+i);
            System.out.println("Size of map="+map.size());
            IntWritable nullI = (IntWritable) map.remove(new IntWritable(0));
            System.out.println("Re- remove key 0");
            System.out.println("Value for key 0="+nullI);
            
            System.out.println("Contains key 0="+map.containsKey(new IntWritable(0)));
            System.out.println("Contains key 1="+map.containsKey(new IntWritable(1)));
            
            System.out.println("Now closing");
            map.close();
            boolean b = false;
            try{
                map.put(new IntWritable(0),new IntWritable(0));    
            }
            catch(Exception ex){
                System.out.println("Exception because acces after close");
                b=true;
            }
            map.open();
            System.out.println("Open again");
            map.put(new IntWritable(0),new IntWritable(0));
            System.out.println("Now put worked. Size of map should be 10. Size of the map ="+map.size());
            
            System.out.println("Now Serialize the Map");
            File serFile = new File(System.getProperty("java.io.tmpdir")+"/x.ser");
            FileSerDeUtils.serializeToFile(map,serFile);            
            System.out.println("Now De-Serialize the Map");
            map = (WritableKVMap<IntWritable,IntWritable>) FileSerDeUtils.deserializeFromFile(serFile);
            System.out.println("After De-Serialization Size of map should be 10. Size of the map ="+map.size());
            printMapCharacteristics(map);
            System.out.println("Now calling map.clear()");
            map.clear();
            System.out.println("After clear map size should be 0 and ="+map.size());
            map.put(new IntWritable(0),new IntWritable(0));
            System.out.println("Just added a record and map size ="+map.size());
            System.out.println("Finally Destroying");
            map.destroy();
            
            System.out.println("Cleanup serialized file");
            FileUtils.deleteQuietly(serFile);
            
            
        } catch (Exception ex) {            
            throw Throwables.propagate(ex);
        }
        
    }
    public static void createWritableKVMap(){        
        WritableKVMap<IntWritable,IntWritable> map = new WritableKVMap<IntWritable,IntWritable>(IntWritable.class,IntWritable.class);
        printMapCharacteristics(map);
        workOnKVMap(map);
    }

    public static void createWritableKVMap(String dbName){
        WritableKVMap<IntWritable,IntWritable> map = new WritableKVMap<IntWritable,IntWritable>(dbName,IntWritable.class,IntWritable.class);
        printMapCharacteristics(map);
        workOnKVMap(map);
    }

    public static void createWritableKVMap(String dbPath,String dbName){
        WritableKVMap<IntWritable,IntWritable> map = new WritableKVMap<IntWritable,IntWritable>(dbPath,dbName,IntWritable.class,IntWritable.class);
        printMapCharacteristics(map);
        workOnKVMap(map);
    }

    public static void createWritableKVMap(String dbPath,String dbName,int cacheSize){
        WritableKVMap<IntWritable,IntWritable> map = new WritableKVMap<IntWritable,IntWritable>(dbPath,dbName,cacheSize,IntWritable.class,IntWritable.class);
        printMapCharacteristics(map);
        workOnKVMap(map);
    }

    public static void createWritableKVMap(String dbPath,String dbName,int cacheSize,int bloomfilterSize){
        WritableKVMap<IntWritable,IntWritable> map = new WritableKVMap<IntWritable,IntWritable>(dbPath,dbName,cacheSize,bloomfilterSize,IntWritable.class,IntWritable.class);
        printMapCharacteristics(map);
        workOnKVMap(map);
    }

    public static void createWritableKVMapOveridePathAndNameWhileDSer(String dbPath,String dbName,int cacheSize,int bloomfilterSize){
        System.setProperty(LargeCollection.OVERRIDE_DB_PATH, dbPath);        
        System.setProperty(LargeCollection.OVERRIDE_DB_NAME, dbName);
        WritableKVMap<IntWritable,IntWritable> map = new WritableKVMap<IntWritable,IntWritable>(dbPath,dbName,cacheSize,bloomfilterSize,IntWritable.class,IntWritable.class);
        printMapCharacteristics(map);
        workOnKVMap(map);
    }

}
