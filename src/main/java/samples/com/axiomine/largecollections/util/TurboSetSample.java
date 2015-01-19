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

import java.io.Closeable;
import java.io.File;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;

import com.axiomine.largecollections.serdes.TurboDeSerializer;
import com.axiomine.largecollections.serdes.TurboSerializer;
import com.axiomine.largecollections.util.TurboKVMap;
import com.axiomine.largecollections.util.KryoKVMap;
import com.axiomine.largecollections.util.LargeCollection;
import com.axiomine.largecollections.util.TurboSet;
import com.axiomine.largecollections.utilities.FileSerDeUtils;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class TurboSetSample {
    public static TurboSerializer<Integer> TSERIALIZER = new com.axiomine.largecollections.serdes.IntegerSerDes.SerFunction();
    public static TurboDeSerializer<Integer>  TDESERIALIZER = new com.axiomine.largecollections.serdes.IntegerSerDes.DeSerFunction();

    public static void main(String[] args) {
        createTurboSet();
        System.out.println("Create Map overriding the dbName");  
        createTurboSet("KVMAP1");
        System.out.println("Create Map overriding the dbPath,dbName");
        createTurboSet(System.getProperty("java.io.tmpdir"),"KVMAP2");
        System.out.println("Create Map overriding the dbPath,dbName,cacheSize(in MB)");
        createTurboSet(System.getProperty("java.io.tmpdir"),"KVMAP3",50);
        System.out.println("Create Map overriding the dbPath,dbName,cacheSize(in MB),bloomFilterSize");
        createTurboSet(System.getProperty("java.io.tmpdir"),"KVMAP4",50,1000);
        System.out.println("Override the path and name at the time of Deserialization.Create Map overriding the dbPath,dbName,cacheSize(in MB),bloomFilterSize");
        createTurboSetOveridePathAndNameWhileDSer("c:/tmp","KVMAP",50,1000);
    }
    
    public static void printListCharacteristics(LargeCollection m){
        System.out.println("DB Path="+m.getDBPath());
        System.out.println("DB Name="+m.getDBName());
        System.out.println("Cache Size="+m.getCacheSize() + "MB");
        System.out.println("Bloomfilter Size="+m.getBloomFilterSize());
    }
    
    
    public static void workOnTurboSet(TurboSet<Integer> lst){
        try {
            
            
            for (int i = 0; i < 10; i++) {
                boolean b = lst.add(i);
                Assert.assertEquals(true, true);
            }
            System.out.println("Size of map="+lst.size());
            System.out.println("Value for key 0="+lst.contains(0));;
            System.out.println("Value for key 5="+lst.contains(5));;
            System.out.println("Value for key 9="+lst.contains(9));;
            System.out.println("Value for key 11="+lst.contains(11));;
            System.out.println("Now remove key 0");
            try{
                boolean b = lst.remove(0);    
                Assert.assertEquals(true, b);
                b = lst.remove(0);    
                Assert.assertEquals(false, b);

            }
            catch(Exception ex){
                System.out.println(ex.getMessage());
            }
            
            lst.remove(9);

            System.out.println("Size of map="+lst.size());
            lst.close();
            
            boolean b = false;
            try{
                lst.add(9);    
            }
            catch(Exception ex){
                System.out.println("Exception because acces after close");
                b=true;
            }
            lst.open();
            System.out.println("Open again");
            b = lst.add(9);            
            
            Assert.assertEquals(true, b);
            lst.add(9);
            lst.add(5);
            System.out.println("Now put worked. Size of map should be 10. Size of the map ="+lst.size());

            Iterator<Integer> iter = lst.iterator(); 
            try{
                while(iter.hasNext()){
                    int i = iter.next();
                    System.out.println("From ITerator = "+i);
                }
            }
            finally{
                //Always close and iterator after use. Otherwise you will not be able to call the clear function
                ((Closeable)iter).close();    
            }
            
            
            
            System.out.println("Now Serialize the List");
            File serFile = new File(System.getProperty("java.io.tmpdir")+"/x.ser");
            FileSerDeUtils.serializeToFile(lst,serFile);            
            System.out.println("Now De-Serialize the List");
            lst = (TurboSet<Integer>) FileSerDeUtils.deserializeFromFile(serFile);
            System.out.println("After De-Serialization "+lst);
            System.out.println("After De-Serialization Size of map should be 10. Size of the map ="+lst.size());
            printListCharacteristics(lst);
            System.out.println("Now calling lst.clear()");
            lst.clear();
            System.out.println("After clear list size should be 0 and ="+lst.size());
            lst.add(0);    
            System.out.println("Just added a record and lst size ="+lst.size());
            System.out.println("Finally Destroying");
            lst.destroy();
            
            System.out.println("Cleanup serialized file");
            FileUtils.deleteQuietly(serFile);
            
            
        } catch (Exception ex) {            
            throw Throwables.propagate(ex);
        }
        
    }
    public static void createTurboSet(){        
        TurboSet<Integer> lst = new TurboSet<Integer>(TSERIALIZER,TDESERIALIZER);
        printListCharacteristics(lst);
        workOnTurboSet(lst);
    }

    public static void createTurboSet(String dbName){
        TurboSet<Integer> lst = new TurboSet<Integer>(dbName,TSERIALIZER,TDESERIALIZER);
        printListCharacteristics(lst);
        workOnTurboSet(lst);
    }

    public static void createTurboSet(String dbPath,String dbName){
        TurboSet<Integer> lst = new TurboSet<Integer>(dbPath,dbName,TSERIALIZER,TDESERIALIZER);
        printListCharacteristics(lst);
        workOnTurboSet(lst);
    }

    public static void createTurboSet(String dbPath,String dbName,int cacheSize){
        TurboSet<Integer> lst = new TurboSet<Integer>(dbPath,dbName,cacheSize,TSERIALIZER,TDESERIALIZER);
        printListCharacteristics(lst);
        workOnTurboSet(lst);
    }

    public static void createTurboSet(String dbPath,String dbName,int cacheSize,int bloomfilterSize){
        TurboSet<Integer> lst = new TurboSet<Integer>(dbPath,dbName,cacheSize,bloomfilterSize,TSERIALIZER,TDESERIALIZER);
        printListCharacteristics(lst);
        workOnTurboSet(lst);
    }

    public static void createTurboSetOveridePathAndNameWhileDSer(String dbPath,String dbName,int cacheSize,int bloomfilterSize){
        System.setProperty(LargeCollection.OVERRIDE_DB_PATH, dbPath);        
        System.setProperty(LargeCollection.OVERRIDE_DB_NAME, dbName);
        TurboSet<Integer> lst = new TurboSet<Integer>(dbPath,dbName,cacheSize,bloomfilterSize,TSERIALIZER,TDESERIALIZER);
        printListCharacteristics(lst);
        workOnTurboSet(lst);
    }

}
