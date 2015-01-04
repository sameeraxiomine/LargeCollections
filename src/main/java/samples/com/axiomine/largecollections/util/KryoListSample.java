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
import com.axiomine.largecollections.util.KryoList;
import com.axiomine.largecollections.util.TurboKVMap;
import com.axiomine.largecollections.util.KryoKVMap;
import com.axiomine.largecollections.util.LargeCollection;
import com.axiomine.largecollections.util.TurboList;
import com.axiomine.largecollections.utilities.FileSerDeUtils;
import com.google.common.base.Function;
import com.google.common.base.Throwables;

public class KryoListSample {

    public static void main(String[] args) {
        createTurboList();
        System.out.println("Create Map overriding the dbName");  
        createTurboList("KVMAP1");
        System.out.println("Create Map overriding the dbPath,dbName");
        createTurboList(System.getProperty("java.io.tmpdir"),"KVMAP2");
        System.out.println("Create Map overriding the dbPath,dbName,cacheSize(in MB)");
        createTurboList(System.getProperty("java.io.tmpdir"),"KVMAP3",50);
        System.out.println("Create Map overriding the dbPath,dbName,cacheSize(in MB),bloomFilterSize");
        createTurboList(System.getProperty("java.io.tmpdir"),"KVMAP4",50,1000);
        System.out.println("Override the path and name at the time of Deserialization.Create Map overriding the dbPath,dbName,cacheSize(in MB),bloomFilterSize");
        createTurboListOveridePathAndNameWhileDSer("c:/tmp","KVMAP",50,1000);
    }
    
    public static void printListCharacteristics(LargeCollection m){
        System.out.println("DB Path="+m.getDBPath());
        System.out.println("DB Name="+m.getDBName());
        System.out.println("Cache Size="+m.getCacheSize() + "MB");
        System.out.println("Bloomfilter Size="+m.getBloomFilterSize());
    }
    
    
    public static void workOnKryoList(KryoList<Integer> lst){
        try {
            ListIterator<Integer> it = lst.listIterator();
            
            for (int i = 0; i < 10; i++) {
                boolean b = lst.add(i);
                Assert.assertEquals(true, true);
            }
            System.out.println("Size of map="+lst.size());
            System.out.println("Value for key 0="+lst.get(0));;
            System.out.println("Now remove key 0");
            try{
                int i = lst.remove(0);    
            }
            catch(Exception ex){
                System.out.println(ex.getMessage());
            }
            
            int i = lst.remove(9);
            System.out.println("Value for deleted key="+i);;
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
            i = lst.set(9,99);
            Assert.assertEquals(99, i);
            i = lst.set(5,55);
            Assert.assertEquals(55, i);
            i = lst.set(0,100);
            Assert.assertEquals(100, i);
            System.out.println(lst.get(0));
            System.out.println(lst.get(5));
            System.out.println(lst.get(9));
            System.out.println("Now put worked. Size of map should be 10. Size of the map ="+lst.size());

            Iterator<Integer> iter = lst.iterator(); 
            try{
                while(iter.hasNext()){
                    i = iter.next();
                    System.out.println("From ITerator = "+i);
                }
            }
            finally{
                //Always close and iterator after use. Otherwise you will not be able to call the clear function
                ((Closeable)iter).close();    
            }
            
            ListIterator<Integer> lstIter = lst.listIterator();
            try{
                while(lstIter.hasNext()){
                    i = lstIter.next();
                    System.out.println("From List Iterator = "+i);
                    System.out.println("From List Iterator Next Index= "+lstIter.nextIndex());
                }
            }
            finally{
                //Always close and iterator after use. Otherwise you will not be able to call the clear function
                ((Closeable)lstIter).close();    
            }
            

            lstIter = lst.listIterator(5);
            try{
                while(lstIter.hasNext()){
                    i = lstIter.next();
                    System.out.println("From List Iterator = "+i);
                    System.out.println("From List Iterator Next Index= "+lstIter.nextIndex());
                }
            }
            finally{
                //Always close and iterator after use. Otherwise you will not be able to call the clear function
                ((Closeable)lstIter).close();    
            }
            
            System.out.println("---");
            lstIter = lst.listIterator(9);
            try{
                while(lstIter.hasPrevious()){
                    i = lstIter.previous();
                    System.out.println("From List Iterator Previous= "+i);
                    System.out.println("From List Iterator Previous Index= "+lstIter.previousIndex());
                }
            }
            finally{
                //Always close and iterator after use. Otherwise you will not be able to call the clear function
                ((Closeable)lstIter).close();    
            }
            
            System.out.println("----------------------------------");
            lstIter = lst.listIterator();
            try{
                while(lstIter.hasNext()){
                    i = lstIter.next();
                    System.out.println("Iterating Forward = "+i);
                }

            }
            finally{
                //Always close and iterator after use. Otherwise you will not be able to call the clear function
                ((Closeable)lstIter).close();    
            }

            
            
            System.out.println("Now Serialize the List");
            File serFile = new File(System.getProperty("java.io.tmpdir")+"/x.ser");
            FileSerDeUtils.serializeToFile(lst,serFile);            
            System.out.println("Now De-Serialize the List");
            lst = (KryoList<Integer>) FileSerDeUtils.deserializeFromFile(serFile);
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
    public static void createTurboList(){        
        KryoList<Integer> lst = new KryoList<Integer>();
        printListCharacteristics(lst);
        workOnKryoList(lst);
    }

    public static void createTurboList(String dbName){
        KryoList<Integer> lst = new KryoList<Integer>(dbName);
        printListCharacteristics(lst);
        workOnKryoList(lst);
    }

    public static void createTurboList(String dbPath,String dbName){
        KryoList<Integer> lst = new KryoList<Integer>(dbPath,dbName);
        printListCharacteristics(lst);
        workOnKryoList(lst);
    }

    public static void createTurboList(String dbPath,String dbName,int cacheSize){
        KryoList<Integer> lst = new KryoList<Integer>(dbPath,dbName,cacheSize);
        printListCharacteristics(lst);
        workOnKryoList(lst);
    }

    public static void createTurboList(String dbPath,String dbName,int cacheSize,int bloomfilterSize){
        KryoList<Integer> lst = new KryoList<Integer>(dbPath,dbName,cacheSize,bloomfilterSize);
        printListCharacteristics(lst);
        workOnKryoList(lst);
    }

    public static void createTurboListOveridePathAndNameWhileDSer(String dbPath,String dbName,int cacheSize,int bloomfilterSize){
        System.setProperty(LargeCollection.OVERRIDE_DB_PATH, dbPath);        
        System.setProperty(LargeCollection.OVERRIDE_DB_NAME, dbName);
        KryoList<Integer> lst = new KryoList<Integer>(dbPath,dbName,cacheSize,bloomfilterSize);
        printListCharacteristics(lst);
        workOnKryoList(lst);
    }

}
