package com.axiomine.largecollections.util;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.iq80.leveldb.CompressionType;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import com.axiomine.largecollections.utilities.KryoUtils;
import com.esotericsoftware.kryo.Kryo;
import com.google.common.base.Throwables;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;

public abstract class LargeCollection implements IDb  {
    public static final Random rnd = new Random();
    public static String OVERRIDE_DB_PATH="override.dbpath";
    public static String OVERRIDE_DB_NAME="override.dbname";
    public static String OVERRIDE_BF_FPP="override.bf.fpp";
    
    public static int DEFAULT_CACHE_SIZE = 25;
    public static int DEFAULT_BLOOM_FILTER_SIZE = 10000000;
    
    
    protected String dbPath=System.getProperty("java.io.tmpdir");
    protected String dbName = "f"+System.currentTimeMillis()+rnd.nextInt();
    protected int cacheSize = DEFAULT_CACHE_SIZE;
    protected int size;
    protected transient DB db;
    protected transient Options options;
    protected transient File dbFile;

    //private SerializationUtils<K, V> serdeUtils = new SerializationUtils<K, V>();
    protected int bloomFilterSize = DEFAULT_BLOOM_FILTER_SIZE;
    protected  transient Funnel<Integer> myFunnel = null;
    protected BloomFilter bloomFilter = null;
    protected transient boolean opened=false;
    //protected transient Kryo kryo = null;
    //protected boolean kryoUsed = false;

    protected ThreadLocal<Kryo> kryos = null;
    
    
    public ThreadLocal<Kryo> getThreadLocalKryos(){
        if(kryos==null){
            kryos = KryoUtils.getThreadLocalKryos();
            this.configureKryoSerializers();
        }
        return  this.kryos;
    }
    
    public void configureKryoSerializers(){
       Kryo kryo = this.getThreadLocalKryos().get();
       //Configure Kryo classes
    }
    
    public Kryo getKryo(){
        return this.getThreadLocalKryos().get();
    }
    
    protected void initializeBloomFilter(){
        this.myFunnel = new Funnel() {
            public void funnel(Object obj, PrimitiveSink into) {
                into.putInt(obj.hashCode());                  
              }
            }; 
        float defaultFalsePositives = 0.03f;
        if(!StringUtils.isBlank(System.getProperty(LargeCollection.OVERRIDE_BF_FPP))){
            String fpp = System.getProperty(LargeCollection.OVERRIDE_BF_FPP);
            try{
                float f = Float.parseFloat(fpp);
                if(f<=0 || f>0.2){
                    throw new RuntimeException("Bloom filter false postives probability range should be between 0 (excluded) and 0.2 (included), provided value = "+f);
                }
                else{
                    defaultFalsePositives = f;
                }
            }
            catch(Exception ex){
                throw Throwables.propagate(ex);
            }
        }
        this.bloomFilter = BloomFilter.create(myFunnel, this.bloomFilterSize,defaultFalsePositives);
    }
    

    public LargeCollection(){      
        //Override path here
        if(!StringUtils.isBlank(System.getProperty(OVERRIDE_DB_PATH))){
            System.out.println("Overriding DBPath from System Property="+System.getProperty(OVERRIDE_DB_PATH));
            this.dbPath=System.getProperty(OVERRIDE_DB_PATH);
        }
        this.open();
    }

    public LargeCollection(String dbName){
        //Override path here
        if(!StringUtils.isBlank(System.getProperty(OVERRIDE_DB_PATH))){
            System.out.println("Overriding DBPath from System Property="+System.getProperty(OVERRIDE_DB_PATH));
            this.dbPath=System.getProperty(OVERRIDE_DB_PATH);
        }
        this.dbName=dbName;
        this.open();

    }
    public LargeCollection(String dbPath,String dbName){
        this.dbPath=dbPath;
        this.dbName = dbName;
        this.open();
    }

    public LargeCollection(String dbPath,String dbName,int cacheSize){
        this.dbPath=dbPath;
        this.dbName = dbName;
        this.cacheSize = cacheSize;
        this.open();
    }
    
    public LargeCollection(String dbPath,String dbName, int cacheSize,int bloomFilterSize){
        this.dbPath=dbPath;
        this.dbName = dbName;
        this.cacheSize = cacheSize;
        this.bloomFilterSize = bloomFilterSize;
        this.initialize();
        this.initializeBloomFilter();   
    }

    
    protected void registerKryoClasses(){
        //Default is to do nothing. Override this class 
    }

    protected void initialize(){
        this.initialize(true);
    }
    
    protected void initialize(boolean exceptionOnExistingFolder){
        try {
            dbFile = new File(this.dbPath+File.separator+this.dbName);
            options = new Options();
            options.cacheSize(cacheSize * 1048576);
            options.compressionType(CompressionType.SNAPPY);            
            if(!dbFile.exists()){
                dbFile.mkdirs();
            }
            else{
                if(exceptionOnExistingFolder){
                    throw new RuntimeException(dbFile.getAbsolutePath() + " already exists");
                }
            }
            db = factory.open(dbFile,options);       
            this.opened = true;
        } catch (Exception ex) {
            ex.printStackTrace();
            Throwables.propagate(ex);
        }
    }
    

    
    protected void serialize(java.io.ObjectOutputStream stream)
            throws IOException {
        System.out.println("Now serializing " + this.dbName);
        stream.writeObject(this.dbPath);
        stream.writeObject(this.dbName);
        stream.writeInt(this.cacheSize);
        stream.writeInt(this.size);
        stream.writeInt(this.bloomFilterSize);
        stream.writeObject(this.bloomFilter);
        this.db.close();

    }

    protected void deserialize(java.io.ObjectInputStream in) throws IOException,
            ClassNotFoundException {
        this.dbPath = (String) in.readObject();
        this.dbName = (String) in.readObject();
        this.cacheSize = in.readInt();
        this.size = in.readInt();
        this.bloomFilterSize = in.readInt();
        this.bloomFilter = (BloomFilter<Integer>) in.readObject();

        //Override path here
        if(!StringUtils.isBlank(System.getProperty(OVERRIDE_DB_PATH))){
            System.out.println("Overriding DBPath from System Property="+System.getProperty(OVERRIDE_DB_PATH));
            this.dbPath=System.getProperty(OVERRIDE_DB_PATH);
        }
        if(!StringUtils.isBlank(System.getProperty(OVERRIDE_DB_NAME))){
            System.out.println("Overriding DBName from System Property="+System.getProperty(OVERRIDE_DB_NAME));
            this.dbName=System.getProperty(OVERRIDE_DB_NAME);
        }
        
        this.initialize(false);
        System.out.println("Now deserialized " + this.dbName);
    }
    
    public DB getDB() {
        return this.db;
    }
    

    /* Destroys the map */
    
    @Override
    public void close() throws IOException {
        try {
            this.initializeBloomFilter();
            this.db.close();
            //factory.destroy(this.dbFile, this.options);
            this.db=null;//The map should be unusable after close. Must call open first
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }
    @Override
    public void destroy() {
        try {
            if(this.db==null){
                this.open();
            }
            this.initializeBloomFilter();
            this.db.close();
            factory.destroy(this.dbFile, this.options);
            this.db=null;//The map should be unusable after close
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }
    
    @Override
    public void open() {
        try {
            if(!opened){
                this.initialize();    
            }
            else{
                this.initialize(false);
            }
            
            this.initializeBloomFilter();
        } catch (Exception ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    public void clearDB(){
        try{
            if(this.db!=null){
                this.db.close();
                Thread.sleep(1000);
                System.out.println(this.dbFile.getAbsolutePath());
                FileUtils.deleteDirectory(this.dbFile);
            }
            this.size=0;
        }
        catch(Exception ex){
            
            throw Throwables.propagate(ex);
        }
        this.initialize();
        this.initializeBloomFilter();

    }
    
    public String getDBPath(){
        return this.dbPath;
    }
    public String getDBName(){
        return this.dbName;
    }
    public int getCacheSize() {
        return this.cacheSize;
    }
    public int getBloomFilterSize(){
        return this.bloomFilterSize;
    }
    
    public BloomFilter getBloomFilter(){
        return this.bloomFilter;
    }
    public abstract void optimize();
}
