# LargeCollections #

LargeCollections supports java.util.Map implementation which is backed by LevelDB. This allows your collections to grow very large as it does not use the JVM heap memory.

Currently only the java.util.Map is supported. The support is complete, in that it supports the underlying iterators as well.

The Javadoc for this project can be found [here](http://sameeraxiomine.github.io/largecollections/ "here").

#Key Design Principles#
The underlying java.util.Map implementations are backed by [https://github.com/google/leveldb](https://github.com/google/leveldb "LevelDB"). LevelDB is a fast key-value storage library written at Google that provides an ordered mapping from byte array keys to byte array values.

While LargeCollections supports any Serializable/Externalizable/Writable/Kryo-Serializable Key and Value classes, the underlying implementations stores everything as byte-array key value store (similar to HBase). Thus every key/value instance needs to be converted to a byte array to write to the LevelDB backing store and convert back from byte array to a Java instance.

To support the conversions from object to byte-array and back, every java.util.Map sub-class provided by LargeCollections library needs to have Serializer-Deserializer (SerDes) pair, one each for Key and Value class. 

# When should you use LargeCollections #

The should use LargeCollections in the following situations

1. The size of your Map is larger than over 50-100MB

2. The size of your Key-Value store is not so large as to justify a NoSQL store like HBase or Cassandra

You you find yourself wishing you could use an in-process MongoDB instance, you should consider using LargeCollections. It is designed to be an in-process persistent Map which allows you to store a few million entries.


# Usage #
The best way to learn how to use the API is to follow the samples contained in the `samples` package. Alternatively the comprehensive list of unit test cases also demonstrate how to use the library.

The main thing to remember when using this library is, it supports the java.util.Map interface. The only method not supported (for performance reason) is  `public boolean containsValue(Object value)`

## Map Attributes ##
We provide implementations of java.util.Map. Each of these Map implementations has the following attributes


1. `dbPath` - This is a folder where the LevelDB named cache is created. Default value is the folder provided by the java System property `java.io.tmpdir`

2. `dbName` - The name of LevelDB Cache. Each Map should have a unique LevelDB backing database. The default value of this attribute is a value which is randomly generated. You can override it with your own user-defined name

3. `cacheSize` - LevelDB uses an internal cache to improve its response time. This parameter is specified in MB. Its default value is `25`.

4. `bloomFilterSize` - Internally we use bloom filter to make swift determination of if a key is contained in the underlying LevelDB database. BloomFilters need to know at the outset the approximate number of elements they will contain. Underestimating this size will lead to more false positives and consequently degrade your performance as the LevelDB will be checked for a key even when the key does not exist. Default value is `10000000` (10 million)
 
## Map Constructors ##
The constructors supported for each of the Map's are as follows-

1. () - All default values Map attributes mentioned above. Note that the dbName is a randomly generated attribute for each map.

2. (dbName) - Override the dbName attribute above. Other attributes take on default values

3. (dbPath,dbName) - Override the dbPath and dbName attributes

4. (dbPath,dbName,cacheSize) - Override the dbPath,dbName,cacheSize attributes

5. (dbPath,dbName,cacheSize,bloomFilterSize) - Override the dbPath,dbName,cacheSize,bloomFilterSize attributes

LargeCollections provides a Map implementations based on how the SerDes classes are provided for K and V class types-

1. Turbo SerDes are highly custom SerDes for primitive (mostly) types which are designed to be very efficient. The following turbo serdes classes are provided
	- 	`com.axiomine.largecollections.serdes.IntegerSerdes` 
	- 	`com.axiomine.largecollections.serdes.LongSerdes 	`
	- 	`com.axiomine.largecollections.serdes.FloatSerdes`
	- 	`com.axiomine.largecollections.serdes.DoubleSerdes`
	- 	`com.axiomine.largecollections.serdes.StringSerdes`
	- 	`com.axiomine.largecollections.serdes.CharacterSerdes`
	- 	`com.axiomine.largecollections.serdes.ByteSerdes`
	- 	`com.axiomine.largecollections.serdes.ByteArraySerdes`
 
2. `com.axiomine.largecollections.serdes.KryoSerdes` class utilizes [Kryo](https://github.com/EsotericSoftware/kryo/ "Kryo") for Serialization and Deserialization. More details on how to register custom Kryo Serializers will be discussed in the next sections.
3. `com.axiomine.largecollections.serdes.WritableSerdes` class provides SerDes for `org.hadoop.io.Writable` implementations. There is only one Serialization implementation and several deserialization implementations. Below are the constructors to various Deserialization implementations
	- 	`public DeSerFunction(Class<? extends Writable> wCls)`  - Utilize this you are using your own custom `Writable` implementation.
	- 	`public ArrayPrimitiveWritableDeSerFunction()` - Utilize this when your Key or Value is of type `ArrayPrimitiveWritable`
	- 	`public BooleanWritableDeSerFunction()` - Utilize this when your Key or Value is of type `BooleanWritable`
	- 	`public BytesWritableDeSerFunction()` - Utilize this when your Key or Value is of type `BytesWritable` 
	- 	`public DoubleWritableDeSerFunction()` - Utilize this when your Key or Value is of type `DoubleWritable` 
	- 	`public FloatWritableDeSerFunction()` - Utilize this when your Key or Value is of type `FloatWritable` 
	- 	`public LongWritableDeSerFunction()` - Utilize this when your Key or Value is of type `LongWritable` 
	- 	`public MapWritableDeSerFunction()` - Utilize this when your Key or Value is of type `MapWritable` 
	- 	`public ShortWritableDeSerFunction()` - Utilize this when your Key or Value is of type `ShortWritable` 
	- 	`public TextDeSerFunction()` - Utilize this when your Key or Value is of type `Text` 
For each of the above `Writable` implementation classes, Kryo Serializers have been provided.

## Map Types Supported ##
There 5 main types of Maps

1. TurboKVMap<K,V> - This implementation of `java.util.Map` expects you to provide SerDes classes for key and value classes used with this map. Each of the above constructor will take four more trailing parameters

	- 	kSerClass This is a Serializer class used to serialize K instance. For example "`com.axiomine.largecollections.serdes.IntegerSerDes$SerFunction`" if K is of type `java.lang.Integer`
	- 	vSerClass This is a Serializer class used to serialize V instance. For example "`com.axiomine.largecollections.serdes.IntegerSerDes$SerFunction`" if V is of type `java.lang.Integer`
	- 	kDeSerClass This is a DeSerializer class used to deserialize K instance. For example "`com.axiomine.largecollections.serdes.IntegerSerDes$DeSerFunction`" if K is of type `java.lang.Integer`
	- 	vDeSerClass This is a DeSerializer class used to deserialize V instance. For example "`com.axiomine.largecollections.serdes.IntegerSerDes$DeSerFunction`" if V is of type `java.lang.Integer`

	For examples of how to use this class see `samples.FastKVSample.java`

    `String KSERIALIZER = "com.axiomine.largecollections.serdes.IntegerSerDes$SerFunction";`
    `String VSERIALIZER = "com.axiomine.largecollections.serdes.IntegerSerDes$SerFunction";`
    `String KDESERIALIZER = "com.axiomine.largecollections.serdes.IntegerSerDes$DeSerFunction";`
    `String VDESERIALIZER = "com.axiomine.largecollections.serdes.IntegerSerDes$DeSerFunction";`
    `java.util.Map<Integer,Integer> map = new FastKVMap<Integer,Integer>(KSERIALIZER,VSERIALIZER,KDESERIALIZER,VDESERIALIZER);`
    `//Use it like a regular java.util.Map`
        
2.  KryoKVMap<K,V> - This implementation of `java.util.Map` utilizes Kryo for high performance serialization and deserialization. For all the standard primitive types Kryo provides default serializers. LargeCollections also provides Kryo Serializers for the following standard org.hadoop.io.Writable implementations in the package `com.axiomine.largecollections.kryo.serializers`
	- 	`ArrayPrimitiveWritable` 
	- 	`BooleanWritable`
	- 	`BytesWritable`
	- 	`ByteWritable`
	- 	`DoubleWritable`
	- 	`FloatWritable`
	- 	`IntWritable`
	- 	`LongWritable`
	- 	`MapWritable`
	- 	`ShortWritable`
	- 	`Text`	

You should review the classes in the package `com.axiomine.largecollections.kryo.serializers` for examples of how to write your own KryoSerializers. The Kryo documentation is the best resource on how to write your own Kryo Serializers

You do have to register your own KryoSerializers. For example `com.axiomine.largecollections.kryo.serializers.MyIntSerializer` is an example of the custom Kryo serializer. If you want to register your own serializer you should create a property file and make entries as follows
    `java.lang.Integer=com.axiomine.largecollections.kryo.serializers.MyIntSerializer`
`java.lang.Float=com.axiomine.largecollections.kryo.serializers.MyFloatSerializer`

An example file is in location `src/test/resources/KryoRegistration.properties`. 


#SerDes#

It was important to select the fasted possible serialization/deserialization mechanisms. The default java serialization is supported but we have done considerably better. Below is a list of various mechanisms used for SerDes-

1. Kryo - This offers better control over serialization. Also the interface is considerably simpler and non-intrusive from the Key/Value class design. It support the default java serialization via the Serializable and Externalizable interface

2. Custom SerDes for primitive types - Where possible we will utilize highly customized mechanisms. For example, it is lot simpler (an faster) to invoke `String.getBytes()` and new String(byte[]) to serialize and deserialize strings. 

3. For basic `org.apache.hadoop.io.Writable` types we provide very fast Kryo SerDes

We will discuss all the above methods and how to utilize them in detail in the subsequent sections.  


##Out of box SerDes##

We provide the following SerDe (Serialization/Deserialization) classes out of the box. They implement the com.google.common.base.Function interface. You can create your own by implementing the same interface and utilize the provided templates to generate your own Map classes-

01. `com.axiomine.largeCollections.serdes.BytesArraySerDes`

02. `com.axiomine.largeCollections.serdes.ByteSerDes`

03. `com.axiomine.largeCollections.serdes.CharacterSerDes`

04. `com.axiomine.largeCollections.serdes.StringSerDes`

05. `com.axiomine.largeCollections.serdes.IntegerSerDes`

06. `com.axiomine.largeCollections.serdes.LongSerDes`

07. `com.axiomine.largeCollections.serdes.FloatSerDes`

08. `com.axiomine.largeCollections.serdes.DoubleSerDes`

09. `com.axiomine.largeCollections.serdes.SerializableSerDes` 

10. `com.axiomine.largeCollections.serdes.ExternalizableSerDes`

11. `com.axiomine.largeCollections.serdes.KryoSerDes`

At this point let us examine a sample Serdes class, `StringSerdes`

    package com.axiomine.largecollections.serdes;

    import com.google.common.base.Function;
    
    public class StringSerDes {
    
		public static class SerFunction implements Function<String,byte[]>{
	    	public byte[] apply(String arg) {
	    		if(arg==null){
	    			return null;
	    		}
	    		else{
	    			return arg.getBytes();
			    }
		    }
	    }
    
	    public static class DeSerFunction implements Function<byte[],String>{
			    public String apply(byte[] arg) {
	    			if(arg==null){
	    				return null;
				    }
	    			else{
	    				return new String(arg);
	    			}
	    		}
	    }    
    }


What's coming
================
In the next few weeks we will add the following implementations

1. java.util.List with most functions implemented. The function which will not be implemented is the ability to insert records in the middle of the list. Records cannot be deleted but can be replaced.
2. java.util.Set 


We will be adding more documentation in the coming days.


License
================

Copyright 2014 Axiomine
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
