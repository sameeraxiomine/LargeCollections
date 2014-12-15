# LargeCollections #

LargeCollections supports java.util.Map implementation which is backed by LevelDB. This allows your collections to grow very large as it does not use the JVM heap memory.

Currently only the java.util.Map is supported. The support is complete, in that it supports the underlying iterators as well.

#Key Differentiator wrt to other Libraries#

What makes this java.util.Map different? It is hard to provide a general implementation for LevelDB back store. We had tried to do that with LargeCollections. However if we depend on Java Serialization to convert objects to/from bytes the overall throughput of the collection API drops. Hence we used a new approach. Instead of providing pre-developed collections like FastUtil does we provide you Collection Generation templates. You can develop your own serialization plugins if you do not like the ones provided. Using these serialization plugins you can simply generate your own java.util collections which are ultra-fast. 

#Key Design Principles#
The underlying java.util.Map implementations are backed by [https://github.com/google/leveldb](https://github.com/google/leveldb "LevelDB"). LevelDB is a fast key-value storage library written at Google that provides an ordered mapping from byte array keys to byte array values.

While LargeCollections supports any Serializable/Externalizable/Writable/Kryo-Serializable Key and Value classes, the underlying implementations stores everything as byte-array key value store (similar to HBase). Thus every key/value instance needs to be converted to a byte array to write to the LevelDB backing store and convert back from byte array to a Java instance.

To support the to-fro byte-array conversions, every java.util.Map sub-class provided by LargeCollections library needs to have Serializer-Deserializer (SerDes) pair, one each for Key and Value class. 

You are unlikely to use this library unless your Map's are so large that that you cannot hold them in your JVM Heap nor are they so large that you need a No-SQL database. Hence it was important to select the fasted possible serialization/deserialization mechanisms. The default java serialization is support but we have done considerably better. Below is a list of various mechanisms used for SerDes-
1. Kryo - This offers better control over serialization. Also the interface is considerably simpler and non-intrusive from the Key/Value class design. It support the default java serialization via the Serializable and Externalizable interface
2. Custom SerDes for primitive types - Where possible we will utilize highly customized mechanisms. For example Google Guava supports a ultra-fast conversions from int to byte-array and back. Likewise it is lot simpler to invoke `String.getBytes()` and new String(byte[]) to serialize and deserialize strings. 
3. For basic `org.apache.hadoop.io.Writable` types we provide very fast Kryo SerDes

We will discuss all the above methods and how to utilize them in detail in the subsequent sections.  



Out of box SerDes
================
We provide the following SerDe (Serialization/Deserialization) classes out of the box. They implement the com.google.common.base.Function interface. You can create your own by implementing the same interface and utilize the provided templates to generate your own Map classes-

01. com.axiomine.LargeCollections.functions.BytesArraySerDe

02. com.axiomine.LargeCollections.functions.ByteSerDe

03. com.axiomine.LargeCollections.functions.CharacterSerDe

04. com.axiomine.LargeCollections.functions.StringSerDe

05. com.axiomine.LargeCollections.functions.IntegerSerDe

06. com.axiomine.LargeCollections.functions.LongSerDe

07. com.axiomine.LargeCollections.functions.FloatSerDe

08. com.axiomine.LargeCollections.functions.DoubleSerDe

09. com.axiomine.LargeCollections.functions.SerializableSerDe 

10. com.axiomine.LargeCollections.functions.ExternalizableSerDe

11. com.axiomine.LargeCollections.functions.KryoSerDe

What's coming
================
In the next few weeks we will add the following implementations

1. java.util.List with most functions implemented. The function which will not be implemented is the ability to insert records in the middle of the list. Records cannot be deleted but can be replaced.
2. java.util.Set 


We will be adding more documentation in the coming days.

Usage
================

See examples of usage by following the Unit Test cases from the 
src/test/java folder

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
