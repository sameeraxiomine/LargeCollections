# LargeCollections #
================

LargeCollections supports java.util.Map implementation which is backed by LevelDB. This allows your collections to grow very large as it does not use the JVM heap memory.

Currently only the java.util.Map is supported. The support is complete, in that it supports the underlying iterators as well.

#Key Differentiator wrt to other Libraries#
================================================

What makes this java.util.Map different? It is hard to provide a general implementation for LevelDB back store. We had tried to do that with LargeCollections. However if we depend on Java Serialization to convert objects to/from bytes the overall throughput of the collection API drops. Hence we used a new approach. Instead of providing pre-developed collections like FastUtil does we provide you Collection Generation templates. You can develop your own serialization plugins if you do not like the ones provided. Using these serialization plugins you can simply generate your own java.util collections which are ultra-fast. 


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
