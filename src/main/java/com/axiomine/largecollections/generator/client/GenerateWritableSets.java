/*
 * Copyright 2014 Axiomine
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
package com.axiomine.largecollections.generator.client;


import com.axiomine.largecollections.generator.GeneratorWritableSet;

public class GenerateWritableSets {
    public static void main(String[] args) throws Exception{
        String[] tclses={"ArrayPrimitiveWritable","BooleanWritable","BytesWritable","ByteWritable","DoubleWritable","FloatWritable","IntWritable","LongWritable","MapWritable","ShortWritable","Text"};
        String myPackage = "com.axiomine.largecollections.turboutil";
        String customImports = "-";
        String tClass="";
        
        for(String t:tclses){
            tClass=t;
            String[] myArgs = {myPackage,customImports,tClass};
            GeneratorWritableSet.main(myArgs);
        }
    }
}
