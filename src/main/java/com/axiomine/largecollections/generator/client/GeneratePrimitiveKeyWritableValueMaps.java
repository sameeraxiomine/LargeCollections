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

import org.apache.hadoop.io.Text;

import com.axiomine.largecollections.generator.GeneratorPrimitiveKeyWritableValue;

public class GeneratePrimitiveKeyWritableValueMaps {
    public static void main(String[] args) throws Exception{
        String[] keys={"String","Integer","Long","Double","Float","Byte","Character","byte[]"};;
        String[] vals={"ArrayPrimitiveWritable","BooleanWritable","BytesWritable","ByteWritable","DoubleWritable","FloatWritable","IntWritable","LongWritable","MapWritable","ShortWritable","Text",};
        String writablePackage = "org.apache.hadoop.io";
        //String[] vwcls={"org.apache.hadoop.io.Text","org.apache.hadoop.io.IntWritable"};
        Text t = null;
        //String[] keys={"String"};
        //String[] vals={"Text"};

        String myPackage = "com.axiomine.largecollections.turboutil";
        String customImports = "-";
        String kPackage = "com.axiomine.largecollections.functions";
        String vPackage = "com.axiomine.largecollections.functions";
        String kClass="";
        String vClass="";
        for(String k:keys){
            kClass=k;
            int i = 0;
            for(String v:vals){
                vClass=v;
                String[] myArgs = {myPackage,customImports,kPackage,vPackage,kClass,vClass,writablePackage+"."+v};
                GeneratorPrimitiveKeyWritableValue.main(myArgs);
                i++;
            }
        }
    }
}
