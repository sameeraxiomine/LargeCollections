package com.axiomine.largecollections.generator.client;

import org.apache.hadoop.io.Text;

import com.axiomine.largecollections.generator.GeneratorPrimitiveKeyWritableValue;

public class GeneratePrimitiveKeyWritableValueMaps {
    public static void main(String[] args) throws Exception{
        String[] keys={"String","byte[]"};
        String[] vals={"Text","IntWritable"};
        String[] vwcls={"org.apache.hadoop.io.Text","org.apache.hadoop.io.IntWritable"};
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
                String[] myArgs = {myPackage,customImports,kPackage,vPackage,kClass,vClass,vwcls[i]};
                GeneratorPrimitiveKeyWritableValue.main(myArgs);
                i++;
            }
        }
    }
}
