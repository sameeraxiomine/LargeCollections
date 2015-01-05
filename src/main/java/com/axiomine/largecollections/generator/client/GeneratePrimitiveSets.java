package com.axiomine.largecollections.generator.client;

import com.axiomine.largecollections.generator.GeneratorPrimitiveSet;

public class GeneratePrimitiveSets {
    public static void main(String[] args) throws Exception{
        String[] tclses={"String","Integer","Long","Double","Float","Byte","Character","byte[]"};
        String myPackage = "com.axiomine.largecollections.turboutil";
        String customImports = "-";
        String tClass="";
        
        for(String t:tclses){
            tClass=t;
            String[] myArgs = {myPackage,customImports,tClass};
            GeneratorPrimitiveSet.main(myArgs);
        }
    }
}
