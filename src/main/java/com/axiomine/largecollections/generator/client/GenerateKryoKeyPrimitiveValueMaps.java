package com.axiomine.largecollections.generator.client;

import com.axiomine.largecollections.generator.GeneratorKryoKPrimitiveValue;

public class GenerateKryoKeyPrimitiveValueMaps {
    public static void main(String[] args) throws Exception {
        String[] vals={"String","Integer","Long","Double","Float","Byte","Character","byte[]"};
        
        
        String myPackage = "com.axiomine.largecollections.turboutil";
        String customImports = "-";
        String vPackage = "com.axiomine.largecollections.serdes";
        String vClass = "";
        for (String v : vals) {
            vClass = v;
            String[] myArgs = { myPackage, customImports, vPackage,vClass };
            GeneratorKryoKPrimitiveValue.main(myArgs);
        }
    }
}
