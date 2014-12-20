package com.axiomine.largecollections.generator.client;

import com.axiomine.largecollections.generator.GeneratorPrimitiveKeyKryoValue;

public class GeneratePrimitiveKeyKryoValueMaps {
    public static void main(String[] args) throws Exception {
        // String[] keys={"String","Integer","Long","Double","Float"};
        //String[] vals = { "String", "Integer", "Long", "Double", "Float" };
        String[] vals = {"String","Integer","Long","Double","Float","Byte","Character","byte[]"};;
        String myPackage = "com.axiomine.largecollections.turboutil";
        String customImports = "-";
        String vPackage = "com.axiomine.largecollections.serdes";
        String vClass = "";
        for (String v : vals) {
            vClass = v;
            String[] myArgs = { myPackage, customImports, vPackage,vClass };
            GeneratorPrimitiveKeyKryoValue.main(myArgs);
        }
    }
}
