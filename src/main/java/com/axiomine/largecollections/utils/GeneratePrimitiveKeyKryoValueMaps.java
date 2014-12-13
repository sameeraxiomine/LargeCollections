package com.axiomine.largecollections.utils;

public class GeneratePrimitiveKeyKryoValueMaps {
    public static void main(String[] args) throws Exception {
        // String[] keys={"String","Integer","Long","Double","Float"};
        String[] vals = { "String", "Integer", "Long", "Double", "Float" };
        String myPackage = "com.axiomine.largecollections";
        String customImports = "-";
        String vPackage = "com.axiomine.largecollections.functions";
        String vClass = "";
        for (String v : vals) {
            vClass = v;
            String[] myArgs = { myPackage, customImports, vPackage,vClass };
            KryoGeneratorPrimitiveObject.main(myArgs);
        }
    }
}
