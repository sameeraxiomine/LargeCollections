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
