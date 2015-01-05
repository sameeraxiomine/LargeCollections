package com.axiomine.largecollections.generator;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

public class GeneratorPrimitiveSet {
    
    /*
     * Sample invocation
     * java GeneratorPrimitivePrimitive com.axiomine.largecollections - com.axiomine.largecollections.functions com.axiomine.largecollections.functions Integer Integer
     */
    public static void main(String[] args) throws Exception{
        //Package of the new class you are generating Ex. com.mypackage
        String MY_PACKAGE = args[0];
        //Any custom imports you need (: seperated). Use - if no custom imports are included
        //Ex. java.util.*:java.lang.Random     
        String CUSTOM_IMPORTS = args[1].equals("-")?"":args[1]; 
        //Package of your Key serializer class. Use com.axiomine.bigcollections.functions
        String T = args[2];
        
        String tCls = T;
        
        if(tCls.equals("byte[]")){
            tCls = "BytesArray";
        }
        String CLASS_NAME=tCls+"Set";
        File root = new File("");
        File outFile = new File(root.getAbsolutePath() + "/src/main/java/"+MY_PACKAGE.replaceAll("\\.", "/")+"/"+CLASS_NAME+".java");

        if(outFile.exists()){
            System.out.println(outFile.getAbsolutePath() +" already exists. Please delete it and try again");
        }
        {
            String[] imports = null;
            String importStr = "";

            if(!StringUtils.isBlank(CUSTOM_IMPORTS)){
                CUSTOM_IMPORTS.split(":");
                for(String s:imports){
                    importStr="import "+s+";\n";
                }
            }

            String program = FileUtils.readFileToString(new File(root.getAbsolutePath()+"/src/main/resources/PrimitiveSetTemplate.java"));

            program = program.replaceAll("#MY_PACKAGE#", MY_PACKAGE);
            program = program.replaceAll("#CUSTOM_IMPORTS#", importStr);
            
            program = program.replaceAll("#CLASS_NAME#", CLASS_NAME);
            program = program.replaceAll("#T#", T);
            program = program.replaceAll("#TCLS#", tCls);

            System.out.println(outFile.getAbsolutePath());
            FileUtils.writeStringToFile(outFile, program);
        }
    }
}
