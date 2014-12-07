package com.axiomine.largecollections.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.Serializable;

import com.google.common.base.Throwables;

public class FileSerDeUtils {
    public  static void serializeToFile(Serializable obj,File f) {
        FileOutputStream fileOut = null;
        try{
            fileOut =
                    new FileOutputStream(f);
            org.apache.commons.lang.SerializationUtils.serialize(obj, fileOut);
        }
        catch(Exception ex){
            throw Throwables.propagate(ex);
        }
        finally{
            try{
                if(fileOut!=null){
                    fileOut.close();
                }  
            }
            catch(Exception ex2){
                throw Throwables.propagate(ex2);
            }

        }
    }

    public  static Object deserializeFromFile(File f) {
        FileInputStream fileIn = null;
        Object m = null;
        try
        {            
           fileIn = new FileInputStream(f);
           m = org.apache.commons.lang.SerializationUtils.deserialize(fileIn);           
           
        }catch(Exception ex)
        {
            try{
                if(fileIn!=null){
                    fileIn.close();
                }  
            }
            catch(Exception ex2){
                throw Throwables.propagate(ex2);
            }
           
        }
        return m;
    }

}
