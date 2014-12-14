package com.axiomine.largecollections;

import java.util.Properties;

import com.axiomine.largecollections.generator.client.GenerateKryoKeyPrimitiveValueMaps;
import com.axiomine.largecollections.utilities.KryoUtils;

public class Test {
    
    public static void main(String[] args) throws Exception{
        // TODO Auto-generated method stub
        final Properties props = new Properties();
        props.load(Test.class.getClassLoader().getResourceAsStream("KryoRegistration.properties"));       

    }
    
}
