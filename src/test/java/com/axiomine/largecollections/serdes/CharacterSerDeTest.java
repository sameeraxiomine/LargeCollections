package com.axiomine.largecollections.serdes;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.CharacterSerDes;

public class CharacterSerDeTest {
    
    @Test
    public void test() {
        TurboSerializer<Character> ser = new CharacterSerDes.SerFunction();
        TurboDeSerializer<Character> deser = new CharacterSerDes.DeSerFunction();

        char c1 = 'a';
        byte[] cba = ser.apply(c1);
        char c2 = deser.apply(cba);
        Assert.assertEquals(c1, c2);

        c1 = 0x01;
        cba = ser.apply(c1);
        c2 = deser.apply(cba);
        Assert.assertEquals(c1, c2);

    }
    
}
