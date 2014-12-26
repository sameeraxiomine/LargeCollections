package com.axiomine.largecollections.serdes;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.serdes.CharacterSerDes;

public class CharacterSerDeTest {
    
    @Test
    public void test() {
        CharacterSerDes.SerFunction cser = new CharacterSerDes.SerFunction();
        CharacterSerDes.DeSerFunction cdeser = new CharacterSerDes.DeSerFunction();
        
        char c1 = 'a';
        byte[] cba = cser.apply(c1);
        char c2 = cdeser.apply(cba);
        Assert.assertEquals(c1, c2);

        c1 = 0x01;
        cba = cser.apply(c1);
        c2 = cdeser.apply(cba);
        Assert.assertEquals(c1, c2);

    }
    
}
