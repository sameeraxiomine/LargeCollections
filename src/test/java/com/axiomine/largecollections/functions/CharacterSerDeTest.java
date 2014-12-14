package com.axiomine.largecollections.functions;

import static org.junit.Assert.*;
import junit.framework.Assert;

import org.junit.Test;

import com.axiomine.largecollections.functions.CharacterSerDe;

public class CharacterSerDeTest {
    
    @Test
    public void test() {
        CharacterSerDe.SerFunction cser = new CharacterSerDe.SerFunction();
        CharacterSerDe.DeSerFunction cdeser = new CharacterSerDe.DeSerFunction();
        
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
