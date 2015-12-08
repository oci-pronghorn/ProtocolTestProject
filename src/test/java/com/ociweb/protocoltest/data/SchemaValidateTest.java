package com.ociweb.protocoltest.data;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.ociweb.pronghorn.pipe.util.build.FROMValidation;

public class SchemaValidateTest {

    //TODO: Check that all other tests end with Test for maven as well. 
    
    @Test
    public void validateSequenceExampleATest() {
        assertTrue(FROMValidation.testForMatchingFROMs("/sequenceExampleA.xml", SequenceExampleASchema.instance));
        assertTrue(FROMValidation.testForMatchingLocators(SequenceExampleASchema.instance)); 
    }
    
    
}

    
    
    