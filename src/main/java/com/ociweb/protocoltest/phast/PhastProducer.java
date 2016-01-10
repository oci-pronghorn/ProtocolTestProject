package com.ociweb.protocoltest.phast;

import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.App;
import com.ociweb.protocoltest.data.PhastWriter;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGeneratorCustom;

public class PhastProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PhastProducer.class);

    private final StreamRegulator regulator;
    private final int count;
    private final SequenceExampleAFactory testDataFactory;
    private final int groupSize;
    
    public PhastProducer(StreamRegulator regulator, int count, SequenceExampleAFactory testSentDataFactory, int groupSize) {
        this.regulator = regulator;
        this.count = count;
        this.testDataFactory = testSentDataFactory;
        this.groupSize = groupSize;
    }
    
    @Override
    public void run() {
        try {
            PhastWriter pWriter = new PhastWriter();

            OutputStream out = regulator.getOutputStream();
            DataOutputBlobWriter<RawDataSchema> blobWriter = regulator.getBlobWriter();
            long lastNow = 0;
            
            SequenceExampleA writeMe = testDataFactory.nextObject();            
            int i = count/groupSize;
            while (i>0) {
                while (regulator.hasRoomForChunk() && --i>=0) { //Note we are only dec when there is room for write
                    lastNow = App.recordSentTime(lastNow, blobWriter);

                    int g = groupSize;
                    while (--g >= 0) {
                        PhastWriter.writeToOuputStream(pWriter, writeMe,  out);                    
                        writeMe = testDataFactory.nextObject();   
                    }
                }
                App.commmonWait(); //we are faster than the consumer
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Pronghorn producer finished");
    }
}
