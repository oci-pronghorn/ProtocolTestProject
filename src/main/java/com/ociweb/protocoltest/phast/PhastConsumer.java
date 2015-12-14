package com.ociweb.protocoltest.phast;

import java.io.InputStream;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.App;
import com.ociweb.protocoltest.data.PhastReader;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;
public class PhastConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PhastConsumer.class);
    private final StreamRegulator regulator;
    private final int count;
    private final Histogram histogram;

    public PhastConsumer(StreamRegulator regulator, int count, Histogram histogram) {
        this.regulator = regulator;
        this.count = count;
        this.histogram = histogram;
    }

    @Override
    public void run() {
        try {
            PhastReader pReader = new PhastReader();
                       
            
            InputStream in = regulator.getInputStream();
            SequenceExampleAFactory testDataFactory = new SequenceExampleAFuzzGenerator();
            
            DataInputBlobReader<RawDataSchema> blobReader = regulator.getBlobReader();
            long lastNow = 0;

            SequenceExampleA targetObject = new SequenceExampleA();

            SequenceExampleA compareToMe = testDataFactory.nextObject();
            int i = count;
            while (i>0) {
                while (regulator.hasNextChunk() && --i>=0) {
                    lastNow= App.recordLatency(lastNow, histogram, blobReader);
                                       
                    PhastReader.readFromInputStream(pReader, targetObject, in);
                    if (!targetObject.equals(compareToMe)) {
                        log.error("Does not match");
                    }
                    
                    compareToMe = testDataFactory.nextObject();
                }
                Thread.yield(); //Only happens when the pipe is empty and there is nothing to read, eg PBSizeConsumer is faster than producer.
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Empty consumer finished");
    }

}
