package com.ociweb.protocoltest.kryo;

import java.io.InputStream;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.App;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;
public class KryoConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KryoConsumer.class);
    private final StreamRegulator regulator;
    private final int count;
    private final Histogram histogram;
    private final Kryo kryo = new Kryo();

    public KryoConsumer(StreamRegulator regulator, int count, Histogram histogram) {
        this.regulator = regulator;
        this.count = count;
        this.histogram = histogram;
    }

    @Override
    public void run() {
        try {
            

            Input input = new Input(regulator.getInputStream());
            
            SequenceExampleAFactory testDataFactory = new SequenceExampleAFuzzGenerator();
            
            DataInputBlobReader<RawDataSchema> blobReader = regulator.getBlobReader();
            long lastNow = 0;


            SequenceExampleA compareToMe = testDataFactory.nextObject();
            int i = count;
            while (i>0) {
                while (regulator.hasNextChunk() && --i>=0) {
                    lastNow= App.recordLatency(lastNow, histogram, blobReader);
                    
                    SequenceExampleA obj = (SequenceExampleA) kryo.readObject(input, SequenceExampleA.class);
                    
                    if (!obj.equals(compareToMe)) {
                        log.error("Does not match "+obj+" vs "+compareToMe);
                    }
                    
                    compareToMe = testDataFactory.nextObject();
                }
                Thread.yield(); //Only happens when the pipe is empty and there is nothing to read, eg PBSizeConsumer is faster than producer.
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Kryo consumer finished");
    }

}
