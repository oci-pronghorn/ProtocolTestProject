package com.ociweb.protocoltest.kryo;

import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.App;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;

public class KryoProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(KryoProducer.class);

    private final StreamRegulator regulator;
    private final int count;
    private final Kryo kryo = new Kryo();

    public KryoProducer(StreamRegulator regulator, int count) {
        this.regulator = regulator;
        this.count = count;
    }
    
    @Override
    public void run() {
        try {
            
            
            DataOutputBlobWriter<RawDataSchema> blobWriter = regulator.getBlobWriter();
            long lastNow = 0;

            SequenceExampleAFactory testDataFactory = new SequenceExampleAFuzzGenerator();
            
            Output out = new Output(regulator.getOutputStream());
            
            SequenceExampleA writeMe = testDataFactory.nextObject();            
            int i = count;
            while (i>0) {
                while (regulator.hasRoomForChunk() && --i>=0) { //Note we are only dec when there is room for write
                    lastNow = App.recordSentTime(lastNow, blobWriter);

                    kryo.writeObject(out, writeMe);                    
                    out.flush();
                    
                    writeMe = testDataFactory.nextObject();   
                }
                Thread.yield(); //we are faster than the consumer
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Empty producer finished");
    }
}
