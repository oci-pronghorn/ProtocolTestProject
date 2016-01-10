package com.ociweb.protocoltest.avro;

import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.DocumentType;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.App;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.SequenceExampleASample;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;

public class AvroProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(AvroProducer.class);

    private final StreamRegulator regulator;
    private final int count;
    private final  SequenceExampleAFactory testDataFactory;
    private final int groupSize;
    
    public AvroProducer(StreamRegulator regulator, int count, SequenceExampleAFactory testSentDataFactory, int groupSize) {
        this.regulator = regulator;
        this.count = count;
        this.testDataFactory = testSentDataFactory;
        
        //This group size HACK is required for Avro because Avro does not work well when it is given exactly the needed bytes
        // instead AVRO requires the ability to pre-fetch its buffer ahead of time but this violates our time regulation constraints.
        this.groupSize = groupSize;
    }
    
    @Override
    public void run() {
        try {
            OutputStream out = regulator.getOutputStream();
            DataOutputBlobWriter<RawDataSchema> blobWriter = regulator.getBlobWriter();
            long lastNow = 0;
            
            
            Schema schema = ReflectData.get().getSchema(SequenceExampleA.class);
            ReflectDatumWriter dout = new ReflectDatumWriter(schema);  
            DataFileWriter writer = null;

            SequenceExampleA writeMe = testDataFactory.nextObject();            
            int i = count/groupSize;
            while (i>0) {
                while (regulator.hasRoomForChunk() && --i>=0) { //Note we are only dec when there is room for write
                    lastNow = App.recordSentTime(lastNow, blobWriter);
                    
                    if (null==writer) {                                                
                        writer = new DataFileWriter(dout);                        
                        writer.create(schema, out);
                    }  
                    
                    int g = groupSize;
                    while (--g >= 0) {
                        writer.append(writeMe);
                        writeMe = testDataFactory.nextObject();   
                    }
                    writer.flush();
                    //writer.close();
                    
                    
                }
                App.commmonWait(); //we are faster than the consumer
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Avro producer finished");
    }
}
