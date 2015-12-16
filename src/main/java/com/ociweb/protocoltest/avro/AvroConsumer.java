package com.ociweb.protocoltest.avro;

import java.io.InputStream;
import java.util.Iterator;

import org.HdrHistogram.Histogram;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.App;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;
public class AvroConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(AvroConsumer.class);
    private final StreamRegulator regulator;
    private final int count;
    private final Histogram histogram;
    private final SequenceExampleAFactory testDataFactory;

    public AvroConsumer(StreamRegulator regulator, int count, Histogram histogram, SequenceExampleAFactory testExpectedDataFactory) {
        this.regulator = regulator;
        this.count = count;
        this.histogram = histogram;
        this.testDataFactory = testExpectedDataFactory;
    }

    @Override
    public void run() {
        try {

            InputStream in = regulator.getInputStream();
          
            
            DataInputBlobReader<RawDataSchema> blobReader = regulator.getBlobReader();
            long lastNow = 0;

            Schema schema = ReflectData.get().getSchema(SequenceExampleA.class);
            DatumReader datumReader =  new ReflectDatumReader(schema);
            
            
            DataFileStream reader = null;   
            SequenceExampleA obj = null;
            
            SequenceExampleA compareToMe = testDataFactory.nextObject();
            int i = count;
            while (i>0) {
                while (regulator.hasNextChunk() && --i>=0) {
                    lastNow= App.recordLatency(lastNow, histogram, blobReader);
                    
                    if (null==reader) {
                        reader = new DataFileStream(in, datumReader);   
                       // objIterator = reader.iterator();
                        obj = (SequenceExampleA) reader.next();
                    }  else {
                        obj = (SequenceExampleA) reader.next(obj);
                    }
                    
                    if (!obj.equals(compareToMe)) {
                        log.error("does not match");
                    }
                    
                    compareToMe = testDataFactory.nextObject();
                }
                App.commmonWait(); //Only happens when the pipe is empty and there is nothing to read.
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Avro consumer finished");
    }

}
