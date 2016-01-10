package com.ociweb.protocoltest.avro;

import java.io.InputStream;

import org.HdrHistogram.Histogram;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.App;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.SequenceExampleASchema;
public class AvroConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(AvroConsumer.class);
    private final StreamRegulator regulator;
    private final int count;
    private final Histogram histogram;
    private final SequenceExampleAFactory testDataFactory;
    private final int groupSize;

    public AvroConsumer(StreamRegulator regulator, int count, Histogram histogram, SequenceExampleAFactory testExpectedDataFactory, int groupSize) {
        this.regulator = regulator;
        this.count = count;
        this.histogram = histogram;
        this.testDataFactory = testExpectedDataFactory;
        this.groupSize = groupSize;
    }

    @Override
    public void run() {
        int i = count/groupSize;
        try {
            InputStream in = regulator.getInputStream();
          
            
            DataInputBlobReader<RawDataSchema> blobReader = regulator.getBlobReader();
            long lastNow = 0;

            Schema schema = ReflectData.get().getSchema(SequenceExampleA.class);
           
            
            DataFileStream reader = null;   
            SequenceExampleA obj = null;
            
            SequenceExampleA compareToMe = testDataFactory.nextObject();
            while (i>0) {
                while (regulator.hasNextChunk() && --i>=0) {
                    lastNow= App.recordLatency(lastNow, histogram, blobReader);
                    
                    //as long as the written block streams are larger than the internal buffer of avro this works
                    //otherwise avro crashes because part of its performance comes from pre-fetching the next block of data.
                    
                    if (null==reader) {
                        DatumReader datumReader =  new ReflectDatumReader(schema);
                        reader = new DataFileStream(in, datumReader);
                        obj = (SequenceExampleA) reader.next();
                    }  else {
                        obj = (SequenceExampleA) reader.next(obj);
                    }

                    if (!obj.equals(compareToMe)) {
                        log.error("does not match");
                    }
                    compareToMe = testDataFactory.nextObject();
                    
                    int g = groupSize-1;
                    while (--g>=0) {
                        obj = (SequenceExampleA) reader.next(obj);
                        if (!obj.equals(compareToMe)) {
                            log.error("does not match");
                        }
                        compareToMe = testDataFactory.nextObject();
                    }

                    //log.error("unread bytes: {} ", regulator.getBlobReader().available());

                }
                App.commmonWait(); //Only happens when the pipe is empty and there is nothing to read.
            }
        } catch (Exception e) {
            log.error("fail with {} iterations remaining",i);
            throw new RuntimeException(e);
        }
        log.info("Avro consumer finished");
    }

}
