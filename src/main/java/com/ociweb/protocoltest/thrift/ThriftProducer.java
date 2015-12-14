package com.ociweb.protocoltest.thrift;

import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.transport.TIOStreamTransport;

import com.ociweb.pronghorn.pipe.DataOutputBlobWriter;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.App;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.SequenceExampleASample;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;

public class ThriftProducer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ThriftProducer.class);

    private final StreamRegulator regulator;
    private final int count;

    public ThriftProducer(StreamRegulator regulator, int count) {
        this.regulator = regulator;
        this.count = count;
    }
    
    @Override
    public void run() {
        try {
            
            
            DataOutputBlobWriter<RawDataSchema> blobWriter = regulator.getBlobWriter();
            long lastNow = 0;

            SequenceExampleAFactory testDataFactory = new SequenceExampleAFuzzGenerator();
            
            OutputStream out = regulator.getOutputStream();
            TIOStreamTransport transport = new TIOStreamTransport(out) ;
            SequenceExampleA writeMe = testDataFactory.nextObject();
            ThriftQuery query = new ThriftQuery();
            TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
            int i = count;
            while (i>0) {
                while (regulator.hasRoomForChunk() && --i>=0) { //Note we are only dec when there is room for write
                    lastNow = App.recordSentTime(lastNow, blobWriter);

                    query.setUser(writeMe.getUser());
                    query.setYear(writeMe.getYear());
                    query.setMonth(writeMe.getMonth());
                    query.setDate(writeMe.getDate());
                    query.setSample_count(writeMe.getSampleCount());

                    for (SequenceExampleASample sample : writeMe.getSamples()) {
                        ThriftSample tsample = new ThriftSample();
                        tsample.setId(sample.getId());
                        tsample.setTime(sample.getTime());
                        tsample.setMeasurement(sample.getMeasurement());
                        tsample.setAction(sample.getAction());
                        query.addToSamples(tsample);
                    }
                    final byte[] serialized = serializer.serialize(query);
                    transport.write(serialized);
                    transport.flush();
                    query.clear();
                    writeMe = testDataFactory.nextObject();   
                }
                Thread.yield(); //we are faster than the consumer
            }
            transport.close();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Thrift producer finished");
    }
}
