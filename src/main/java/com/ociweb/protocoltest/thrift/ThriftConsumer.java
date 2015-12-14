package com.ociweb.protocoltest.thrift;

import java.io.InputStream;
import java.util.List;

import org.HdrHistogram.Histogram;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.DataInputBlobReader;
import com.ociweb.pronghorn.pipe.RawDataSchema;
import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.App;
import com.ociweb.protocoltest.data.SequenceExampleA;
import com.ociweb.protocoltest.data.SequenceExampleAFactory;
import com.ociweb.protocoltest.data.SequenceExampleASample;
import com.ociweb.protocoltest.data.build.SequenceExampleAFuzzGenerator;

public class ThriftConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ThriftConsumer.class);
    private final StreamRegulator regulator;
    private final int count;
    private final Histogram histogram;

    public ThriftConsumer(StreamRegulator regulator, int count, Histogram histogram) {
        this.regulator = regulator;
        this.count = count;
        this.histogram = histogram;
    }

    private int countSamplesReceived = 0;
    private boolean compareSamples(ThriftQuery query, SequenceExampleA sample) {
        countSamplesReceived++;
        if (query.getUser() != sample.getUser() ||
            query.getYear() != sample.getYear() ||
            query.getMonth() != sample.getMonth() ||
            query.getDate() != sample.getDate() ||
            query.getSample_count() != sample.getSampleCount()) {
            System.out.println("Failed on received sample: "+countSamplesReceived);
            throw new RuntimeException("Received User: "+query.getUser()+"Expecting: "+sample.getUser()
                    +"\nReceived Year: "+query.getYear()+"Expecting: "+sample.getYear()
                    +"\nReceived Month: "+query.getMonth()+"Expecting: "+sample.getMonth()
                    +"\nReceived Date: "+query.getDate()+"Expecting: "+sample.getDate()
                    +"\nReceived Sample Count: "+query.getSample_count()+"Expecting: "+sample.getSampleCount());
        }
        List<ThriftSample> localSamples = query.getSamples();
        List<SequenceExampleASample> localSequenceSamples = sample.getSamples();
        if (localSamples.size() != localSequenceSamples.size() ||
            query.getSample_count() != localSamples.size()) {
            System.out.println("Failed on received sample: "+countSamplesReceived);
            throw new RuntimeException("SampleCount: "+query.getSample_count()
                                       +"\nQuery List Size: "+localSamples.size()
                                       +"\nGenerated List Size: "+localSequenceSamples.size());
        }
        for (int x = 0; x < localSamples.size(); ++x) {
            if (localSamples.get(x).getId() != localSequenceSamples.get(x).getId() ||
                localSamples.get(x).getTime() != localSequenceSamples.get(x).getTime() ||
                localSamples.get(x).getMeasurement() != localSequenceSamples.get(x).getMeasurement() ||
                localSamples.get(x).getAction() != localSequenceSamples.get(x).getAction()) {
                System.out.println("Failed on received sample: "+countSamplesReceived);
                    throw new RuntimeException("Received Id: "+localSamples.get(x).getId()+" Expected Id: "+localSequenceSamples.get(x).getId()
                        +"\nReceived Measurement: "+localSamples.get(x).getMeasurement()+" Expected Measurement: "+localSequenceSamples.get(x).getMeasurement()
                        +"\nReceived Action: "+localSamples.get(x).getAction()+" Expected Action: "+localSequenceSamples.get(x).getAction());
            }
        }
        return true;
    }

    @Override
    public void run() {
        ThriftQuery query = new ThriftQuery();
        try {
            

            InputStream in = regulator.getInputStream();
            TIOStreamTransport transport = new TIOStreamTransport(in) ;
            final TProtocol protocol = new TCompactProtocol.Factory().getProtocol(transport);

            SequenceExampleAFactory testDataFactory = new SequenceExampleAFuzzGenerator();
            
            DataInputBlobReader<RawDataSchema> blobReader = regulator.getBlobReader();
            long lastNow = 0;


            SequenceExampleA compareToMe = testDataFactory.nextObject();
            int i = count;
            while (i>0) {
                while (regulator.hasNextChunk() && --i>=0) {
                    lastNow= App.recordLatency(lastNow, histogram, blobReader);

                    query.read(protocol);

                    if (!compareSamples(query, compareToMe)) {
                        log.error("Does not match "+query+" vs "+compareToMe);
                    }
                    query.clear();
                    compareToMe = testDataFactory.nextObject();
                }
                Thread.yield(); //Only happens when the pipe is empty and there is nothing to read, eg PBSizeConsumer is faster than producer.
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        log.info("Thrift consumer finished");
    }

}
