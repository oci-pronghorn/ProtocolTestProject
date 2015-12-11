package com.ociweb.protocoltest.protobuf;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.HdrHistogram.Histogram;
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
import com.ociweb.protocoltest.protobuf.PBQueryProvider.PBQuery;
import com.ociweb.protocoltest.protobuf.PBQueryProvider.PBQuery.PBSample;
public class PBConsumer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(PBConsumer.class);
    private final StreamRegulator regulator;
    private final int count;
    private final Histogram histogram;

    public PBConsumer(StreamRegulator regulator, int count, Histogram histogram) {
        this.regulator = regulator;
        this.count = count;
        this.histogram = histogram;
    }

    private boolean compareSamples(PBQuery query, SequenceExampleA sample) {
        if (query.getUser() != sample.getUser() ||
            query.getYear() != sample.getYear() ||
            query.getMonth() != sample.getMonth() ||
            query.getDate() != sample.getDate() ||
            query.getSampleCount() != sample.getSampleCount()) {
            throw new RuntimeException("Received: "+query+"\nExpecting: "+sample);
        }
        List<PBSample> localSamples = query.getSamplesList();
        List<SequenceExampleASample> localSequenceSamples = sample.getSamples();
        if (localSamples.size() != localSequenceSamples.size() ||
            query.getSampleCount() != localSamples.size()) {
            throw new RuntimeException("SampleCount: "+query.getSampleCount()
                                       +"\nQuery List Size: "+localSamples.size()
                                       +"\nGenerated List Size: "+localSequenceSamples.size());
        }
        for (int x = 0; x < localSamples.size(); ++x) {
            if (localSamples.get(x).getId() != localSequenceSamples.get(x).getId() ||
                localSamples.get(x).getMeasurement() != localSequenceSamples.get(x).getMeasurement() ||
                localSamples.get(x).getAction() != localSequenceSamples.get(x).getAction()) {
                    throw new RuntimeException("Received Id: "+localSamples.get(x).getId()+" Expected Id: "+localSequenceSamples.get(x).getId()
                        +"\nReceived Measurement: "+localSamples.get(x).getMeasurement()+" Expected Measurement: "+localSequenceSamples.get(x).getMeasurement()
                        +"\nReceived Action: "+localSamples.get(x).getAction()+" Expected Action: "+localSequenceSamples.get(x).getAction());
            }
        }
        return true;
    }

    @Override
    public void run() {
        try {

            InputStream in = regulator.getInputStream();
            SequenceExampleAFactory testDataFactory = new SequenceExampleAFuzzGenerator();
            
            DataInputBlobReader<RawDataSchema> blobReader = regulator.getBlobReader();
            long lastNow = 0;
            
            
            int i = count;
            while (i>0) {
                while (regulator.hasNextChunk() && --i>=0) {
                    //use something to read the data from the input stream

                    PBQuery query = PBQuery.parseDelimitedFrom(in);

                    SequenceExampleA compareToMe = testDataFactory.nextObject();

                    compareSamples(query, compareToMe);
                    
                    lastNow= App.recordLatency(lastNow, histogram, blobReader);
                    

                }
                Thread.yield(); //Only happens when the pipe is empty and there is nothing to read, eg PBConsumer is faster than producer.
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("PBConsumer finished");
    }

}
