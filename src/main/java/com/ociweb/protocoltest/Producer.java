package com.ociweb.protocoltest;

import java.io.IOException;
import java.io.OutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ociweb.pronghorn.pipe.util.StreamRegulator;
import com.ociweb.protocoltest.PBMessageProvider.ProductQuery;
import com.ociweb.protocoltest.PBMessageProvider.ProductQueryProvider;

public class Producer implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(Producer.class);

    private final StreamRegulator regulator;
    private final int count;

    public Producer(StreamRegulator regulator, int count) {
        this.regulator = regulator;
        this.count = count;
    }

    @Override
    public void run() {
        try {

            OutputStream out = regulator.getOutputStream();


            int i = count;
            while (i>0) {
                while (regulator.hasRoomForChunk() && --i>=0) { //Note we are only dec when there is room for write

                    //NOTE: the messages sent must contain the timestamp for now so we can compute latency per message
                    long now = System.nanoTime();

                    //Use something to write objects to the output stream
                    //Note this must NOT exceeded the chunk size.

//                        out.write(42);  //Do not keep this code, for example only.
//
//                        out.write((byte)(now >>> 56));//Do not keep this code, for example only.
//                        out.write((byte)(now >>> 48));//Do not keep this code, for example only.
//                        out.write((byte)(now >>> 40));//Do not keep this code, for example only.
//                        out.write((byte)(now >>> 32));//Do not keep this code, for example only.
//                        out.write((byte)(now >>> 24));//Do not keep this code, for example only.
//                        out.write((byte)(now >>> 16));//Do not keep this code, for example only.
//                        out.write((byte)(now >>> 8));//Do not keep this code, for example only.
//                        out.write((byte) now);//Do not keep this code, for example only.

//                        log.trace("producer:{}",i);

                    String productDesc = "Dive into the world of Star Wars"
                          + " with the LEGO Star Wars Slave 1 75060. "
                          + "LEGO delivers once again and this time they "
                          + "make the legendary bounty hunter, Boba Fett, "
                          + "the star with his cool Slave 1 ship. This model "
                          + "has a rotating cockpit and wings for flight and "
                          + "landing mode, dual shooters and hidden blasters. "
                          + "Perfectly suited to reenact the capture of Han Solo "
                          + "from Star Wars: Episode V The Empire Strikes Back or"
                          + " create entirely different stories and add layers to"
                          + " one of the most loved sagas of all times. Includes 4"
                          + " mini figures with weapons.";
                    String productName = "LEGO Star Wars Slave 1 75060";
                    String storeItemId = "204-00-1114";
                    long onlineItemId = 17170752;

                    ProductQuery.Builder productQuery = ProductQuery.newBuilder();
                    productQuery.setProductName(productName)
                        .setOnlineItemId(onlineItemId)
                        .setStoreItemId(storeItemId)
                        .setProductDesc(productDesc);

                    for (int x = 0; x < 5; ++x) {
                        ProductQuery.ProductAvailability.Builder available_at =
                            ProductQuery.ProductAvailability.newBuilder()
                              .setStoreId(x)
                              .setStoreLocation("St. Louis")
                              .setNumAvailable(x*2);
                        productQuery.addInStockLocations(available_at);
                    }

                    productQuery.setQueryTime(now).build();

                    ProductQueryProvider.Builder productQueryProvider =
                        ProductQueryProvider.newBuilder().addQueries(productQuery);


                    productQueryProvider.build().writeDelimitedTo(out);

                    log.trace("producer:{}",i);

                }
                Thread.yield(); //we are faster than the consumer
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        log.info("producer finished");
    }
}
