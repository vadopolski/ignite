package org.apache.ignite.internal;

import org.apache.ignite.Ignite;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.plugin.segmentation.SegmentationPolicy.NOOP;

/**
 */
public class EventsListenerTest extends GridCommonAbstractTest {
    /** Is client. */
    private boolean isClient = false;

    private int count = 0;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setClientMode(isClient);

        c.setPeerClassLoadingEnabled(true);

        c.setDeploymentMode(CONTINUOUS);

        c.setMetricsLogFrequency(6_000_000);

        c.setIncludeEventTypes(EventType.EVT_NODE_JOINED,
            EVT_NODE_LEFT,
            EventType.EVT_NODE_FAILED,
            EventType.EVT_NODE_SEGMENTED);

        if (!isClient) {
            TcpCommunicationSpi commSpi = new TcpCommunicationSpi();

            commSpi.setSharedMemoryPort(-1);

            c.setCommunicationSpi(commSpi);

            c.setSegmentationPolicy(NOOP);
        }

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        if (!isClient) {
            CacheConfiguration cc1 = defaultCacheConfiguration();

            cc1.setName("CACHE_NAME");

            cc1.setCacheMode(CacheMode.PARTITIONED);

            c.setCacheConfiguration(cc1);
        }

//        CacheConfiguration cc1 = defaultCacheConfiguration();
//
//        cc1.setName(isClient ? "client" : "server" + count++);
//
//        cc1.setCacheMode(isClient ? CacheMode.LOCAL : CacheMode.PARTITIONED);
        return c;
    }

    /**
     *
     */
    public void testListenerClient() throws Exception {

        CountDownLatch remoteEvents = new CountDownLatch(1);
        CountDownLatch localEvents = new CountDownLatch(1);

        try {
            isClient = false;

            Ignite server0 = startGrid(0);

            Thread.sleep(2_000);

            isClient = true;

            Ignite client = startGrid(1);

            Thread.sleep(2_000);

            checkTopology(2);

            client.events().remoteListen(new LocalListener(localEvents),
                new RemoteDiscoveryEventFilter(remoteEvents),
                EventType.EVT_NODE_LEFT);

            isClient = false;

            Ignite server2 = startGrid(2);

            Thread.sleep(2_000);

            checkTopology(3);

//            Thread.sleep(10_000);

            //restart 
            stopGrid(0);
//            server0 = startGrid(0);
            Thread.sleep(2_000);

//            checkTopology(2);

            assertTrue(remoteEvents.await(5, TimeUnit.SECONDS));
            assertTrue(localEvents.await(5, TimeUnit.SECONDS));
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class LocalListener implements IgniteBiPredicate<UUID, DiscoveryEvent> {
        /** Latch. */
        private CountDownLatch latch;

        /**
         * @param latch Latch.
         */
        public LocalListener(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        public boolean apply(UUID uuid, DiscoveryEvent event) {
            latch.countDown();

            return true;
        }
    }

    /**
     *
     */
    private static class RemoteDiscoveryEventFilter implements IgnitePredicate<DiscoveryEvent> {
        /** Latch. */
        private CountDownLatch latch;

        /**
         * @param latch Latch.
         */
        public RemoteDiscoveryEventFilter(CountDownLatch latch) {
            this.latch = latch;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(DiscoveryEvent event) {
            latch.countDown();

            return true;
        }
    }
}