/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package org.apache.bookkeeper.client;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.net.InetAddresses;
import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.bookkeeper.bookie.Bookie;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.common.component.ComponentStarter;
import org.apache.bookkeeper.common.component.Lifecycle;
import org.apache.bookkeeper.common.component.LifecycleComponent;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.conf.TestBKConfiguration;
import org.apache.bookkeeper.discover.BookieServiceInfo;
import org.apache.bookkeeper.meta.UnderreplicatedLedger;
import org.apache.bookkeeper.meta.ZkLedgerUnderreplicationManager;
import org.apache.bookkeeper.meta.zk.ZKMetadataDriverBase;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.BookieServer;
import org.apache.bookkeeper.replication.ReplicationException.UnavailableException;
import org.apache.bookkeeper.server.Main;
import org.apache.bookkeeper.server.conf.BookieConfiguration;
//import org.apache.bookkeeper.test.BookKeeperClusterTestCase;
import org.apache.bookkeeper.util.AvailabilityOfEntriesOfLedger;
import org.apache.bookkeeper.util.BookKeeperConstants;
import org.apache.bookkeeper.util.PortManager;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test the bookkeeper admin.
 */
public class BookKeeperAdminTest extends BookKeeperClusterTestCase {

    private static final Logger LOG = LoggerFactory.getLogger(BookKeeperAdminTest.class);
    private DigestType digestType = DigestType.CRC32;
    private static final String PASSWORD = "testPasswd";
    private static final int numOfBookies = 2;
    private final int lostBookieRecoveryDelayInitValue = 1800;

    public BookKeeperAdminTest() {
        super(numOfBookies, 480);
        baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
        baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
        setAutoRecoveryEnabled(true);
    }

    @Test
    public void testInitNewCluster() throws Exception {
        ServerConfiguration newConfig = new ServerConfiguration(baseConf);
        String ledgersRootPath = "/testledgers";
        newConfig.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));
        Assert.assertTrue("New cluster should be initialized successfully", BookKeeperAdmin.initNewCluster(newConfig));

        Assert.assertTrue("Cluster rootpath should have been created successfully " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) != null));
        String availableBookiesPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + AVAILABLE_NODE;
        Assert.assertTrue("AvailableBookiesPath should have been created successfully " + availableBookiesPath,
                (zkc.exists(availableBookiesPath, false) != null));
        String readonlyBookiesPath = availableBookiesPath + "/" + READONLY;
        Assert.assertTrue("ReadonlyBookiesPath should have been created successfully " + readonlyBookiesPath,
            (zkc.exists(readonlyBookiesPath, false) != null));
        String instanceIdPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
            + "/" + BookKeeperConstants.INSTANCEID;
        Assert.assertTrue("InstanceId node should have been created successfully" + instanceIdPath,
                (zkc.exists(instanceIdPath, false) != null));

        String ledgersLayout = ledgersRootPath + "/" + BookKeeperConstants.LAYOUT_ZNODE;
        Assert.assertTrue("Layout node should have been created successfully" + ledgersLayout,
                (zkc.exists(ledgersLayout, false) != null));

        /**
         * create znodes simulating existence of Bookies in the cluster
         */
        int numOfBookies = 3;
        Random rand = new Random();
        for (int i = 0; i < numOfBookies; i++) {
            String ipString = InetAddresses.fromInteger(rand.nextInt()).getHostAddress();
            String regPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
                + "/" + AVAILABLE_NODE + "/" + ipString + ":3181";
            zkc.create(regPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

        /*
         * now it should be possible to create ledger and delete the same
         */
        BookKeeper bk = new BookKeeper(new ClientConfiguration(newConfig));
        LedgerHandle lh = bk.createLedger(numOfBookies, numOfBookies, numOfBookies, BookKeeper.DigestType.MAC,
                new byte[0]);
        bk.deleteLedger(lh.ledgerId);
        bk.close();
    }

    @Test
    public void testNukeExistingClusterWithForceOption() throws Exception {
        String ledgersRootPath = "/testledgers";
        ServerConfiguration newConfig = new ServerConfiguration(baseConf);
        newConfig.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));
        List<String> bookiesRegPaths = new ArrayList<String>();
        initiateNewClusterAndCreateLedgers(newConfig, bookiesRegPaths);

        /*
         * before nuking existing cluster, bookies shouldn't be registered
         * anymore
         */
        for (int i = 0; i < bookiesRegPaths.size(); i++) {
            zkc.delete(bookiesRegPaths.get(i), -1);
        }

        Assert.assertTrue("New cluster should be nuked successfully",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, null, true));
        Assert.assertTrue("Cluster rootpath should have been deleted successfully " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) == null));
    }

    @Test
    public void testNukeExistingClusterWithInstanceId() throws Exception {
        String ledgersRootPath = "/testledgers";
        ServerConfiguration newConfig = new ServerConfiguration(baseConf);
        newConfig.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));
        List<String> bookiesRegPaths = new ArrayList<String>();
        initiateNewClusterAndCreateLedgers(newConfig, bookiesRegPaths);

        /*
         * before nuking existing cluster, bookies shouldn't be registered
         * anymore
         */
        for (int i = 0; i < bookiesRegPaths.size(); i++) {
            zkc.delete(bookiesRegPaths.get(i), -1);
        }

        byte[] data = zkc.getData(
            ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + BookKeeperConstants.INSTANCEID,
            false, null);
        String readInstanceId = new String(data, UTF_8);

        Assert.assertTrue("New cluster should be nuked successfully",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false));
        Assert.assertTrue("Cluster rootpath should have been deleted successfully " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) == null));
    }

    @Test
    public void tryNukingExistingClustersWithInvalidParams() throws Exception {
        String ledgersRootPath = "/testledgers";
        ServerConfiguration newConfig = new ServerConfiguration(baseConf);
        newConfig.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));
        List<String> bookiesRegPaths = new ArrayList<String>();
        initiateNewClusterAndCreateLedgers(newConfig, bookiesRegPaths);

        /*
         * create ledger with a specific ledgerid
         */
        BookKeeper bk = new BookKeeper(new ClientConfiguration(newConfig));
        long ledgerId = 23456789L;
        LedgerHandle lh = bk.createLedgerAdv(ledgerId, 1, 1, 1, BookKeeper.DigestType.MAC, new byte[0], null);
        lh.close();

        /*
         * read instanceId
         */
        byte[] data = zkc.getData(
            ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + BookKeeperConstants.INSTANCEID,
            false, null);
        String readInstanceId = new String(data, UTF_8);

        /*
         * register a RO bookie
         */
        String ipString = InetAddresses.fromInteger((new Random()).nextInt()).getHostAddress();
        String roBookieRegPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
            + "/" + AVAILABLE_NODE + "/" + READONLY + "/" + ipString + ":3181";
        zkc.create(roBookieRegPath, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Assert.assertFalse("Cluster should'nt be nuked since instanceid is not provided and force option is not set",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, null, false));
        Assert.assertFalse("Cluster should'nt be nuked since incorrect instanceid is provided",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, "incorrectinstanceid", false));
        Assert.assertFalse("Cluster should'nt be nuked since bookies are still registered",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false));
        /*
         * delete all rw bookies registration
         */
        for (int i = 0; i < bookiesRegPaths.size(); i++) {
            zkc.delete(bookiesRegPaths.get(i), -1);
        }
        Assert.assertFalse("Cluster should'nt be nuked since ro bookie is still registered",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false));

        /*
         * make sure no node is deleted
         */
        Assert.assertTrue("Cluster rootpath should be existing " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) != null));
        String availableBookiesPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig) + "/" + AVAILABLE_NODE;
        Assert.assertTrue("AvailableBookiesPath should be existing " + availableBookiesPath,
                (zkc.exists(availableBookiesPath, false) != null));
        String instanceIdPath = ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
            + "/" + BookKeeperConstants.INSTANCEID;
        Assert.assertTrue("InstanceId node should be existing" + instanceIdPath,
                (zkc.exists(instanceIdPath, false) != null));
        String ledgersLayout = ledgersRootPath + "/" + BookKeeperConstants.LAYOUT_ZNODE;
        Assert.assertTrue("Layout node should be existing" + ledgersLayout, (zkc.exists(ledgersLayout, false) != null));

        /*
         * ledger should not be deleted.
         */
        lh = bk.openLedgerNoRecovery(ledgerId, BookKeeper.DigestType.MAC, new byte[0]);
        lh.close();
        bk.close();

        /*
         * delete ro bookie reg znode
         */
        zkc.delete(roBookieRegPath, -1);

        Assert.assertTrue("Cluster should be nuked since no bookie is registered",
                BookKeeperAdmin.nukeExistingCluster(newConfig, ledgersRootPath, readInstanceId, false));
        Assert.assertTrue("Cluster rootpath should have been deleted successfully " + ledgersRootPath,
                (zkc.exists(ledgersRootPath, false) == null));
    }

    void initiateNewClusterAndCreateLedgers(ServerConfiguration newConfig, List<String> bookiesRegPaths)
            throws Exception {
        Assert.assertTrue("New cluster should be initialized successfully", BookKeeperAdmin.initNewCluster(newConfig));

        /**
         * create znodes simulating existence of Bookies in the cluster
         */
        int numberOfBookies = 3;
        Random rand = new Random();
        for (int i = 0; i < numberOfBookies; i++) {
            String ipString = InetAddresses.fromInteger(rand.nextInt()).getHostAddress();
            bookiesRegPaths.add(ZKMetadataDriverBase.resolveZkLedgersRootPath(newConfig)
                + "/" + AVAILABLE_NODE + "/" + ipString + ":3181");
            zkc.create(bookiesRegPaths.get(i), new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

        /*
         * now it should be possible to create ledger and delete the same
         */
        BookKeeper bk = new BookKeeper(new ClientConfiguration(newConfig));
        LedgerHandle lh;
        int numOfLedgers = 5;
        for (int i = 0; i < numOfLedgers; i++) {
            lh = bk.createLedger(numberOfBookies, numberOfBookies, numberOfBookies, BookKeeper.DigestType.MAC,
                    new byte[0]);
            lh.close();
        }
        bk.close();
    }

    @Test
    public void testAreEntriesOfLedgerStoredInTheBookieForLastEmptySegment() throws Exception {
        int lastEntryId = 10;
        long ledgerId = 100L;
        BookieId bookie0 = new BookieSocketAddress("bookie0:3181").toBookieId();
        BookieId bookie1 = new BookieSocketAddress("bookie1:3181").toBookieId();
        BookieId bookie2 = new BookieSocketAddress("bookie2:3181").toBookieId();
        BookieId bookie3 = new BookieSocketAddress("bookie3:3181").toBookieId();

        List<BookieId> ensembleOfSegment1 = new ArrayList<BookieId>();
        ensembleOfSegment1.add(bookie0);
        ensembleOfSegment1.add(bookie1);
        ensembleOfSegment1.add(bookie2);

        List<BookieId> ensembleOfSegment2 = new ArrayList<BookieId>();
        ensembleOfSegment2.add(bookie3);
        ensembleOfSegment2.add(bookie1);
        ensembleOfSegment2.add(bookie2);

        LedgerMetadataBuilder builder = LedgerMetadataBuilder.create();
        builder.withId(ledgerId)
                .withEnsembleSize(3)
                .withWriteQuorumSize(3)
                .withAckQuorumSize(2)
                .withDigestType(digestType.toApiDigestType())
                .withPassword(PASSWORD.getBytes())
                .newEnsembleEntry(0, ensembleOfSegment1)
                .newEnsembleEntry(lastEntryId + 1, ensembleOfSegment2)
                .withLastEntryId(lastEntryId).withLength(65576).withClosedState();
        LedgerMetadata meta = builder.build();

        assertFalse("expected areEntriesOfLedgerStoredInTheBookie to return False for bookie3",
                BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledgerId, bookie3, meta));
        assertTrue("expected areEntriesOfLedgerStoredInTheBookie to return true for bookie2",
                BookKeeperAdmin.areEntriesOfLedgerStoredInTheBookie(ledgerId, bookie2, meta));
    }
}
