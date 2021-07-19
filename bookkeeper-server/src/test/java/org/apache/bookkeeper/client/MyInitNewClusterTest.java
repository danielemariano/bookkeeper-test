package org.apache.bookkeeper.client;

import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.test.BookKeeperClusterTestCase;

import org.junit.Test;
//import org.junit.Test.RunWith;
//import org.junit.Parameterized;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runner.RunWith;
import junit.framework.TestCase;

/**
 * Test initNewCluster in bookkeeper admin.
 */
//@RunWith(value=Parameterized.class)
@RunWith(Parameterized.class)
public class MyInitNewClusterTest extends BookKeeperClusterTestCase {

    private boolean result;
    private ServerConfiguration conf;
    private String confType;
    private static final int numOfBookies = 2;
    private final int lostBookieRecoveryDelayInitValue = 1800;

    public MyInitNewClusterTest(boolean result, String conf) throws Exception {
      super(numOfBookies, 480);
      baseConf.setLostBookieRecoveryDelay(lostBookieRecoveryDelayInitValue);
      baseConf.setOpenLedgerRereplicationGracePeriod(String.valueOf(30000));
      setAutoRecoveryEnabled(true);

      this.result = result;
      this.confType = conf;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> value() {
      return Arrays.asList(new Object[][]{
        {true, "new"},
        {false, "null"},
        {false, "wrong"}
      });
    }

    @Test
    public void testInitNewCluster() throws Exception {
      boolean realResult;

      if (confType == "null") {
        this.conf = null;
      }
      else if (confType == "wrong") {
        String ledgersRootPath = "zk+hierarcchical://127.0.0.1/ledgers";
        this.conf = new ServerConfiguration().setMetadataServiceUri(ledgersRootPath);
      }
      else if (confType == "new") {
        this.conf = new ServerConfiguration(baseConf);
        String ledgersRootPath = "/testledgers";
        this.conf.setMetadataServiceUri(newMetadataServiceUri(ledgersRootPath));
      }

      try {
        realResult = BookKeeperAdmin.testInitNewCluster(conf);
      } catch (Exception e) {
        realResult = false;
        e.printStackTrace();
      }
      assertEquals(result, realResult);
    }

}
