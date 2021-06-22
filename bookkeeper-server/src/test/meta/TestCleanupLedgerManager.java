package meta;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import JCSCacheElementRetrievalUnitTest.TestInput;

@RunWith(Parameterized.class)
public class TestCleanupLedgerManager {
	
	private LedgerManager ledgerManager;
	private LedgerManagerFactory ledgerManagerFactory;
	
	 public static Collection<TestInput[]> configure(){
		   Collection<TestInput> inputs = new ArrayList<>();
	       
	       Collection<TestInput[]> result = new ArrayList<>();
		   
//		   inputs.add(new TestInput(jcs,"test_key","test_data","test_key","Name wasn't right","testCache1","Create time should have been at or after the call"));
//		   inputs.add(new TestInput(jcs,"string_not_valid","test_data","test_key","Name wasn't right","testCache1","Create time should have been at or after the call"));
		 
		   
		   for (TestInput e : inputs) {
	           result.add(new TestInput[] { e });
	       }
	       return result;
		     
	   }
	 
	 private static class TestInput {
	       
	   
	 }
	
	 @Parameters
	   public static Collection<TestInput[]> getTestParameters(){
	   	return configure();

	   }
	
	@Before
	public void setUp() throws Exception {
		if (null == ledgerManager) {
            ledgerManager = ledgerManagerFactory.newLedgerManager();
        }
	}
	
	

	@Test
	public void testCleanupLedgerManager() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetUnderlying() {
		fail("Not yet implemented");
	}

	@Test
	public void testRegisterLedgerMetadataListener() {
		fail("Not yet implemented");
	}

	@Test
	public void testUnregisterLedgerMetadataListener() {
		fail("Not yet implemented");
	}

	@Test
	public void testCreateLedgerMetadata() {
		fail("Not yet implemented");
	}

	@Test
	public void testRemoveLedgerMetadata() {
		fail("Not yet implemented");
	}

	@Test
	public void testReadLedgerMetadata() {
		fail("Not yet implemented");
	}

	@Test
	public void testWriteLedgerMetadata() {
		fail("Not yet implemented");
	}

	@Test
	public void testAsyncProcessLedgers() {
		fail("Not yet implemented");
	}

	@Test
	public void testGetLedgerRanges() {
		fail("Not yet implemented");
	}

	@Test
	public void testClose() {
		fail("Not yet implemented");
	}

}
