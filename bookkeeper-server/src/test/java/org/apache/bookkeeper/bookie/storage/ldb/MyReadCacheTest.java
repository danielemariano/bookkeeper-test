package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

@RunWith(Parameterized.class)
public class MyReadCacheTest {
	
	private ByteBufAllocator allocator;
    private long maxCacheSize;
    private int bufferSize;
    private int segmentSize;
	
	@Parameters
	public static Collection<Object[]> configure(){
       return Arrays.asList(new Object[][] {
			{UnpooledByteBufAllocator.DEFAULT, 10*1024, 1024, 2*1024},
				{UnpooledByteBufAllocator.DEFAULT, 10*512, 512, 2*512},
				{UnpooledByteBufAllocator.DEFAULT, 10*2048, 2048, 2*2048}
		});
	}
	
    public MyReadCacheTest(ByteBufAllocator allocator, long maxCacheSize, int bufferSize, int segmentSize){
        this.maxCacheSize = maxCacheSize;
        this.allocator = allocator;
        this.bufferSize = bufferSize;
        this.segmentSize = segmentSize;
    }
    
    @Test
    public void checkEmptyAndReadingCacheSingleSegmentTest() {   	
 
    	System.out.println("checkEmptyAndReadingCacheSingleSegmentTest");
        
    	// Inizializzazione della cache utilizzando la ReadCache con gli input 
    	// presi dai parametri del test definiti nel metodo configure() che essendo
    	// taggato con @Parameters i suoi parametri vengono utilizzati per ogni run di test
    	ReadCache cache = new ReadCache(allocator, maxCacheSize);

    	// Test della cache vuota utilizzando e quindi testando il corretto funzionamento
    	// del metodo count, size e get della classe ReadCache
        System.out.println("Check empty " + maxCacheSize + " cache (created for reading) test");
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        assertEquals(null, cache.get(0, 0));

        // Test dell'inserimento di dati nella cache utilizzando il metodo put, size,
        // count della classe ReadCache sotto test
        System.out.println("Read " + maxCacheSize + " cache test");
        ByteBuf entry = Unpooled.wrappedBuffer(new byte[bufferSize]);
        cache.put(1, 0, entry);
        assertEquals(1, cache.count());
        assertEquals(bufferSize, cache.size());
        
        // Il test di scrittura viene controllato attraverso il metodo get con il quale
        // eseguiamo il retrieving delle informazioni appena scritte
        assertEquals(entry, cache.get(1, 0));
        assertNull(cache.get(1, 1));

        // Eseguiamo nuovamente le operazioni attraverso un ciclo di 10 iterazioni 
        // ed eseguiamo i controlli come in precedenza
        for (int i = 1; i < 10; i++) {
            cache.put(1, i, entry);
        }
        assertEquals(10, cache.count());
        assertEquals(10 * bufferSize, cache.size());
        cache.put(1, 10, entry);
        for (int i = 0; i < 5; i++) {
            assertNull(cache.get(1, i));
        }
        for (int i = 5; i < 11; i++) {
            assertEquals(entry, cache.get(1, i));
        }
        
        // Si utilizza il metodo close per rialasciare le risorse della cache
        cache.close();
    }
    
    @Test
    public void checkEmptyAndReadingCacheMultiSegmentTest() {

    	System.out.println("checkEmptyAndReadingCacheMultiSegmentTest");
        
    	// Inizializzazione della cache attraverso la ReadCache con gli input 
    	// presi dai parametri del test definiti nel metodo configure() che essendo
    	// taggato con @Parameters viene utilizzato per ogni run di test
        ReadCache cache = new ReadCache(allocator, maxCacheSize, segmentSize);
        
        // Test della cache vuota
        System.out.println("Check empty " + maxCacheSize + " cache (created for reading) test");
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        assertEquals(null, cache.get(0, 0));
        
        // Eseguo ora un test di scrittura su un esempio di cache multilivello sempre 
        // utilizzando la put in combinazione con la get
        for (int i = 0; i < 10; i++) {
            ByteBuf entry = Unpooled.wrappedBuffer(new byte[bufferSize]);
            entry.setInt(0, i);
            cache.put(1, i, entry);
        }
        for (int i = 0; i < 10; i++) {
            ByteBuf res = cache.get(1, i);
            assertEquals(1, res.refCnt());
            assertEquals(bufferSize, res.readableBytes());
            assertEquals(i, res.getInt(0));
        }
        assertEquals(10, cache.count());
        assertEquals(10 * bufferSize, cache.size());
        ByteBuf entry = Unpooled.wrappedBuffer(new byte[bufferSize]);
        cache.put(2, 0, entry);
        assertEquals(9, cache.count());
        assertEquals(9 * bufferSize, cache.size());

        //Si utilizza il metodo close per rialasciare le risorse della cache
        cache.close();

    }

}
