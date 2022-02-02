package org.apache.bookkeeper.bookie.storage.ldb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.runner.RunWith;
import java.util.Arrays;


@RunWith(Parameterized.class)
public class MyWriteCacheTest {

    private ByteBufAllocator allocator;
    private long maxCacheSize;
    private int bufferSize;

    @Parameters
	public static Collection<Object[]> configure(){
		return Arrays.asList(new Object[][] {
			{UnpooledByteBufAllocator.DEFAULT, 10*1024,1024},
				{UnpooledByteBufAllocator.DEFAULT, 1000 * 1024,1024},
				{UnpooledByteBufAllocator.DEFAULT, 10*1024,512}, 
				{UnpooledByteBufAllocator.DEFAULT, 10*1024,2048}
		});
	}

    public MyWriteCacheTest(ByteBufAllocator allocator, long maxCacheSize, int bufferSize){
        this.maxCacheSize = maxCacheSize;
        this.allocator = allocator;
        this.bufferSize = bufferSize;
    }

    @Test
    public void checkEmptyAndWriteCacheSingleSegmentTest() throws Exception {
        
    	System.out.println("checkEmptyAndWriteCacheSingleSegmentTest");
    	
    	// Inizializzazione della cache utilizzando la WriteCache con gli input 
    	// presi dai parametri del test definiti nel metodo configure() che essendo
    	// taggato con @Parameters i suoi parametri vengono utilizzati per ogni run di test
    	WriteCache cache = new WriteCache(allocator, maxCacheSize, bufferSize);
        
        // Test della cache vuota utilizzando e quindi testando il corretto funzionamento
    	// del metodo isEmpty, count e size della classe under test, la WriteCache
    	System.out.println("Check empty " + maxCacheSize + " cache (created for writing) test");
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        
        // Test dell'inserimento di dati nella cache utilizzando i metodi
        // put, size, count della classe WriteCache 
    	System.out.println("Check writing " + maxCacheSize + " cache test");
        ByteBuf entry = allocator.buffer(bufferSize);
        ByteBufUtil.writeUtf8(entry, "entry-1");
        entry.writerIndex(entry.capacity());
        cache.put(1, 1, entry);
        assertFalse(cache.isEmpty());
        assertEquals(1, cache.count());
        assertEquals(entry.readableBytes(), cache.size());
        assertEquals(entry, cache.getLastEntry(1));
        assertEquals(null, cache.getLastEntry(2));
        
        // Si utilizza il metodo clear per rialasciare le risorse della cache e
        // i metodi isEmpty, count e size per controllare la corretta esecuzione
    	System.out.println("Check clearing " + maxCacheSize + " cache writed test");
        cache.clear();
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        entry.release();
        cache.close();     
    }
    
    @Test
    public void fillCacheFullTest() throws Exception {
    	
    	System.out.println("fillCacheFullTest");
    	
    	// Inizializzazione della cache utilizzando la WriteCache con gli input 
    	// presi dai parametri del test definiti nel metodo configure() che essendo
    	// taggato con @Parameters i suoi parametri vengono utilizzati per ogni run di test
        WriteCache cache = new WriteCache(allocator, maxCacheSize,bufferSize);
    	
        
        // Eseguiamo un test di fill completo della cache con le put per poi eseguire i 
        // controlli isEmpty, count e in questo caso (per completare la copertura della
        // WriteCache utilizziamo anche il metodo forEach per il controllo della corretta scrittura
        int entriesCount = (int) (maxCacheSize / bufferSize);
        ByteBuf entry = allocator.buffer(bufferSize);
        entry.writerIndex(entry.capacity());

    	System.out.println("Check filling " + maxCacheSize + " cache test");
        for (int i = 0; i < entriesCount; i++) {
            assertTrue(cache.put(1, i, entry));
        }
        assertFalse(cache.put(1, 11, entry));
        assertFalse(cache.isEmpty());
        assertEquals(entriesCount, cache.count());
        assertEquals(maxCacheSize, cache.size());
        AtomicInteger findCount = new AtomicInteger(0);
        cache.forEach((ledgerId, entryId, data) -> {
            findCount.incrementAndGet();
        });        
        assertEquals(entriesCount, findCount.get());
        cache.deleteLedger(1);
        findCount.set(0);
        cache.forEach((ledgerId, entryId, data) -> {
            findCount.incrementAndGet();
        });
        assertEquals(0, findCount.get());
        
        //Si utilizza il metodo close per rialasciare le risorse della cache
        entry.release();
        cache.close();
    }

}
