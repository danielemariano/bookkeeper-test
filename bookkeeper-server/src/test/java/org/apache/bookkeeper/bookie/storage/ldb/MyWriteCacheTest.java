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
        // Scegliamo i parametri per testare il metodo in test, vogliamo andare a testare i
        // metodi put e clear oltre al costruttore questi prendono come argomenti solamente
        // un long e un allocator, (il metodo put prende anche due id che metterò hardcoded)
        // ho deciso di aggiungere l'atttributo bufferSize per vedere se aggiungendo entry
        // di dimensione diversa da quella della cache ci sarebbero stati problemi
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
    public void checkEmptyAndWriteCacheTestSingleSegment() throws Exception {
        
    	System.out.println("checkEmptyAndWriteCacheTestSingleSegment");
    	WriteCache cache = new WriteCache(allocator, maxCacheSize, bufferSize);
        
    	System.out.println("Check empty " + maxCacheSize + " cache (created for writing) test");
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        
    	System.out.println("Check writing " + maxCacheSize + " cache test");
        ByteBuf entry1 = allocator.buffer(bufferSize);
        ByteBufUtil.writeUtf8(entry1, "entry-1");
        entry1.writerIndex(entry1.capacity());
        cache.put(1, 1, entry1);
        assertFalse(cache.isEmpty());
        assertEquals(1, cache.count());
        assertEquals(entry1.readableBytes(), cache.size());
        assertEquals(entry1, cache.getLastEntry(1));
        assertEquals(null, cache.getLastEntry(2));
        
    	System.out.println("Check clearing " + maxCacheSize + " cache writed test");
        cache.clear();
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        entry1.release();
        cache.close();     
    }
    
    @Test
    public void cacheFull() throws Exception {
    	
    	System.out.println("cacheFull");
        WriteCache cache = new WriteCache(allocator, maxCacheSize,bufferSize);
    	
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
        entry.release();
        cache.close();
    }

}
