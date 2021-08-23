package org.apache.bookkeeper.bookie.storage.ldb;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.nio.charset.Charset;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.Test;
import java.util.Collection;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import java.util.Arrays;


@RunWith(Parameterized.class)
public class WriteCacheTest {

    private ByteBufAllocator allocator;
    private long maxCacheSize;
    private int bufferSize;

    @Parameters
	public static Collection<Object[]> configure(){
        //Scegliamo i parametri per testare il metodo in test, vogliamo andare a testare i metodi put e clear oltre al costruttore
        //questi prendono come argomenti solamente un long e un allocator, (il metodo put prende anche due id che metterò hardcoded)
        //ho deciso di aggiungere l'atttributo bufferSize per vedere se aggiungendo entry di dimensione diversa da quella della cache ci sarebbero stati problemi
		return Arrays.asList(new Object[][] {
			//{UnpooledByteBufAllocator.DEFAULT, 10*1024,1024},{UnpooledByteBufAllocator.DEFAULT, 1000 * 1024,1024},  {UnpooledByteBufAllocator.DEFAULT, 0,01 * 1024,1024}, {UnpooledByteBufAllocator.DEFAULT, 10*1024,512}, {UnpooledByteBufAllocator.DEFAULT, 10*1024,2048}
			{UnpooledByteBufAllocator.DEFAULT, 10*1024,1024}
		});
	}

    public WriteCacheTest(ByteBufAllocator allocator, long maxCacheSize, int bufferSize){
        this.maxCacheSize = maxCacheSize;
        this.allocator = allocator;
        this.bufferSize = bufferSize;
    }

    @Test
    public void create_Clear_Delete_Test() throws Exception {
        WriteCache cache = new WriteCache(allocator, maxCacheSize, bufferSize);
        ByteBuf entry1 = allocator.buffer(bufferSize);
        ByteBufUtil.writeUtf8(entry1, "entry-1");
        entry1.writerIndex(entry1.capacity());
        System.out.println("sono qui");
        assertTrue(cache.isEmpty());
        System.out.println("sono qua");
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        cache.put(1, 1, entry1);
        assertFalse(cache.isEmpty());
        assertEquals(1, cache.count());
        assertEquals(entry1.readableBytes(), cache.size());
        assertEquals(entry1, cache.getLastEntry(1));
        assertEquals(null, cache.getLastEntry(2));
        cache.clear();
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.count());
        assertEquals(0, cache.size());
        entry1.release();
        cache.close();
    }

    //da mettere ignore
    @Test
    public void testForDeleteAndGet(){
        WriteCache cache = new WriteCache(allocator, maxCacheSize,bufferSize);
        ByteBuf entry = allocator.buffer(bufferSize);
        entry.writerIndex(entry.capacity());
        //inserisco una serie di entry nella cache per poi andarle a cercare ed eliminare per testare i due metodi
        for (long ledgerId = 0; ledgerId < 10; ledgerId++) {
            for (int entryId = 0; entryId < 10; entryId++) {
                cache.put(ledgerId, entryId, entry);
            }
        }
        for(long c = 0; c <10; c ++) {
             //assertEquals((cache.get(c,c)),);
             cache.deleteLedger(c);
             //assertEquals((cache.get(c,c)),null);
    }

}

}
