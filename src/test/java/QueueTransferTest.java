/*
 * Copyright Chris2018998
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import cn.beecp.util.FastTransferQueue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * Transfer Packet Performance Test
 *
 * @author Chris.Liao
 */
public class QueueTransferTest {
	public static void main(String[] args) throws Exception {
		int producerSize = Runtime.getRuntime().availableProcessors(), consumerSize = 100;
		System.out.println(".................QueueTransferTest......................");
		ArrayList<Long> timeList = new ArrayList<Long>(5);
		timeList.add(testQueue("ArrayBlockingQueue", new ArrayBlockingQueue<TransferPacket>(producerSize), producerSize,
				consumerSize));
		timeList.add(testQueue("LinkedBlockingQueue", new LinkedBlockingQueue<TransferPacket>(), producerSize,
				consumerSize));
		timeList.add(testQueue("LinkedTransferQueue", new LinkedTransferQueue<TransferPacket>(), producerSize,
				consumerSize));
		timeList.add(testQueue("SynchronousQueue", new SynchronousQueue<TransferPacket>(), producerSize,
				consumerSize));
		timeList.add(testQueue("FastTransferQueue", new FastTransferQueue<TransferPacket>(), producerSize,
				consumerSize));
		Collections.sort(timeList);
		System.out.println(timeList);
	}

	private static long testQueue(String queueName,Queue<TransferPacket>queue,int producerSize,int consumerSize) throws Exception {
		BlqConsumer[] blkConsumers=null;
		FstQConsumer[] fstConsumers=null;
		CountDownLatch producersDownLatch = new CountDownLatch(producerSize);
		CountDownLatch consumersDownLatch = new CountDownLatch(consumerSize);
		AtomicBoolean existConsumerInd = new AtomicBoolean(true);

		if(queue instanceof BlockingQueue){//Blocking Queue Consumers
			BlockingQueue<TransferPacket> blockingQueue=(BlockingQueue<TransferPacket>)queue;
			blkConsumers = new BlqConsumer[consumerSize];
			for (int i = 0; i < consumerSize; i++) {
				blkConsumers[i] = new BlqConsumer(blockingQueue,consumersDownLatch);
				blkConsumers[i].start();
			}
		}else {//Fast Transfer Queue Consumers
			FastTransferQueue<TransferPacket> fastTransferQueue=(FastTransferQueue<TransferPacket>)queue;
			fstConsumers = new FstQConsumer[consumerSize];
			for (int i = 0; i < consumerSize; i++) {
				fstConsumers[i] = new FstQConsumer(fastTransferQueue,consumersDownLatch);
				fstConsumers[i].start();
			}
		}
		
		LockSupport.parkNanos(TimeUnit.SECONDS.toNanos(10));

		// Producers
		for (int i = 0; i < producerSize; i++) {
			new Producer(queue, existConsumerInd, producersDownLatch).start();
		}

		consumersDownLatch.await();
		existConsumerInd.set(false);
		producersDownLatch.await();

		// Summary and Conclusion
		int totPacketSize = 0;
		BigDecimal totTime = new BigDecimal(0);
		if(queue instanceof BlockingQueue){
			for (int i = 0; i < consumerSize; i++) {
				TransferPacket packet = blkConsumers[i].getTransferPacket();
				totPacketSize++;
				totTime = totTime.add(new BigDecimal(packet.arriveTime - packet.sendTime));
			}
		}else{
			for (int i = 0; i < consumerSize; i++) {
				TransferPacket packet = fstConsumers[i].getTransferPacket();
				totPacketSize++;
				totTime = totTime.add(new BigDecimal(packet.arriveTime - packet.sendTime));
			}	
		}
		
		BigDecimal avgTime = totTime.divide(new BigDecimal(totPacketSize), 0, BigDecimal.ROUND_HALF_UP);
		System.out.println("<" + queueName + "> producer-size:" + producerSize + ",consumer-size:" + consumerSize
				+ ",total packet size:" + totPacketSize + ",total time:" + totTime.longValue() + "(ns),avg time:"
				+ avgTime + "(ns)");

		return avgTime.longValue();
	}

	static final class TransferPacket {
		long sendTime = System.nanoTime();
		long arriveTime;
	}

	static final class Producer extends Thread {
		private AtomicBoolean activeInd;
		private Queue<TransferPacket> queue;
		private CountDownLatch producersDownLatch;

		public Producer(Queue<TransferPacket> queue, AtomicBoolean activeInd, CountDownLatch producersDownLatch) {
			this.queue = queue;
			this.activeInd = activeInd;
			this.producersDownLatch = producersDownLatch;
		}

		public void run() {
			while (activeInd.get()) {
				queue.offer(new TransferPacket());
			}
			producersDownLatch.countDown();
		}
	}

	static final class BlqConsumer extends Thread {
		private CountDownLatch latch;
		private BlockingQueue<TransferPacket> queue;
		private TransferPacket packet = null;

		public BlqConsumer(BlockingQueue<TransferPacket> queue, CountDownLatch latch) {
			this.queue = queue;
			this.latch = latch;
		}

		public TransferPacket getTransferPacket() {
			return packet;
		}

		public void run() {
			try {
				packet = queue.poll(Long.MAX_VALUE,TimeUnit.NANOSECONDS);
				packet.arriveTime = System.nanoTime();
			} catch (InterruptedException e) {
			}
			latch.countDown();
		}
	}

	static final class FstQConsumer extends Thread {
		private CountDownLatch latch;
		private FastTransferQueue<TransferPacket> queue;
		private TransferPacket packet = null;

		public FstQConsumer(FastTransferQueue<TransferPacket> queue, CountDownLatch latch) {
			this.queue = queue;
			this.latch = latch;
		}

		public TransferPacket getTransferPacket() {
			return packet;
		}

		public void run() {
			try {
				packet = queue.poll(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
				packet.arriveTime = System.nanoTime();
			} catch (InterruptedException e) {
			}
			latch.countDown();
		}
	}
}
