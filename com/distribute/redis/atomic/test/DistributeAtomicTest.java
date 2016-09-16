package com.distribute.redis.atomic.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.distribute.redis.atomic.DistributeAtomicIntegerUtil;

import redis.clients.jedis.Jedis;

public class DistributeAtomicTest{
	
	private static CountDownLatch countDownLatch = new CountDownLatch(1);
	private static AtomicInteger record = new AtomicInteger(0);
	
	public static class TestThread extends Thread{
		TestThread(String name){
			super.setName(name);
		}
		public void run(){
			Jedis jedis = new Jedis("127.0.0.1",6379);
			DistributeAtomicIntegerUtil util = new DistributeAtomicIntegerUtil(jedis,"value",0,-1);
			try {
				countDownLatch.await();
			} catch (InterruptedException e) {  
				e.printStackTrace();
			}
			for (int i = 0; i < 11; i++) {
				if(!util.changeUtilDeadline()){
					System.out.println("thread_name: " + Thread.currentThread().getName() + "; quit the value;");
				}else{
					System.out.println("thread_name: " + Thread.currentThread().getName() + "; got the value:" + record.addAndGet(1));
				}
				jedis.disconnect();
			}
		}
	}
	
	
	
	public static void main(String[] args){
		Jedis jedis = new Jedis("127.0.0.1",6379);
		jedis.set("value","90");
		for(int i = 0; i < 11; i++){
			TestThread t = new TestThread("test" + i);
			t.start();
		}
		countDownLatch.countDown();
	}
}
	