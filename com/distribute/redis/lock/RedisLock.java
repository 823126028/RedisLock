package com.distribute.redis.lock;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

public class RedisLock {
	
	private final String PATH;
	private final static long EXPIRE_TIME = 5000;
	private final String UUID;
	private final Jedis jedis;
	private InnerLock innerLock;
	
	public RedisLock(Jedis jedis, String path, UUIDGenerator uuidGenerator){
		this.PATH = path;
		UUID = uuidGenerator.generate();
		this.jedis = jedis;
		this.innerLock = null;
	}
	
	private String getPathValue(){
		String abc = new String(jedis.get(this.PATH.getBytes()));
		return abc;
	}
	
	public static class InnerLock{
		private long lockedTime;
		private String uuid;
		private final static String REGEX = ":";
		
		public InnerLock(String uuid){
			this.uuid = uuid;
			this.lockedTime = System.currentTimeMillis();
		}
		
		
		public InnerLock(String uuid,long lockedTime){
			this.uuid = uuid;
			this.lockedTime = lockedTime;
		}
		
		public boolean isExpired(){
			return System.currentTimeMillis() - lockedTime > EXPIRE_TIME;
		}
		
		public boolean isOwner(String uuid){
			if(this.uuid.equals(uuid)){
				return true;
			}
			return false;
		}
		
		public String toString(){
			return this.uuid + REGEX + lockedTime;
		}
		
		public static InnerLock makeInnerLock(String value){
			String[] array = value.split(REGEX);
			return new InnerLock(array[0], Long.parseLong(array[1]));
		}
	}
	
	public void release(){
		if(this.innerLock == null || this.innerLock.isExpired()){
			this.innerLock = null;
			return;
		}
		for(;;){
			InnerLock currentLock = InnerLock.makeInnerLock(getPathValue());
			if(currentLock.isOwner(this.UUID)){
				try{
					jedis.watch(this.PATH);
					if(!getPathValue().equals(currentLock.toString())){
						continue;
					}
					Transaction tx = jedis.multi();
					tx.del(this.PATH);
					if(tx.exec() != null){
						this.innerLock = null;
						return;
					}
				}finally{
					jedis.unwatch();
				}
			}else{
				this.innerLock = null;
				return; 
			}
		}
	}
	
	public boolean acquire(long retryTotalTime, long rest){
		long totalTime = retryTotalTime;
		while(totalTime > 0){
			InnerLock myLock = new InnerLock(this.UUID);
			if(jedis.setnx(this.PATH.getBytes(), myLock.toString().getBytes()) == 1){
				this.innerLock = myLock;
				return true;
			}
			InnerLock currentLock = InnerLock.makeInnerLock(getPathValue());
			if(currentLock.isExpired() || currentLock.isOwner(myLock.uuid)){
				try{
					//防止没到wacth就已经更改了.
					jedis.watch(this.PATH);
					if(!getPathValue().equals(currentLock.toString())){
						//中间被改了,进行下一次尝试
						continue;
					}
					Transaction tx = jedis.multi();
					tx.set(this.PATH.getBytes(), myLock.toString().getBytes());
					if(tx.exec() != null){
						this.innerLock = myLock;
						return true;
					}
				}finally{
					jedis.unwatch();
				}
			}
			totalTime -= rest;
			try {
				Thread.sleep(totalTime);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return false;
	}
    public static void main(String[] args){
    	Jedis jedis = new Jedis("127.0.0.1", 6379);
    	RedisLock redisLock = new RedisLock(jedis, "lock",new RawUUIDGenerator());
    	redisLock.acquire(2000,100);
    	redisLock.acquire(2000,100);
    	redisLock.release();
    }
	
}
