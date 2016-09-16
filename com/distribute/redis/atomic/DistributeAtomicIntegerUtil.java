package com.distribute.redis.atomic;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

/**
 * @author chenjl
 * 分布式的原子变量 
 */
public class DistributeAtomicIntegerUtil{
	private int deadLineValue;
	private int delta;
	private Jedis jedis;
	private String path;
	public DistributeAtomicIntegerUtil(Jedis jedis,String path,int dlValue,int _delata){
		if(_delata == 0){
			throw new RuntimeException("delta can't be zero");
		}
		this.deadLineValue = dlValue;
		this.delta = _delata;
		this.jedis = jedis;
		this.path = path;
	}
	
	/**
	 * if delta < 0,it's a minus operation, we should minus util beyond a num
	 * otherwise we should add util we below a num
	 * @param recordNum
	 * @return
	 */
	private boolean checkDeadLine(int recordNum){
		if(delta < 0){
			return recordNum + delta >= deadLineValue;
		}else{
			return recordNum + delta <= deadLineValue;
		}
	}
	
	/**
	 *
	 */
	public boolean changeUtilDeadline(){
		for(;;){
			int recordNum = Integer.parseInt(jedis.get(path));
			// do a check, if not meet,return.
			if(!checkDeadLine(recordNum)){
				return false;
			}
			try{
				//do watch ,
				jedis.watch(this.path);
				int watchRecordNum = Integer.parseInt(jedis.get(path));
				//防止没到watch就改了,if already changed, next compare
				if(recordNum != watchRecordNum){
					continue;
				}
				//Optimistic try, use wacth and mutl.transaction
				//if watched value changed,tx.exec will be null
				Transaction tx = jedis.multi();
				tx.set(this.path, "" + (watchRecordNum + delta));
				if(tx.exec() != null){
					return true;
				}
			}finally{
				jedis.unwatch();
			}
		}
	}	
}
