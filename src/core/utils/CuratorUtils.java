package core.utils;

import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.retry.ExponentialBackoffRetry;

public class CuratorUtils {

	/**
	 * Settings
	 */
	private static int baseSleepTimeMills = 1000;	
	private static int maxRetries = 3;
	private static int waitTimeSeconds = 1000;
	
	
	/*
	 * Curator client utils
	 */
	
	public static CuratorFramework createClient(String zkHosts){
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMills, maxRetries);
		return CuratorFrameworkFactory.newClient(zkHosts, retryPolicy);
	}
	
	public static CuratorFramework createAndStartClient(String zkHosts){
		CuratorFramework client = createClient(zkHosts);
		client.start();
		return client;
	}
	
	public static void stopClient(CuratorFramework client){
		client.close();
	}
	
	
	/*
	 * Lock utils
	 */
	
	public static InterProcessLock acquireLock(CuratorFramework client, String lockPath){
		InterProcessLock lock = new InterProcessMutex(client, lockPath);			
		try {
			if (lock.acquire(waitTimeSeconds, TimeUnit.SECONDS))
				return lock;
			else
				throw new RuntimeException("Time out: Failed to obtain lock: "+ lockPath);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to obtain lock: "+lockPath+"\n "+e.getMessage());
		}
	}

	public static void releaseLock(InterProcessLock lock){
		try {
			lock.release();
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to unlock "+lock+"\n "+e.getMessage());
		}			
	}
		
	
	/*
	 * Counter utils 
	 */
	
	public static SharedCount createCounter(String hosts, String counterPath){
		CuratorFramework client = createClient(hosts);
		return new SharedCount(client, counterPath, 0);		
	}
	
	public static SharedCount createAndStartCounter(String zkHosts, String counterPath){
		SharedCount c = createCounter(zkHosts, counterPath);
		try {
			c.start();
			return c;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to start the counter: "+counterPath+"\n"+e.getMessage());
		}
	}
	
	public static SharedCount createAndStartCounter(CuratorFramework client, String counterPath){
		SharedCount c = new SharedCount(client, counterPath, 0);
		try {
			c.start();
			return c;
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to start the counter: "+counterPath+"\n"+e.getMessage());
		}
	}
	
	public static int getCounter(SharedCount c){
		return c.getCount();
	}
	
	public static int getCounter(CuratorFramework client, String counterPath){
		SharedCount c = new SharedCount(client, counterPath, 0);
		try {
			c.start();
			int val = c.getCount();
			c.close();
			return val;
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to add the counter: "+counterPath+"\n"+e.getMessage());
		}
	}
	
	public static void addCounter(SharedCount c, int increment){
		int oldVal = c.getCount();
		try {
			c.setCount(oldVal + increment);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to increment the counter: "+c);
		}
	}
	
	public static void addCounter(CuratorFramework client, String counterPath, int increment){
		SharedCount c = new SharedCount(client, counterPath, 0);
		try {
			c.start();
			int oldVal = c.getCount();
			c.setCount(oldVal + increment);
			c.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to add the counter: "+counterPath+"\n"+e.getMessage());
		}
	}
}
