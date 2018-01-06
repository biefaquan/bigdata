package cn.itcast.bigdata.zkdist;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class DistributedServer {
	private static final String connectString = "mini1:2181,mini2:2181,mini3:2181";  
	private static final int sessionTimeout = 2000;
	private static final String parentNode = "/servers";
	
	private ZooKeeper zk = null;
	
	/**
	 * 创建到ZK客户端的连接
	 * @throws Exception
	 */
	public void getConnect() throws Exception{
		zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				//收到事件通知后的回调函数（应该是我们自己的事件处理逻辑）
				System.out.println(event.getType() + "---" + event.getPath());
				//此时
//				try {
//					zk.getChildren("/", true);
//				} catch (Exception e) {
//					e.printStackTrace();
//				}
			}
		});
	}
	/**
	 * 向ZK集群注册服务信息
	 * @param hostName
	 * @throws Exception
	 */
	public void registerServer(String hostName) throws Exception {
		String create = zk.create(parentNode + "/server", hostName.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println(hostName + "is online.." + create);
	}
	
	/**
	 * 业务功能
	 * @throws Exception 
	 */
	public void handleBussiness(String hostName) throws Exception {
		System.out.println(hostName + "is working.......");
		Thread.sleep(Long.MAX_VALUE);
	}
	
	public static void main(String[] args) throws Exception {
		//获取ZK连接
		DistributedServer server = new DistributedServer();
		server.getConnect();
		//利用zk连接注册服务器信息
		server.registerServer(args[0]);
		//业务功能
		server.handleBussiness(args[0]);
	}
}
