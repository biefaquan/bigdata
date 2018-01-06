package cn.itcast.bigdata.zk;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

public class SimpleZkClient {
	private static final String connectString = "mini1:2181,mini2:2181,mini3:2181";
	private static final int sessionTimeout = 2000;
	ZooKeeper zkClient = null;
	
	@Before
	public void init() throws Exception {
		zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				//收到事件通知后的回调函数
				System.out.println(event.getType() + "====" +event.getPath());
				try {
					zkClient.getChildren("/", true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	} 
	/**
	 * 数据的增删改查
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	//创建数据节点到zk中
	@Test
	public void testCreate() throws KeeperException, InterruptedException {
		//参数1：要创建节点的路径   参数2：节点大数据    参数3：节点的权限    参数4：节点的类型
		String nodeCreated = zkClient.create("/eclipse", "hellozk".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		//上传的数据可以使任何类型，但都要转成byte[]
	}  
	
	/**
	 * 判断znode是否存在
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void testExits() throws KeeperException, InterruptedException {
		Stat stat = zkClient.exists("/eclipse", false);
		System.out.println(stat==null?"not exist":"exist");
	}
	
	/**
	 * 获取子节点
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void getChildren() throws KeeperException, InterruptedException {
		List<String> list = zkClient.getChildren("/", true);
		System.out.println(list.toString());
		Thread.sleep(Long.MAX_VALUE);
	}
	
	/**
	 * 获取znode的数据
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void getData() throws KeeperException, InterruptedException {
		byte[] data= zkClient.getData("/eclipse", false, new Stat());
		System.out.println(new String(data));
	}
	
	/**
	 * 删除znode的数据
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void delZnode() throws KeeperException, InterruptedException {
		//参数2：指定要删除的版本，-1表示删除所有版本
		zkClient.delete("/eclipse", -1);
		System.out.println("删除成功！！");
	}
	
	/**
	 * 修改znode的数据
	 * @throws InterruptedException 
	 * @throws KeeperException 
	 */
	@Test
	public void setData() throws KeeperException, InterruptedException {
		//参数2：指定要删除的版本，-1表示删除所有版本
		Stat stat = zkClient.setData("/app1", "setOK".getBytes(), -1);
		byte[] data = zkClient.getData("/app1", false, null);
		System.out.println(new String(data));
	}
}
