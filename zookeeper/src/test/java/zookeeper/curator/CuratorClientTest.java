package zookeeper.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CuratorClientTest {
    //根据自己集群的实际情况，Zookeeper info 替换
    private static final String ZK_ADDRESS = "node01:2181,node02:2181,node03:2181";
    private static final String ZK_PATH = "/zk_test";
    private static final String ZK_PATH01 = "/beijing/goddess/anzhulababy";
    private static final String ZK_PATH02 = "/beijing/goddess";

    //初始化，建立连接
    @Before
    public void init() {
        //重试连接策略，失败重试次数；每次休眠5000毫秒
        //RetryPolicy policy = new ExponentialBackoffRetry(3000, 3);
        RetryNTimes retryPolicy = new RetryNTimes(10, 5000);

        // 1.设置客户端参数，参数1：指定连接的服务器集端口列表；参数2：重试策略
        client = CuratorFrameworkFactory.newClient(ZK_ADDRESS, retryPolicy);
        //启动客户端，连接到zk集群
        client.start();

        System.out.println("zookeeper客户端启动成功");
    }

    static CuratorFramework client = null;

    @After
    //关闭连接
    public void clean() {
        System.out.println("close session");
        client.close();
    }

    // 创建永久节点
    @Test
    public void createPersistentZNode() throws Exception {
        String zNodeData = "火辣的";

        ///a/b/c
        client.create().
                creatingParentsIfNeeded().          //如果父目录不存在，则创建
                withMode(CreateMode.PERSISTENT).    //创建永久节点
                forPath(ZK_PATH01, zNodeData.getBytes());//指定路径及节点数据
    }

    @Test
    // 创建临时节点
    public void createEphemeralZNode() throws Exception {
        String zNodeData2 = "流星雨";
        client.create().
                creatingParentsIfNeeded().
                withMode(CreateMode.EPHEMERAL).
                forPath("/beijing/star", zNodeData2.getBytes());

        Thread.sleep(5000);
    }

    @Test
    //查询znode数据
    public void queryZNodeData() throws Exception {
        // 查询列表
        System.out.println("查看子znode");
        print(client.getChildren().forPath("/"));

        //查询节点数据
        System.out.println("查询znode的数据");
        if (client.checkExists().forPath(ZK_PATH01) != null) {//判断znode是否存在
            print(client.getData().forPath(ZK_PATH01));//获得znode的数据
        } else {
            System.out.println("节点不存在");
        }
    }

    @Test
    public void modifyZNodeData() throws Exception {
        //修改前的数据
        System.out.println("修改前，znode数据是：");
        print(client.getData().forPath(ZK_PATH01));

        String data2 = "welcome to jumanji";
        System.out.println("修改znode数据为 welcome to jumanji");

        // 修改节点数据
        client.setData().forPath(ZK_PATH01, data2.getBytes());
        print("get", ZK_PATH01);
        //修改后的数据
        System.out.println("修改后，znode数据是：");
        print(client.getData().forPath(ZK_PATH01));
    }

    @Test
    public void deleteZNode() throws Exception {
        // 删除节点
        System.out.println("删除节点" + ZK_PATH01);
        client.delete().forPath(ZK_PATH01);

        System.out.println("查看" + ZK_PATH02 + "的子znode");
        print(client.getChildren().forPath(ZK_PATH02));
    }

    @Test
    //监听ZNode
    public void watchZNode() throws Exception {

        //cache: TreeCache\PathChildrenCache\DataCache
        /**
         * TreeCache:可以将指定的路径节点作为根节点，对其所有的子节点操作进行监听，呈现树形目录的监听
         * PathChildrenCache:监听节点下一级子节点的增、删、改操作
         * NodeCache:监听节点对应增、删、改操作
         *
         *  TreeCache:将指定的path下所有的子znode的数据缓存起来；
         *  该类将监控path，可监控更新、创建、删除、获取数据等事件
         *  可以注册一个监听器，当以上事件发生时，将都到通知
         */
        //设置节点的cache
        TreeCache treeCache = new TreeCache(client, ZK_PATH02);
        //设置监听器、及收到通知后的处理过程
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                ChildData data = event.getData();
                if (data != null) {
                    switch (event.getType()) {
                        case NODE_ADDED:
                            System.out.println("NODE_ADDED : " + data.getPath() + "  数据:" + new String(data.getData()));
                            break;
                        case NODE_REMOVED:
                            System.out.println("NODE_REMOVED : " + data.getPath() + "  数据:" + new String(data.getData()));
                            break;
                        case NODE_UPDATED:
                            System.out.println("NODE_UPDATED : " + data.getPath() + "  数据:" + new String(data.getData()));
                            break;
                        default:
                            break;
                    }
                } else {
                    System.out.println("data is null : " + event.getType());
                }
            }
        });

        //开始监听
        treeCache.start();
        Thread.sleep(10000);
        //关闭cache
        System.out.println("关闭treeCache");
        treeCache.close();
    }

    @Test
    public void watchPathChildren() throws Exception {
        //path cache
        ///zktest/b/a
        //PathChildrenCache:监听节点下一级子节点的增、删、改操作
        PathChildrenCache pathCache = new PathChildrenCache(client, ZK_PATH, true);

        //Listener for PathChildrenCache changes
        PathChildrenCacheListener listener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case CHILD_ADDED: {
                        System.out.println("Node added: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }

                    case CHILD_UPDATED: {
                        System.out.println("Node changed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }

                    case CHILD_REMOVED: {
                        System.out.println("Node removed: " + ZKPaths.getNodeFromPath(event.getData().getPath()));
                        break;
                    }
                    default:
                        break;
                }
            }
        };

        //添加监听器
        pathCache.getListenable().addListener(listener);

        //开启缓存
        pathCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        System.out.println("成功注册zk pathCache");

        Thread.sleep(20000);
        //关闭缓存
        pathCache.close();
    }


    private static void print(String... cmds) {
        StringBuilder text = new StringBuilder("$ ");
        for (String cmd : cmds) {
            text.append(cmd).append(" ");
        }
        System.out.println(text.toString());
    }

    private static void print(Object result) {
        System.out.println(
                result instanceof byte[]
                        ? new String((byte[]) result)
                        : result);
    }
}
