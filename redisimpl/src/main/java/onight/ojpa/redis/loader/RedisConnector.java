package onight.ojpa.redis.loader;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.data.redis.listener.Topic;
import org.springframework.data.redis.serializer.RedisSerializer;

import lombok.extern.slf4j.Slf4j;
import onight.tfw.ntrans.api.exception.MessageException;
import onight.tfw.orouter.api.IQClient;
import onight.tfw.orouter.api.IRecievier;

@Slf4j
public class RedisConnector implements MessageListener {

	RedisTemplate<String, Object> template;
	LettuceConnectionFactory factory;

	ConcurrentHashMap<String, List<IRecievier>> topicReceivers = new ConcurrentHashMap<>();
	// public RedisConnectionFactory jRedisFactory(String addr,int port,int
	// poolsize) {
	// JedisPoolConfig poolConfig=new JedisPoolConfig();
	// poolConfig.setMaxTotal(poolsize);
	// JedisConnectionFactory lfactory=new JedisConnectionFactory(poolConfig);
	// lfactory.setHostName(addr);
	// lfactory.setPort(port);
	// lfactory.setUsePool(true);
	//// LettuceConnectionFactory lfactory = new LettuceConnectionFactory(addr,
	// port);
	//// lfactory.setShareNativeConnection(true);
	//
	// lfactory.afterPropertiesSet();
	//// lfactory.initConnection();
	// return lfactory;
	// }

	public LettuceConnectionFactory jRedisFactory(String addr, int port, int poolsize) {
		// LettuceConnectionFactory lfactory=new LettuceConnectionFactory();
		// lfactory.setHostName(addr);
		// lfactory.setPort(port);
		// lfactory.setUsePool(true);
		LettuceConnectionFactory lfactory = new LettuceConnectionFactory(addr, port);
		lfactory.setShareNativeConnection(true);
		lfactory.afterPropertiesSet();
		lfactory.initConnection();
		return lfactory;
	}

	RedisMessageListenerContainer rML = new RedisMessageListenerContainer();

	boolean started = false;

	private volatile RedisSerializer<String> serializer = RedisSerializer.string();

	public synchronized void onStart(String addr, int port, int poolsize) {
		log.info("Redis启动...");
		factory = jRedisFactory(addr, port, poolsize);

		template = new FastRedisTemplate<String, Object>();
		template.setConnectionFactory(factory);
		template.afterPropertiesSet();

		rML.setConnectionFactory(factory);
		rML.afterPropertiesSet();
		rML.start();
		ArrayList<Topic> topics = new ArrayList<>();
		this.createMessageListener(null, "redis_impl_man", new IRecievier() {

			@Override
			public boolean onMessage(String arg0, Serializable obj) {
				if (obj != null && obj instanceof byte[]) {
					log.info("get redis redis_impl_man message:" + new String((byte[])obj));
				} else {
					log.info("get redis redis_impl_man message:" + obj);
				}
				return true;
			}
		}, 0, 0);

		for (String key : topicReceivers.keySet()) {
			topics.add(new ChannelTopic(key));
		}
		rML.addMessageListener(this, topics);

		long size = exec(new RedisCallback<Long>() {
			public Long doInRedis(RedisConnection connection) throws DataAccessException {
				Long size = connection.dbSize();
				return size;
			}
		});
		log.info("Redis启动成功,存储个数：" + size);
		started = true;
	}

	public <T> T exec(RedisCallback<T> callback) {
		return template.execute(callback);
	}

	public void onDestory() {
		log.info("Redis退出...");
		rML.stop();
		factory.destroy();
		
		log.info("Redis退出成功");
	}

	@Override
	public void onMessage(Message message, byte[] pattern) {
		String topic = serializer.deserialize(message.getChannel());
		List<IRecievier> recs = topicReceivers.get(topic);
		for (IRecievier rec : recs) {
			rec.onMessage(topic, message.getBody());
		}
	}

	public synchronized void createMessageListener(IQClient arg0, String qName, IRecievier reciever, int arg3, int arg4)
			throws MessageException {
		// rML.addMessageListener(this, topics);
		List<IRecievier> recs = topicReceivers.get(qName);
		if (recs == null) {
			recs = new ArrayList<IRecievier>();
			topicReceivers.put(qName, recs);
			if (started) {
				// 已经启动的，重新注册这个topic
				ArrayList<Topic> topics = new ArrayList<>();
				topics.add(new ChannelTopic(qName));
				rML.addMessageListener(this, topics);
			}
		}
		recs.add(reciever);

	}

	public int getQueueSize(String arg0) {
		return 0;
	}

	public void removeQueue(String arg0) {
		throw new RedisJDAException("not supported removeQ");
	}

	public void sendMessage(String channel, Object obj) throws MessageException {
		template.convertAndSend(channel, obj);
	}

	public Object syncSendMessage(String channel, String arg1, Object obj) throws MessageException {
		throw new RedisJDAException("not supported syncSendMessage");
	}

}
