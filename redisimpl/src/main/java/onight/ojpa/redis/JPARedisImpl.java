package onight.ojpa.redis;

import org.apache.felix.ipojo.annotations.Component;
import org.apache.felix.ipojo.annotations.Instantiate;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Validate;
import org.osgi.framework.BundleContext;

import lombok.extern.slf4j.Slf4j;
import onight.ojpa.redis.loader.BatchDao;
import onight.ojpa.redis.loader.DaoRedisImpl;
import onight.ojpa.redis.loader.RedisConnector;
import onight.tfw.ntrans.api.ActorService;
import onight.tfw.ntrans.api.exception.MessageException;
import onight.tfw.ojpa.api.BatchExecutor;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.OJpaDAO;
import onight.tfw.ojpa.api.ServiceSpec;
import onight.tfw.ojpa.api.StoreServiceProvider;
import onight.tfw.orouter.api.IQClient;
import onight.tfw.orouter.api.IRecievier;
import onight.tfw.orouter.api.QService;
import onight.tfw.outils.conf.PropHelper;

@Component(immediate = true)
@Instantiate(name = "redisimpl")
@Provides(specifications = { ActorService.class, StoreServiceProvider.class, QService.class }, strategy = "SINGLETON")
@Slf4j
public class JPARedisImpl implements StoreServiceProvider, QService,ActorService {

	@Override
	public String getProviderid() {
		return ServiceSpec.REDIS_STORE.getTarget();
	}

	BundleContext bundleContext;
	PropHelper props;

	public JPARedisImpl(BundleContext bundleContext) {
		super();
		log.debug("create:JPARedisImpl:");
		this.bundleContext = bundleContext;
		redis = new RedisConnector();
	}

	@Validate
	public void startup() {
		props = new PropHelper(bundleContext);
		log.info("启动中...@" + bundleContext);
		redis.onStart(props.get("ofw.redis.addr", "172.30.12.44"), props.get("ofw.redis.port", 6379),
				props.get("ofw.redis.poolsize", 20));
		log.info("启动完成...");
	}

	@Invalidate
	public void shutdown() {
		log.info("退出中...");
		redis.onDestory();
		log.info("退出完成...");
	}

	RedisConnector redis;

	@Override
	public DomainDaoSupport getDaoByBeanName(DomainDaoSupport dao) {
		if (BatchExecutor.class.equals(dao.getDomainClazz())) {
			return new BatchDao(redis, (OJpaDAO) dao);
		}
		return new DaoRedisImpl(redis, (OJpaDAO) dao);
	}

	@Override
	public String[] getContextConfigs() {
		return null;
	}

	@Override
	public void createMessageListener(IQClient arg0, String qName, IRecievier reciever, int arg3, int arg4)
			throws MessageException {
		redis.createMessageListener(arg0, qName, reciever, arg3, arg4);
		;
	}

	@Override
	public int getQueueSize(String arg0) {
		return redis.getQueueSize(arg0);
	}

	@Override
	public void removeQueue(String arg0) {
		redis.removeQueue(arg0);
	}

	@Override
	public void sendMessage(String arg0, Object arg1) throws MessageException {
		redis.sendMessage(arg0, arg1);
	}

	@Override
	public Object syncSendMessage(String channel, String arg1, Object obj) throws MessageException {
		return redis.syncSendMessage(channel, arg1, obj);
	}

}
