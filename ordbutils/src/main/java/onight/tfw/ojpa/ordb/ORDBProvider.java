package onight.tfw.ojpa.ordb;

import java.util.concurrent.LinkedBlockingDeque;

import org.apache.felix.ipojo.annotations.Bind;
import org.apache.felix.ipojo.annotations.Invalidate;
import org.apache.felix.ipojo.annotations.Provides;
import org.apache.felix.ipojo.annotations.Unbind;
import org.apache.felix.ipojo.annotations.Validate;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.springframework.context.support.AbstractXmlApplicationContext;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.SqlMapper;
import onight.tfw.ojpa.api.StoreServiceProvider;
import onight.tfw.ojpa.ordb.loader.CommonSqlMapper;
import onight.tfw.ojpa.ordb.loader.SpringContextLoader;

//@Component(immediate = true)
//@Instantiate(name = "mysqlimpl")
@Provides(specifications = StoreServiceProvider.class, strategy = "SINGLETON")
@Slf4j
public abstract class ORDBProvider implements StoreServiceProvider {

	// @Override
	// public String getProviderid() {
	// return ServiceSpec.MYSQL_STORE.getTarget();
	// }

	protected BundleContext bundleContext;

	protected SpringContextLoader springLoader;

	public ORDBProvider(BundleContext bundleContext) {
		super();
		log.debug("create:JPAORDBImpl:");
		this.bundleContext = bundleContext;
	}
	
	public ORDBProvider(BundleContext bundleContext,String sql_target) {
		super();
		log.debug("create:JPAORDBImpl:with target:"+sql_target);
		this.sql_target = sql_target;
		this.bundleContext = bundleContext;
	}


	@AllArgsConstructor
	public class SqlMapperInfo {
		Object sqlmapper;
		String sf;
	}

	LinkedBlockingDeque<SqlMapperInfo> wishlist = new LinkedBlockingDeque<SqlMapperInfo>();
	
	String sql_target = "";
	
	@Validate
	public synchronized void startup() {
		log.info("启动中...@" + bundleContext);
		if (springLoader == null) {
			springLoader = new SpringContextLoader();
		}
		springLoader.init(bundleContext, getContextConfigs(),sql_target);
		springLoader.registerDaoBeans();
		for (SqlMapperInfo mapper : wishlist) {
			springLoader.registerMapper(mapper.sqlmapper, mapper.sf);
		}
		wishlist.clear();
		log.info("启动完成...");
	}

	@Invalidate
	public void shutdown() {
		log.info("退出中...");
		if (springLoader != null) {
			springLoader.destory();
		}
		log.info("退出完成...");
	}

	@Bind(aggregate = true, optional = true)
	public synchronized void bindMapper(SqlMapper mapper, ServiceReference sf) {
		if (springLoader != null) {
			springLoader.registerMapper(mapper, String.valueOf(sf.getBundle().getBundleId()));
		} else {
			wishlist.add(new SqlMapperInfo(mapper, String.valueOf(sf.getBundle().getBundleId())));
		}
	}

	@Unbind(aggregate = true, optional = true)
	public synchronized void unbindMapper(SqlMapper mapper, ServiceReference sf) {
		wishlist.remove(mapper);
		if (springLoader != null) {
			springLoader.unregisterMapper(mapper, String.valueOf(sf.getBundle().getBundleId()));
		}
	}

	@Override
	public DomainDaoSupport getDaoByBeanName(DomainDaoSupport dao) {
		if (springLoader != null) {
			return springLoader.getBeans(dao.getDomainName() + "Dao");
		} else {
			//log.warn("bean dao not found:" + dao.getDomainName());
			return null;
		}
	}

	public StaticTableDaoSupport getStaticDao(String beanname) {
		return springLoader.getStaticDao(beanname + "Dao");
	}

	public CommonSqlMapper getCommonSqlMapper() {
		if (springLoader != null) {
			return (CommonSqlMapper) springLoader.getSpringBeans("commonSqlMapper");
		} else {
			//log.warn("bean getCommonSqlMapper not found:");
			return null;
		}
	}

	public Object getApplicationCtx() {
		return springLoader.getAppContext();
	}

}
