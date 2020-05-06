package onight.tfw.ojpa.ordb.loader;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.mapper.MapperFactoryBean;
import org.osgi.framework.BundleContext;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.SingletonBeanRegistry;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.transaction.PlatformTransactionManager;

import com.zaxxer.hikari.HikariDataSource;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.annotations.Tab;
import onight.tfw.ojpa.ordb.ExtendDaoSupper;
import onight.tfw.ojpa.ordb.ORDBDataService;
import onight.tfw.ojpa.ordb.StaticTableDaoSupport;
import onight.tfw.outils.conf.PropHelper;

@Slf4j
public class SpringContextLoader {

	protected HashMap<String, ORDBDataService> daos = new HashMap<>();
	@Getter
	protected ClassPathXmlApplicationContext appContext;
	protected PlatformTransactionManager txManager;

	public SpringContextLoader() {
		log.debug("created");
	}

	public class WrapClassLoader extends ClassLoader {

		public WrapClassLoader(ClassLoader parent) {
			super(parent);
		}

		@Override
		public Class<?> loadClass(String name) throws ClassNotFoundException {
			// log.info("LOadClass::" + name);
			Class<?> clazz = null;
			try {
				clazz = super.loadClass(name);
			} catch (Exception e) {
			}
			if (clazz == null) {
				try {
					clazz = bundleContext.getBundle().loadClass(name);
				} catch (Exception e) {
				}
			}
			if (clazz == null) {
				clazz = loadContext.getBundle().loadClass(name);

			}
			return clazz;
		}

	}

	public Resource[] loadResource(String locationPattern) {
		List<Resource> ret = new ArrayList<Resource>();
		if (locationPattern.startsWith(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX)) {
			int rootstartidx = locationPattern.indexOf(ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX)
					+ ResourcePatternResolver.CLASSPATH_ALL_URL_PREFIX.length();
			int rootendidx = locationPattern.lastIndexOf("/");
			String rootpath = locationPattern.substring(rootstartidx, rootendidx);
			String filePattern = locationPattern.substring(rootendidx + 1);
			Enumeration<URL> en = bundleContext.getBundle().findEntries(rootpath, filePattern, true);
			while (en.hasMoreElements()) {
				URL url = en.nextElement();
				log.debug("find Resource:" + url);
				ret.add(new UrlResource(url));
			}
		}
		return ret.toArray(new Resource[] {});
	}

	BundleContext bundleContext;

	ConcurrentHashMap<String, ConcurrentHashMap<String, Object>> beanMappersByBundle = new ConcurrentHashMap<String, ConcurrentHashMap<String, Object>>();

	String getMapperBeanName(Object mapper, String scope) {
		return scope + "." + StringUtils.uncapitalize(mapper.getClass().getName());
	}

	public void registerMapper(Object mapper, String scope) {
		ConcurrentHashMap<String, Object> beans = beanMappersByBundle.get(scope);
		if (beans == null) {
			beans = new ConcurrentHashMap<String, Object>();
			beanMappersByBundle.put(scope, beans);
		}

		MapperFactoryBean mfb = new MapperFactoryBean(mapper.getClass());
		mfb.setSqlSessionFactory((SqlSessionFactory) appContext.getBean("sqlSessionFactory"));
		mfb.setMapperInterface(mapper.getClass());
		mfb.setAddToConfig(false);

		ConfigurableApplicationContext configContext = (ConfigurableApplicationContext) appContext;
		SingletonBeanRegistry beanRegistry = configContext.getBeanFactory();
		beanRegistry.registerSingleton(getMapperBeanName(mapper, scope), mfb);

		beans.put(getMapperBeanName(mapper, scope), mfb);
		log.info("Registry SQLMapper :" + mapper);
	}

	public void unregisterMapper(Object mapper, String scope) {
		ConcurrentHashMap<String, Object> beans = beanMappersByBundle.get(scope);
		if (beans == null) {
			return;
		}

		Object mfb = beans.remove(getMapperBeanName(mapper, scope));
		if (mfb == null) {
			log.debug("cannot unregister mapper: bean object not found:" + getMapperBeanName(mapper, scope));
			return;
		}

		ConfigurableApplicationContext configContext = (ConfigurableApplicationContext) appContext;
		ConfigurableBeanFactory beanRegistry = configContext.getBeanFactory();
		beanRegistry.destroyBean(scope + "." + StringUtils.uncapitalize(mapper.getClass().getName()), mfb);
		log.info("Registry SQLMapper :" + mapper);
	}

	public String[] toBundleUrls(String[] paths) {
		String[] ret = new String[paths.length];
		int i = 0;
		for (String path : paths) {
			URL url = bundleContext.getBundle().getResource(path);
			// log.info("getURL::" + path + "==>" + url);
			ret[i++] = url.toString();

		}

		return ret;
	}

	BundleContext loadContext;

	public void init(BundleContext bundleContext, String[] contextConfigs, String sql_target) {

		this.bundleContext = bundleContext;

		PropHelper propHelper = new PropHelper(bundleContext);

		HashSet<String> configs = new HashSet<String>();
		HashSet<String> newconfigs = new HashSet<String>();
		newconfigs.addAll(Arrays.asList(contextConfigs));
		if (StringUtils.isBlank(sql_target)) {
			sql_target = "";
		} else {
			sql_target = "." + sql_target;
		}
		for (String config : Arrays
				.asList(new String[] { "/SpringContext-ordb-common.xml", "/SpringContext-ordb-driver.xml", })) {
			if (!configs.contains(config)) {
				configs.add(SpringContextLoader.class.getResource(config).toString());
			}
		}
		// bundle://42.0:18/SpringContext-ordb-common.xml
		String bundleid = SpringContextLoader.class.getResource("/SpringContext-ordb-common.xml").getHost();

		// MT Update ##########
		// loadContext =
		// bundleContext.getBundle(Integer.parseInt(bundleid.split("\\.")[0])).getBundleContext();
		int bid;
		try {
			bid = Integer.parseInt(bundleid.split("\\.")[0]);
		} catch (NumberFormatException e1) {
			bid = Integer.parseInt(bundleid.split("_")[1].split("\\.")[0]);
		}
		loadContext = bundleContext.getBundle(bid).getBundleContext();
		// ##########

		// bundleContext.getBundle()
		for (String config : newconfigs) {
			if (configs.contains(config)) {
				log.debug("override config:" + config);
			}
			URL url = bundleContext.getBundle().getResource(config);
			if (url == null) {
				throw new RuntimeException(" cannot load config file:" + config);
			}
			configs.add(bundleContext.getBundle().getResource(config).toString());
		}
		log.debug("start loading spring mybatis");
		appContext = new ClassPathXmlApplicationContext((configs.toArray(new String[] {}))) {
			protected void initBeanDefinitionReader(XmlBeanDefinitionReader reader) {
				super.initBeanDefinitionReader(reader);
				reader.setValidationMode(XmlBeanDefinitionReader.VALIDATION_NONE);
				reader.setBeanClassLoader(getClassLoader());
			}

			@Override
			public Resource getResource(String location) {
				// TODO Auto-generated method stub
				return super.getResource(location);
			}

			@Override
			public Resource[] getResources(String locationPattern) throws IOException {
				Resource res[] = super.getResources(locationPattern);
				if (res == null || res.length == 0) {
					return loadResource(locationPattern);
				}
				return res;
			}

			@Override
			public ClassLoader getClassLoader() {
				super.setClassLoader(new WrapClassLoader(super.getClassLoader()));
				// log.info("getClassLoader:" + super.getClassLoader());
				return super.getClassLoader();
			}
		};

		// org.mybatis.spring.SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
		// C3PO => Hikari针对mysql连接池, @author lance 2018.12.30
		HikariDataSource ds = (HikariDataSource) appContext.getBean("dataSource");
		ds.setJdbcUrl(propHelper.get("ofw.ordb.url"+sql_target,
				"jdbc:mysql://localhost:3306/msb?autoReconnect=true&amp;useUnicode=true&amp;characterEncoding=utf8"));
		ds.setUsername(propHelper.get("ofw.ordb.usr"+sql_target, "msbao"));
		ds.setPassword(propHelper.get("ofw.ordb.pwd"+sql_target, "msbao"));
		ds.setMaximumPoolSize(propHelper.get("ofw.ordb.maxpool"+sql_target, 100));
		ds.setMinimumIdle(propHelper.get("ofw.ordb.minpool"+sql_target, 10));
		ds.setConnectionTestQuery("SELECT 1");
		// 30000 (30 seconds)
		ds.setConnectionTimeout(30000);
		ds.setPoolName("CSC-ConnectionPool");
		ds.setDriverClassName(propHelper.get("ofw.ordb.driver"+sql_target, "com.mysql.cj.jdbc.Driver"));

		ds.addDataSourceProperty("cachePrepStmts", "true");
		ds.addDataSourceProperty("prepStmtCacheSize", "250");
		ds.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

		log.info("ORDBURL=" + ds.getJdbcUrl());
		String names[] = appContext.getBeanDefinitionNames();
		log.info("total beans:" + names.length + ",springcontext=" + appContext + "@" + this);

		txManager = (PlatformTransactionManager) appContext.getBean("transactionManager");
		log.info("txManager=" + txManager+",target="+sql_target);
	}

	public void registerDaoBeans() {
		Map<String, Object> beans = appContext.getBeansWithAnnotation(Tab.class);

		for (Entry<String, Object> bean : beans.entrySet()) {
			if (bean.getValue() instanceof ExtendDaoSupper) {
				// if(bean.getKey().equals("custdao_usercount"))
				daos.put(bean.getKey(), new ORDBDataService((ExtendDaoSupper) bean.getValue(), txManager));
				log.debug("regist ordb dao bean:" + bean.getKey() + "@" + bean.getValue() + ",tx=" + txManager);
			} else {
				log.debug("unknow dao bean:" + bean.getKey() + "@" + bean.getValue());
			}
		}
	}

	public DomainDaoSupport getBeans(String name) {
		return daos.get(name);
	}

	public Object getSpringBeans(String name) {
		return appContext.getBean(name);
	}

	public StaticTableDaoSupport getStaticDao(String name) {
		if (daos.containsKey(name)) {
			return ((ORDBDataService) daos.get(name)).getDao();
		}
		return null;
	}

	public void destory() {
		log.info("退出：SpringContext:@" + appContext + ":@" + this);
		if (appContext != null) {
			((ConfigurableApplicationContext) appContext).close();
		}

		log.info("退出完成：SpringContext");
	}
}
