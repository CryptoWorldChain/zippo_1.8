package onight.osgi.otransio.nio;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.glassfish.grizzly.Connection;
import org.glassfish.grizzly.filterchain.FilterChainBuilder;
import org.glassfish.grizzly.filterchain.TransportFilter;
import org.glassfish.grizzly.nio.transport.TCPNIOTransport;
import org.glassfish.grizzly.nio.transport.TCPNIOTransportBuilder;
import org.glassfish.grizzly.threadpool.ThreadPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import onight.osgi.otransio.impl.OSocketImpl;
import onight.osgi.otransio.sm.OutgoingSessionManager;
import onight.tfw.outils.conf.PropHelper;

public class OClient {
	Logger log = LoggerFactory.getLogger(OClient.class);

	public TCPNIOTransport transport;
	OSocketImpl oimpl;

	public OClient(OSocketImpl oimpl) {
		// init();
		this.oimpl = oimpl;
	}
	int connect_timeout_sec = 10;
	public void init(OutgoingSessionManager sm, PropHelper params) {
		FilterChainBuilder filterChainBuilder = FilterChainBuilder.stateless();
		filterChainBuilder.add(new TransportFilter());
		filterChainBuilder.add(new OTransFilter());
		SessionFilter sf = new SessionFilter(oimpl, params);
		filterChainBuilder.add(sf);

		// Create TCP transport
		transport = TCPNIOTransportBuilder.newInstance().build();
		transport.setProcessor(filterChainBuilder.build());
		ThreadPoolConfig ktpc = ThreadPoolConfig.defaultConfig();
		ktpc.setCorePoolSize(params.get("otransio.ckernel.core", 10))
				.setMaxPoolSize(params.get("otransio.ckernel.max", 100));
		transport.setKernelThreadPoolConfig(ktpc);

		connect_timeout_sec = params.get("otransio.client.connect.timeout.sec", 60);
		ThreadPoolConfig wtpc = ThreadPoolConfig.defaultConfig();
		wtpc.setCorePoolSize(params.get("otransio.cworker.core", 10))
				.setMaxPoolSize(params.get("otransio.cworker.max", 100));
		transport.setWorkerThreadPoolConfig(wtpc);
		transport.setKeepAlive(true);
		transport = TCPNIOTransportBuilder.newInstance().build();
		transport.setProcessor(filterChainBuilder.build());
		transport.setClientSocketSoTimeout(params.get("otransio.client.sotimeout", 10));
		transport.setKernelThreadPool(
				oimpl.getDispatcher().getExecutorServiceOrDefault("otransio.client.kernel", "otransio"));
		transport.setWorkerThreadPool(
				oimpl.getDispatcher().getExecutorServiceOrDefault("otransio.client.cworkers", "otransio"));

		transport.setTcpNoDelay(true);
		try {
			transport.start();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public Connection<?> getConnection(String address, int port)
			throws InterruptedException, ExecutionException, TimeoutException {
		Connection<?> connect;
		long start=System.currentTimeMillis();
		try {
			
			connect = transport.connect(address, port).get(connect_timeout_sec, TimeUnit.SECONDS);
			if(System.currentTimeMillis()-start>10*1000) {
				log.info("connect time too long:"+(System.currentTimeMillis()-start)+",to="+address+",port="+port);
			}
			return connect;
		} catch (Exception e) {
			log.info("get connect Exception:cost="+(System.currentTimeMillis()-start)+",address="+address+",port="+port,e);
			throw e;
		}

	}

	public void stop() {
		try {
			transport.shutdownNow();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			log.info("socket服务关闭");
		}
	}

	public static void main(String[] args) {
		CountDownLatch cdl = new CountDownLatch(1);
		boolean end = false;
		long start = System.currentTimeMillis();
		try {
			end = cdl.await(10, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("end" + end);
	}

}
