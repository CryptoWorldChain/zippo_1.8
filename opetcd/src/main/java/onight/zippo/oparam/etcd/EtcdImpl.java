package onight.zippo.oparam.etcd;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import mousio.client.promises.ResponsePromise;
import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.promises.EtcdResponsePromise;
import mousio.etcd4j.requests.EtcdKeyGetRequest;
import mousio.etcd4j.responses.EtcdKeysResponse;
import onight.tfw.async.CallBack;
import onight.tfw.mservice.ThreadContext;
import onight.tfw.ojpa.api.DomainDaoSupport;
import onight.tfw.ojpa.api.ServiceSpec;
import onight.tfw.oparam.api.OPFace;
import onight.tfw.oparam.api.OTreeValue;
import onight.tfw.outils.serialize.JsonSerializer;

@Slf4j
public class EtcdImpl implements OPFace, DomainDaoSupport {
	EtcdClient etcd;

	@Setter
	@Getter
	int default_ttl = 99999999;

	public EtcdClient getEtcd() {
		return etcd;
	}

	public void setEtcd(EtcdClient etcd) {
		this.etcd = etcd;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#getHealth()
	 */
	@Override
	public String getHealth() {
		Object obj = ThreadContext.getContext("iscluster");
		if (obj != null && obj instanceof Boolean) {
			if ((Boolean) obj) {
				return JsonSerializer.formatToString(etcd.getMembers().getMembers());
			}
		}
		return etcd.getHealth().getHealth();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#put(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public Future<OTreeValue> put(String key, String value) throws IOException {
		return new FutureWP(etcd.put(key, value).timeout(ThreadContext.getContextInt("wait.timeout", 60), TimeUnit.SECONDS).ttl(ThreadContext.getContextInt("ttl", default_ttl)).send());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#putDir(java.lang.String)
	 */
	@Override
	public Future<OTreeValue> putDir(String dir) throws IOException {
		return new FutureWP(etcd.putDir(dir).timeout(ThreadContext.getContextInt("wait.timeout", 60), TimeUnit.SECONDS).ttl(ThreadContext.getContextInt("ttl", default_ttl)).send());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#post(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public Future<OTreeValue> post(String key, String value) throws IOException {
		return new FutureWP(etcd.post(key, value).timeout(ThreadContext.getContextInt("wait.timeout", 60), TimeUnit.SECONDS).ttl(ThreadContext.getContextInt("ttl", default_ttl)).send());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#delete(java.lang.String)
	 */
	@Override
	public Future<OTreeValue> delete(String key) throws IOException {
		// TODO Auto-generated method stub
		return new FutureWP(etcd.delete(key).timeout(ThreadContext.getContextInt("wait.timeout", 60), TimeUnit.SECONDS).recursive().send());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#deleteDir(java.lang.String)
	 */
	@Override
	public Future<OTreeValue> deleteDir(String dir) throws IOException {
		return new FutureWP(etcd.deleteDir(dir).recursive().send());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#get(java.lang.String)
	 */
	@Override
	public Future<OTreeValue> get(String key) throws IOException {
		if (ThreadContext.getContextInt("sorted", 0) == 1) {
			return new FutureWP(etcd.get(key).timeout(ThreadContext.getContextInt("wait.timeout", 60), TimeUnit.SECONDS).recursive().sorted().send());
		}
		return new FutureWP(etcd.get(key).recursive().send());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#getDir(java.lang.String)
	 */
	@Override
	public Future<OTreeValue> getDir(String dir) throws IOException {
		return new FutureWP(etcd.getDir(dir).recursive().send());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#getAll()
	 */
	@Override
	public Future<OTreeValue> getAll() throws IOException {
		return new FutureWP(etcd.getAll().recursive().send());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#watchOnce(java.lang.String,
	 * onight.tfw.async.CallBack)
	 */
	@Override
	public void watchOnce(final String key, final CallBack<OTreeValue> cb) {
		watch(key, cb, false);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see onight.zippo.oparam.etcd.OPFace#watch(java.lang.String,
	 * onight.tfw.async.CallBack, boolean)
	 */
	@Override
	public void watch(final String key, final CallBack<OTreeValue> cb, final boolean always) {

		EtcdKeyGetRequest getRequest = etcd.getDir(key).recursive().waitForChange()
				.timeout(ThreadContext.getContextInt("wait.timeout", 60), TimeUnit.SECONDS);
		try {

			EtcdResponsePromise<EtcdKeysResponse> promise = getRequest.send();
			promise.addListener(new ResponsePromise.IsSimplePromiseResponseHandler<EtcdKeysResponse>() {
				@Override
				public void onResponse(ResponsePromise<EtcdKeysResponse> response) {
					try {
						log.trace("onResponse@" + this + ",response=" + response);
						cb.onSuccess(new OTreeValue(response.get().getNode().key, response.get().getNode().value,
								FutureWP.getTrees(response.get().getNode().nodes),response.get().getNode().modifiedIndex,response.get().getNode().createdIndex));
					} catch (TimeoutException te) {
						// log.debug("Etcd Watch Timeout:" + key+",@"+this);
						cb.onFailed(te, new OTreeValue(key, null, null,0,0));
					} catch (Exception e) {
						cb.onFailed(e, new OTreeValue(key, null, null,0,0));
					} finally {
						if (always) {
							// still watch
							EtcdImpl.this.watch(key, cb, always);
						}
					}
				}

			});
		} catch (Exception e) {
			cb.onFailed(e, new OTreeValue(key, null, null,0,0));
		}
	}

	@Override
	public DomainDaoSupport getDaosupport() {
		return this;
	}

	@Override
	public Class<?> getDomainClazz() {
		return Object.class;
	}

	@Override
	public String getDomainName() {
		return "etcd";
	}

	@Override
	public ServiceSpec getServiceSpec() {
		return ServiceSpec.ETCD_STORE;
	}

	@Override
	public void setDaosupport(DomainDaoSupport dao) {
		log.trace("setDaosupport::dao=" + dao);
	}

	@Override
	public Future<OTreeValue> compareAndDelete(String key, String value) throws IOException {
		return new FutureWP(etcd.delete(key).timeout(ThreadContext.getContextInt("wait.timeout", 60), TimeUnit.SECONDS).prevValue(value).send());
	}

	@Override
	public Future<OTreeValue> compareAndSwap(String key, String newvalue, String comparevalue) throws IOException {
		if (comparevalue == null) {
			return new FutureWP(etcd.put(key, newvalue).timeout(ThreadContext.getContextInt("wait.timeout", 60), TimeUnit.SECONDS).prevExist(false)
					.ttl(ThreadContext.getContextInt("ttl", default_ttl)).send());
		}
		return new FutureWP(etcd.put(key, newvalue).timeout(ThreadContext.getContextInt("wait.timeout", 60), TimeUnit.SECONDS).prevValue(comparevalue)
				.ttl(ThreadContext.getContextInt("ttl", default_ttl)).send());
	}

}
