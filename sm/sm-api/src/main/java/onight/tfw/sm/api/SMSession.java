package onight.tfw.sm.api;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Data;

@Data
public class SMSession {
	String smid;
	long loginMS;
	long lastUpdateMS;
	long maxInactiveInterval;
	boolean validate  = true;
	Map<String, String> kvs = new ConcurrentHashMap<String, String>();
	String loginId;
	String resId; // 资源id
	String userId;
	String status;

	public SMSession() {
	}

}
