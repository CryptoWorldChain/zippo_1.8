package onight.tfw.sm.api;

import onight.tfw.otransio.api.beans.FramePacket;

public interface ISessionManager {

	public SMSession invalidSession(String smid);

	public SMSession getSMSesion(String smid);

	public SMSession getSMSesion(FramePacket pack);
	
	public SMSession updateSession(SMSession session);
	
	
}
