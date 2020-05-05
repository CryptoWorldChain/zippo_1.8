package onight.sm.redis.scala.service

import scala.concurrent.ExecutionContext.Implicits.global

import scala.concurrent.duration.DurationInt
import com.github.mauricio.async.db.RowData
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.sm.Ssm.PBSSO
import onight.sm.Ssm.PBSSORet
import onight.sm.Ssm.RetCode
import onight.sm.redis.scala.LService
import onight.sm.redis.scala.PBUtils
import onight.sm.redis.scala.SessionManager
import onight.sm.redis.scala.SessionModules
import onight.tfw.async.CompleteHandler
import onight.tfw.otransio.api.PacketHelper
import onight.tfw.otransio.api.beans.ExtHeader
import onight.tfw.otransio.api.beans.FramePacket
import onight.sm.Ssm.PBCommand
import onight.sm.redis.scala.PBUtils
import onight.sm.redis.entity.LoginResIDSession
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import org.apache.felix.ipojo.annotations.Instantiate
import onight.tfw.sm.api.SMSession

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class SessionSet extends SessionModules[PBSSO] {
  override def service = SessionSetService
}

//http://localhost:8081/ssm/pbset.do?fh=VSETSSM000000J00&bd={"smid":"VWo1Z0trd0EycmltemU6YWJjL2FuZHJvaWQ5","session":{"kvs":{"a":"bbb"}}}&gcmd=SETSSM

object SessionSetService extends OLog with PBUtils with LService[PBSSO] {

  override def cmd: String = PBCommand.SET.name();
  def onPBPacket(pack: FramePacket, pbo: PBSSO, handler: CompleteHandler) = {
    // ！！检查用户是否已经登录
    val ret = PBSSORet.newBuilder();
    if (pbo == null) {
      ret.setDesc("Packet_Error").setRetcode(RetCode.FAILED);
      handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()));
    } else {
      val newsession = pbBeanUtil.copyFromPB(pbo, new SMSession);
      newsession.setLoginId(null);
      newsession.setUserId(null);
      pbo.getKvsMap.forEach((k, v) => {
        newsession.getKvs.put(k, v);
      })
      val session = SessionManager.checkAndUpdateSession(pbo.getSmid, newsession)

      if (session._1 != null) {
        ret.setRetcode(RetCode.SUCCESS) setLoginId (session._1.getLoginId());
        pbBeanUtil.toPB[PBSSORet](ret, session._1)
        pack.putHeader(ExtHeader.SESSIONID, pbo.getSmid);
      } else {
        //      log.debug("result error: session not found")
        ret.setDesc(session._2).setLoginId(pbo.getLoginId) setRetcode (RetCode.FAILED);
        pack.getExtHead().remove(ExtHeader.SESSIONID)
      }
    }
    handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()));
  }
}