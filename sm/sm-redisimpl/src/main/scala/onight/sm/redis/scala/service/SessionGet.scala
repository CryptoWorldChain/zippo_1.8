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
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService

@NActorProvider
@Instantiate
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class SessionGet extends SessionModules[PBSSO] {
  override def service = SessionGetService
}

//http://localhost:8081/ssm/pbget.do?fh=VGETSSM000000J00&bd={%22smid%22:%22VWo1Z0trd0EycmltemU6YWJjL2FuZHJvaWQ5%22}&gcmd=GETSSM

object SessionGetService extends OLog with PBUtils with LService[PBSSO] {

  override def cmd: String = PBCommand.GET.name();
  def onPBPacket(pack: FramePacket, pbo: PBSSO, handler: CompleteHandler) = {
    // ！！检查用户是否已经登录
    val ret = PBSSORet.newBuilder();
    if (pbo == null) {
      ret.setDesc("Packet_Error").setRetcode (RetCode.FAILED);
    } else {
      val session = SessionManager.checkAndUpdateSession(pbo.getSmid)
      if (session._1 != null) {
        ret.setRetcode(RetCode.SUCCESS)
        pbBeanUtil.toPB[PBSSORet](ret, session._1)
//        pbBeanUtil.copyFromPB(ret, session._1)
//        ret.mergeFrom(session._1);
        pack.putHeader(ExtHeader.SESSIONID, pbo.getSmid);
      } else {
        //      log.debug("result error: session not found")
        ret.setDesc(session._2).setLoginId(pbo.getLoginId) setRetcode (RetCode.FAILED);
      }
    }
    handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()));
  }
}