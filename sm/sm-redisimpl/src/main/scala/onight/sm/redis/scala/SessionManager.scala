package onight.sm.redis.scala

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import org.apache.commons.lang3.StringUtils
import onight.oapi.scala.traits.OLog
import onight.sm.redis.scala.persist.LoginIDRedisLoCache
import onight.sm.redis.scala.persist.RedisDAOs
import onight.tfw.mservice.NodeHelper
import onight.tfw.mservice.ThreadContext
import onight.tfw.ojpa.api.JpaContextConstants
import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.HashMap
import onight.sm.redis.entity.LoginResIDSession
import onight.tfw.ojpa.api.KVExample
import onight.tfw.sm.api.SMSession
import onight.osgi.annotation.NActorProvider
import onight.sm.Ssm.PBSSO
import onight.sm.Ssm.PBCommand
import onight.tfw.ntrans.api.ActorService
import onight.tfw.proxy.IActor
import onight.tfw.otransio.api.session.CMDService
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.sm.api.ISessionManager
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.otransio.api.beans.ExtHeader
import onight.tfw.outils.serialize.SessionIDGenerator
import org.fc.zippo.filter.exception.FilterException
import onight.tfw.async.CompleteHandler
import onight.sm.Ssm.PBSSORet
import onight.sm.Ssm.RetCode
import onight.tfw.otransio.api.PacketHelper

@NActorProvider
@Provides(specifications = Array(classOf[ActorService], classOf[IActor], classOf[CMDService]))
class SessionManagerImpl extends SessionModules[PBSSO] with ISessionManager {
  override def service = SessionManager

  override def invalidSession(smid: String): Unit = {
    val session = SessionManager.checkAndUpdateSession(smid)._1
    if (session != null) {
      SessionManager.removeSession(session)
    }
  }
  
  override def updateSession(newsession:SMSession): Unit = {
    newsession.setLoginId(null)
    newsession.setUserId(null);
    newsession.setResId(null);
    newsession.setStatus(null);
    SessionManager.checkAndUpdateSession(newsession.getSmid,newsession)._1
  }


  override def getSMSesion(smid: String): SMSession = {
    SessionManager.checkAndUpdateSession(smid)._1
  }

  override def getSMSesion(pack: FramePacket): SMSession = {
    val smid = pack.getExtStrProp(ExtHeader.SESSIONID);
    if (StringUtils.isBlank(smid) && !SessionIDGenerator.checkSum(smid)) {
      throw new FilterException("9004"); // filter: [smid is not correct]
    }
    if (StringUtils.isBlank(smid)) {
      throw new FilterException("9004"); // filter: [no smid]
    }
    SessionManager.checkAndUpdateSession(smid)._1
  }
}

object SessionManager extends OLog with PBUtils with LService[PBSSO] {
  override def cmd: String = PBCommand.MAN.name();
//  val exec = new ScheduledThreadPoolExecutor(NodeHelper.getPropInstance.get("sm.check.thread", 3));
//  val opexec = new ScheduledThreadPoolExecutor(NodeHelper.getPropInstance.get("sm.op.thread", 100));
  val BATCH_SIZE = NodeHelper.getPropInstance.get("sm.op.batchsize", 1000);
  val runningPool = new ConcurrentHashMap[String, SMSession]();
  val TimeOutSec = NodeHelper.getPropInstance.get("sm.log.timeoutsec", 30 * 60) //默认30分钟超时
  val TimeOutMS = TimeOutSec * 1000 //默认30分钟超时
  val OpDelaySec = NodeHelper.getPropInstance.get("sm.op.delaysec", 10) //默认5秒钟延迟操作redis
  val InsertDelaySec = NodeHelper.getPropInstance.get("sm.insert.delaysec", 2) //默认5秒钟延迟操作redis

  //  val CleanDelaySec = NodeHelper.getPropInstance.get("sm.clean.delaysec", 30) //默认30秒清空redis中的超时缓存

  //  val deleteBox = new LinkedBlockingQueue[LoginResIDSession]();
//  val checkBox = new ConcurrentHashMap[String, SMSession]();
//  val insertBox = new ConcurrentHashMap[String, SMSession]();
  //  val cleanBox = new ConcurrentHashMap[String, LoginResIDSession](); //smid被彻底从redis里面移除

  val NONE_RELATEID = "__";

//  exec.scheduleAtFixedRate(CheckRunner, OpDelaySec, OpDelaySec, TimeUnit.SECONDS)
  def onPBPacket(pack: FramePacket, pbo: PBSSO, handler: CompleteHandler) = {
    val ret = PBSSORet.newBuilder();
    ret.setDesc("Packet_Error").setRetcode(RetCode.FAILED);
    handler.onFinished(PacketHelper.toPBReturn(pack, ret.build()));

  }
  def watchSMID(session: SMSession) = {
    try {
      //      session.setLastUpdateMS(System.currentTimeMillis())
      //检查重复登录
      val exists = LoginIDRedisLoCache.get(session);
      //      val exists = LoginIDRedisLoCache.redisLocalCache.getIfPresent(session.globalID());
      //      if (exists != null) exists.kickout(true)

      if (exists != null) {
        //same session:insert into redis,
        if (!StringUtils.equals(session.getSmid(), exists.getSmid())) {
          log.info("UserSessionRelogin:KickoutOldSession:" + exists.getSmid() + ",newsmid=" + session.getSmid);
          //          rcsession.kickout(true);
          val example = new KVExample();
          example.getCriterias.add(exists.getSmid());
          //        example.setSelectCol("status")
          LoginIDRedisLoCache.dao.deleteByExample(example);
        } else {
          log.debug("sameSessionRelogin")
        }
      }
      val maxTimeOutMS = session.getMaxInactiveInterval() match {
        case f if f > 0 => f * 1000
        case _          => TimeOutMS
      }

      ThreadContext.setContext(JpaContextConstants.Cache_Timeout_Second, maxTimeOutMS / 1000)
      session.setLastUpdateMS(System.currentTimeMillis())
      LoginIDRedisLoCache.insert(session);
      //      insertBox.put(session.globalID(), session)

    } finally {
      //      ThreadContext.cleanContext()
    }
  }

  def checkSMID(session: SMSession): Boolean = {
    val current = System.currentTimeMillis();
    if (StringUtils.equalsAnyIgnoreCase("K", session.getStatus)) { //已经被踢出去的，不检查了
      false
    }
    val rsession = RedisDAOs.logiddao.selectByPrimaryKey(session);

    if (rsession == null) {
      log.info("SessionTimeOut:logid:or:notfoundinRedis:logid:" + session.getLoginId() + ":smid:" + session.getSmid())
      //被踢出来的
      removeSession(session)
      return false
    }
    val maxTimeOutMS = rsession.getMaxInactiveInterval() match {
      case f if f > 0 => f * 1000
      case _          => TimeOutMS
    }

    if (current - rsession.getLastUpdateMS() >= maxTimeOutMS && current - session.getLastUpdateMS() >= maxTimeOutMS) {
      //        RedisDAOs.getSmiddao().deleteByPrimaryKey(session)
      removeSession(session)
      log.info("SessionTimeOut:logid:" + session.getLoginId() + ":userid:" + session.getUserId() + ":lastup:" + session.getLastUpdateMS() + ":logtime:" + session.getLoginMS())
      false
    } else { //redis里面被别的集群节点更新过了
      if (!rsession.getLastUpdateMS().equals(session.getLastUpdateMS())) {
        ThreadContext.setContext(JpaContextConstants.Cache_Timeout_Second, maxTimeOutMS / 1000)
        log.debug("update Session:" + session.getSmid + ":gid:" + session.getUserId + "/" + session.getResId + ",rsession=" + rsession + ",session = " + session)
        val upsession = LoginResIDSession(session, true);
        val rsessionv = RedisDAOs.logiddao.getAndSet(upsession);
        if (StringUtils.equalsAnyIgnoreCase("k", rsessionv.getStatus())) {
          removeSession(session)
          return false;
        }
      }
      true
    }
  }

  def removeSession(session: SMSession) {
    LoginIDRedisLoCache.delete(session)
  }

  abstract class Runner extends Runnable {
    var isRunner = false;
    def doSomething();
    def run() {
      isRunner = true;
      ThreadContext.setContext(JpaContextConstants.Cache_Timeout_Second, TimeOutSec)
      try {
        doSomething()
      } finally {
        isRunner = false;
        ThreadContext.cleanContext()
      }
    }
  }
//  object CheckRunner extends Runner {
//    def doSomething() {
//      var size = checkBox.size()
//      val it = checkBox.values().iterator();
//      while (size > 0 && it.hasNext()) {
//        val obj = it.next();
//        size = size - 1;
//        if (obj != null) {
//          if (checkSMID(obj)) {
//            //            exec.schedule(CheckRunner,
//            //              Math.max(1, TimeOutSec - (System.currentTimeMillis() - obj.getLastUpdateMS()) / 1000), TimeUnit.SECONDS); //timeout的
//          } else {
//            checkBox.remove(obj.getUserId + "/" + obj.getResId)
//          }
//        } else {
//          size = 0;
//        }
//      }
//    }
//  }
  //检查是否登录
  def checkAndUpdateSession(smid: String, newsession: SMSession = null): Tuple2[SMSession, String] = {
    val tkgid = SMIDHelper.fetchUID(smid);
    if (StringUtils.isBlank(tkgid)) {
      return (null, "smid_error_0")
    }
    val searchSession = LoginResIDSession(tkgid)
    if (searchSession == null) {
      return (null, "smid_error_1")
    }
    var session = LoginIDRedisLoCache.get(searchSession);
    if (session != null) {
      if (StringUtils.equalsAnyIgnoreCase("k", session.getStatus())) {
        return (null, "smid_error_2:kickout");
      }
      if (!StringUtils.equals(session.getSmid(), smid)) {
        return (null, "smid_error_3:kickout")
      }

      val maxTimeOutMS = session.getMaxInactiveInterval() match {
        case f if f > 0 => f * 1000
        case _          => TimeOutMS
      }

      if (System.currentTimeMillis() - session.getLastUpdateMS > maxTimeOutMS) {
        removeSession(session);
        return (null, "session_timeout");
      }
      if (newsession != null) {
        //        if (!StringUtils.equals(session.smid, newsession.smid) ||
        //          !StringUtils.equals(session.loginId, newsession.loginId) ||
        //          !StringUtils.equals(session.resId, newsession.resId)) {
        //          return (null, "smid_error_3:not_the_same_session");
        //        }
        session.getKvs.putAll(newsession.getKvs);
        LoginIDRedisLoCache.redisLocalCache.put(session.getUserId + "/" + session.getResId, session)
      }

      ThreadContext.setContext(JpaContextConstants.Cache_Timeout_Second, maxTimeOutMS / 1000)

      session.setLastUpdateMS(System.currentTimeMillis());

      val rsessionv = RedisDAOs.logiddao.getAndSet(session);
      if (StringUtils.equalsAnyIgnoreCase("k", rsessionv.getStatus())) {
        removeSession(session)
        return (null, "session_kickout");
      }

      //      checkBox.put(session.globalID, session)

      //      opexec.schedule(CheckRunner, Math.min(OpDelaySec, Math.max(1, (TimeOutMS - (System.currentTimeMillis() - lastup)) / 100)), TimeUnit.SECONDS); //timeout的
      return (session, "OK");
    }
    (null, "not_login")
  }

  // 登出
  def logout(smid: String, loginId: String, resId: String): Tuple2[SMSession, String] = {
    val tkgid = SMIDHelper.fetchUID(smid);
    val searchSession = LoginResIDSession(loginId, resId);
    if (!StringUtils.equals(searchSession.getUserId + "/" + searchSession.getResId, tkgid)) {
      return (null, "smid_error_1")
    }
    var session = LoginIDRedisLoCache.get(searchSession);
    if (session != null) {
      if (StringUtils.equalsAnyIgnoreCase("k", session.getStatus())) {
        return (null, "smid_error_2")
      }
      if (!StringUtils.equals(session.getSmid(), smid)) {
        session = LoginIDRedisLoCache.getFromDb(searchSession)
        if (!StringUtils.equals(session.getSmid(), smid)) {
          return (null, "smid_error_3")
        }
      }

      if (System.currentTimeMillis() - session.getLastUpdateMS > TimeOutMS) {
        removeSession(session);
        return (null, "session_timeout");
      }
      removeSession(session)
      return (session, "OK");
    }
    (null, "not_login")

  }
}