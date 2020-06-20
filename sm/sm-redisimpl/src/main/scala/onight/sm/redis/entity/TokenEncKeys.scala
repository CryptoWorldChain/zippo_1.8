package onight.sm.redis.entity

import scala.beans.BeanProperty

import org.apache.commons.lang3.StringUtils
import onight.tfw.sm.api.SMSession
import java.util.Map

//import onight.tfw.sm.api.SMSession

//class LoginResIDSession extends SMSession{
//  @BeanProperty var password: String = null
//  @BeanProperty var resId: String = null
//  @BeanProperty var bc_address: String = null;
//  @BeanProperty var bc_pri: String = null;
//  @BeanProperty var kvs = new HashMap[String, Object]();
//
//  def globalID(): String = { loginId + "/" + resId }
//  def isKickout(): Boolean = {
//    status != null && StringUtils.equals(status, "K")
//  }
//  def kickout(kick: Boolean) = {
//    if (kick) status = "K";
//    else status = "N"
//  }
//}
//
////class LoginResIDSession extends SMIDSession {
////}
object LoginResIDSession {
  //  def apply(session: LoginResIDSession) = {
  //    val ret = new LoginResIDSession();
  //    ret.smid = session.smid
  //    ret.userId = session.userId
  //    ret.loginId = session.loginId
  //    ret.password = session.password
  //    ret.resId = session.resId
  //    ret.ext = session.ext;
  //    ret
  //  }
  def apply(session: SMSession, update: Boolean): SMSession = {
    val ret = new SMSession();
    ret.setLoginId(session.getLoginId)
    ret.setResId(session.getResId)
    ret.setLastUpdateMS(session.getLastUpdateMS);
    ret.getKvs.clear();
    ret.getKvs.putAll(session.getKvs);
    ret.setStatus(session.getStatus);
    ret
  }
  def apply(login_res: String): SMSession = {
    val arrs = login_res.split("/");
    if (arrs.length > 2) {
      null;
    } else {
      val ret = new SMSession()
      ret.setUserId(arrs(0))
      if (arrs.length == 1) {
        ret.setResId("");
      } else {

        ret.setResId(arrs(1))
      }
      ret
    }
  }

  def apply(loginId: String, resId: String): SMSession = {
    val ret = new SMSession();
    ret.setLoginId(loginId)
    ret.setUserId(loginId)
    ret.setResId(resId)
    ret
  }
  def apply(smid: String, userId: String, loginId: String, password: String, resId: String, kvs: Map[String, String]): SMSession = {
    val ret = new SMSession();
    ret.setSmid(smid)
    ret.setUserId(userId)
    ret.setLoginId(loginId)
    //    ret. = password
    ret.setResId(resId)
    if (kvs != null) {
      ret.getKvs.putAll(kvs)
    }
    ret
  }
}
//object SMIDSession {
//  def apply(smid: String) = {
//    val ret = new SMIDSession();
//    ret.smid = smid
//    ret
//  }
//  def apply(smid: String, userId: String, loginId: String, password: String, resId: String, ext: String) = {
//    val ret = new SMIDSession();
//    ret.smid = smid
//    ret.userId = userId
//    ret.loginId = loginId
//    ret.password = password
//    ret.resId = resId
//    ret.ext = ext;
//    ret
//  }
//}

/*
 * 保存token的地方
 */
class TokenEncKeys {
  @BeanProperty var timeIdx: String = null;
  @BeanProperty var enckey: String = null;
  @BeanProperty var gentime: Long = System.currentTimeMillis();
}
object TokenEncKeys {
  def apply(timeIdx: String) = {
    val ret = new TokenEncKeys();
    ret.timeIdx = timeIdx;
    ret
  }
}
  