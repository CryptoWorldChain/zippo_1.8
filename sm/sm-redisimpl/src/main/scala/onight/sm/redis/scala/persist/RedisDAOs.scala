package onight.sm.redis.scala.persist

import scala.beans.BeanProperty

import com.google.protobuf.Message

import onight.osgi.annotation.NActorProvider
import onight.sm.redis.scala.SessionModules
import onight.tfw.ojpa.api.OJpaDAO
import onight.tfw.ojpa.api.annotations.StoreDAO
import onight.tfw.sm.api.SMSession
import onight.sm.redis.entity.TokenEncKeys

@NActorProvider
object RedisDAOs extends SessionModules[Message] {
//  @BeanProperty
//  @StoreDAO(domain = classOf[SMIDSession], key = "smid", target = "redis")
//  var smiddao: OJpaDAO[SMIDSession] = null
 
  @StoreDAO(domain = classOf[SMSession], key = "userId,resId", target = "redis")
//  @BeanProperty
  var logiddao: OJpaDAO[SMSession] = null

  @StoreDAO(domain = classOf[TokenEncKeys], key = "timeIdx", target = "redis")
//  @BeanProperty
  var tokenDao: OJpaDAO[TokenEncKeys] = null
  
  def setLogiddao(daodb: OJpaDAO[SMSession]) {
    log.debug("set Login dao"+daodb);
      logiddao = daodb
  }
  def getLogiddao:OJpaDAO[SMSession] = {
    logiddao
  }
  
  def setTokenDao(daodb: OJpaDAO[TokenEncKeys]) {
    log.debug("set token dao:"+daodb);
      tokenDao = daodb
  }
  def getTokenDao:OJpaDAO[TokenEncKeys] = {
    tokenDao
  }
  
//  def newSMID(userId: String, loginId: String, password: String, resourceid: String, ext: String) {
//    val smid = UUIDGenerator.generate() + "_" + loginId;
//    HashCode.fromString(smid)
//    val logsession = SMIDSession(smid, userId, loginId, password, resourceid, ext)
//  }

}

