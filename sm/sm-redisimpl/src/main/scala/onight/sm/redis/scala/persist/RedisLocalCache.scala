package onight.sm.redis.scala.persist

import java.util.concurrent.TimeUnit

import com.google.common.cache.Cache
import com.google.common.cache.CacheBuilder

import onight.tfw.mservice.NodeHelper
import onight.tfw.ojpa.api.OJpaDAO
import com.google.protobuf.Message
import onight.tfw.sm.api.SMSession
import onight.sm.redis.entity.TokenEncKeys

abstract class RedisLocalCache[T] {
  val pconfig = NodeHelper.getPropInstance;
  //  val redisLocalCache = new ConcurrentHashMap[String, T](NodeHelper.getPropInstance.get("ssm.vmcache.redismax", 100000))
  val redisLocalCache: Cache[String, T] = CacheBuilder.newBuilder().maximumSize(pconfig.get("ssm.vmcache.redismax", 100000)) //
    .expireAfterWrite(pconfig.get("sm.localcache.timeoutsec", 60), TimeUnit.SECONDS)
    .build().asInstanceOf[Cache[String, T]];

  def getKey(v: T): String
  def dao: OJpaDAO[T]

  def get(v: T): T = {
    val obj = redisLocalCache.getIfPresent(getKey(v));
    if (obj != null) {
      obj
    } else {
      val redisobj = dao.selectByPrimaryKey(v);
      if (redisobj != null) {
        redisLocalCache.put(getKey(v), redisobj)
      }
      redisobj
    }
  }

  def getFromDb(v: T): T = {
    val redisobj = dao.selectByPrimaryKey(v);
    if (redisobj != null)
      redisLocalCache.put(getKey(v), redisobj)
    redisobj
  }
  //这个函数性能很差
  def getAndSet(v: T): T = {
    getKey(v).intern().synchronized({
      redisLocalCache.put(getKey(v), v)
      return dao.getAndSet(v)
    })
  }

  def insert(v: T) = {
    redisLocalCache.put(getKey(v), v)
    dao.insert(v)
  }
  def delete(v: T) = {
    redisLocalCache.invalidate(getKey(v))
    dao.deleteByPrimaryKey(v)
  }
}

object LoginIDRedisLoCache extends RedisLocalCache[SMSession] {
  def getKey(v: SMSession) = v.getUserId+"/"+v.getResId
  def dao: OJpaDAO[SMSession] = {
    RedisDAOs.logiddao;
  }
}

//object SMIDIDRedisLoCache extends RedisLocalCache[SMIDSession] {
//  def getKey(v: SMIDSession) = v.smid
//  val dao: OJpaDAO[SMIDSession] = RedisDAOs.smiddao;
//}

object TokenRedisLoCache extends RedisLocalCache[TokenEncKeys] {
  def getKey(v: TokenEncKeys) = v.getTimeIdx
  def dao: OJpaDAO[TokenEncKeys] = {
    RedisDAOs.tokenDao;
  }
}
