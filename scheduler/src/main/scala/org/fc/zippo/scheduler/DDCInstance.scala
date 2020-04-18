package org.fc.zippo.scheduler

import org.apache.felix.ipojo.annotations.Invalidate
import org.apache.felix.ipojo.annotations.Validate
import org.fc.zippo.scheduler.pbgens.Schedule.PModule

import com.google.protobuf.Message

import onight.oapi.scala.commons.SessionModules
import onight.oapi.scala.traits.OLog
import onight.osgi.annotation.NActorProvider
import onight.tfw.otransio.api.beans.FramePacket
import onight.tfw.async.CompleteHandler
import org.apache.felix.ipojo.annotations.Component
import onight.osgi.annotation.iPojoBean
import org.apache.felix.ipojo.annotations.Instantiate
import org.apache.felix.ipojo.annotations.Provides
import onight.tfw.ntrans.api.ActorService
import org.fc.zippo.dispatcher.IActorDispatcher
import onight.tfw.ntrans.api.PBActor
import onight.tfw.async.ActorRunner
import onight.tfw.async.CallBack
import java.util.concurrent.ExecutorService
import org.fc.zippo.dispatcher.TimeLimitRunner
import onight.tfw.outils.conf.PropHelper
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.TimeUnit
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.BlockingQueue
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ForkJoinWorkerThread
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory
import java.lang.Thread.UncaughtExceptionHandler
import onight.tfw.outils.pool.ReusefulLoopPool
import java.util.concurrent.ThreadFactory

object DDCInstance extends OLog {
  val prop = new PropHelper(null);

  val daemonsWTC = new ScheduledThreadPoolExecutor(DDCConfig.DAEMON_WORKER_THREAD_COUNT);

  val timeoutSCH = new ScheduledThreadPoolExecutor(DDCConfig.TIMEOUT_CHECK_THREAD_COUNT);
  val defaultQ = new LinkedBlockingDeque[Worker]
  val specQDW = new ConcurrentHashMap[String, (LinkedBlockingDeque[Worker], Iterable[DDCDispatcher], ExecutorService)];
  val specQ = new ConcurrentHashMap[String, (String, LinkedBlockingDeque[Worker])];

  val running = new AtomicBoolean(true);

  class WrapperThread(pool: ReusefulLoopPool[Thread]) extends Thread {
    override def run() {

      try {
        super.run();
      } finally {
        Thread.currentThread().setName("BWT-POOL")
        if (pool.size() < DDCConfig.RUNTIME_MAX_THREAD_COUNT) {
          pool.retobj(this);
        }
      }
    }
  }
  class ExitRejectedExecutionHandler extends RejectedExecutionHandler {
    def rejectedExecution(r: Runnable, executor: ThreadPoolExecutor) {
      log.error("rejectedExecution in create thread:" + executor);
      System.exit(-1)
    }
  }
  class FailToExitForkJoinWorkerThread(pool: ForkJoinPool, runtime_fjthread_pool: ReusefulLoopPool[FailToExitForkJoinWorkerThread], putTOPool: Boolean) extends ForkJoinWorkerThread(pool) {
    override def onStart() {
      Thread.currentThread().setName("F2E-JoinWorker-" + putTOPool)
      super.onStart();
    }

    override def start() {
      try {
        super.start()
      } catch {
        case oom: java.lang.OutOfMemoryError =>
          log.error("Out of memory Error", oom);
          System.exit(-1)
        case t: Throwable =>
          log.error("Thread start Error", t);
          System.exit(-1)
      }

    }
    override def onTermination(exception: Throwable) {
      if (putTOPool) {
        runtime_fjthread_pool.retobj(this)
      }
      Thread.currentThread().setName("F2E-JoinWorker-" + putTOPool + ".end")
      super.onTermination(exception);
    }
  }
  val runtime_fjthread_pool = new ReusefulLoopPool[FailToExitForkJoinWorkerThread]();
  class FailToExistForkJoinWorkerThreadFactory(totalThreadSize: Int) extends ForkJoinWorkerThreadFactory {
    def newThread(pool: ForkJoinPool): ForkJoinWorkerThread = {
      try {
        if (totalThreadSize <= 0) {
          new FailToExitForkJoinWorkerThread(pool, null, false);
        } else {
          var t = runtime_fjthread_pool.borrow();
          var cc = 0;
          while (t == null && cc < DDCConfig.TRY_POOL_THREAD_MAX_COUNT) {
            t = runtime_fjthread_pool.borrow();
            cc = cc + 1;
          }
          if (t == null) {
            new FailToExitForkJoinWorkerThread(pool, runtime_fjthread_pool, runtime_fjthread_pool.size() < totalThreadSize);
          } else {
            t
          }
        }
      } catch {
        case t: Throwable =>
          log.error("error in create fork join thread:", t);
          System.exit(-1)
          null
      } finally {
      }
    }
  }

  val runtime_thread_pool = new ReusefulLoopPool[Thread]();

  val factory = new ThreadFactory {
    def newThread(r: Runnable): Thread = {
      var t = runtime_thread_pool.borrow();
      var cc = 0
      while (t == null && cc < DDCConfig.TRY_POOL_THREAD_MAX_COUNT) {
        t = runtime_thread_pool.borrow();
        cc = cc + 1;
      }
      if (t != null) {
        t
      } else {
        try {
          new Thread()
        } catch {
          case t: Throwable =>
            log.error("error in create thread:", t);
            System.exit(-1)
            null
        }
      }
    }
  }

  val defaultWTC: ExecutorService = if ("FIX".equalsIgnoreCase(DDCConfig.THREAD_POOL)) {
    for (i <- 1 to DDCConfig.DEFAULT_WORKER_THREAD_COUNT if runtime_thread_pool.size() < DDCConfig.RUNTIME_MAX_THREAD_COUNT) {
      runtime_thread_pool.addObject(new WrapperThread(runtime_thread_pool))
    }
    new ThreadPoolExecutor(
      Math.max(1, Runtime.getRuntime.availableProcessors() / 2),
      DDCConfig.DEFAULT_WORKER_THREAD_COUNT, 60, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](DDCConfig.DEFAULT_WORKER_THREAD_COUNT * 2), factory, new ExitRejectedExecutionHandler());
  } else {
    new ForkJoinPool(DDCConfig.DEFAULT_WORKER_THREAD_COUNT, new FailToExistForkJoinWorkerThreadFactory(DDCConfig.RUNTIME_MAX_THREAD_COUNT), null, false);
  }

  @Validate
  def init(): Unit = {
    running.set(true)

    log.info("DDC-Startup: defaultWTC=" + DDCConfig.DEFAULT_WORKER_THREAD_COUNT + ",daemonsWTC=" + DDCConfig.DAEMON_WORKER_THREAD_COUNT);

    //    val runtime_thread_pool = new ReusefulLoopPool[Thread]();

    for (i <- 1 to DDCConfig.DEFAULT_DISPATCHER_COUNT) {
      new Thread(new DDCDispatcher("default(" + i + ")", defaultQ, defaultWTC)).start()
    }

    //init specify dispatcher -- thread pools
    DDCConfig.specDispatchers().map { x =>
      val dcname = x._1
      val ddc = x._2;
      val wc = x._3;
      val newQ = new LinkedBlockingDeque[Worker]
      val newWTC = DDCConfig.THREAD_POOL match {
        case "FIX" =>
          log.info("init fix thread pool");
          for (i <- 1 to wc if runtime_thread_pool.size() < DDCConfig.RUNTIME_MAX_THREAD_COUNT) {
            runtime_thread_pool.addObject(new WrapperThread(runtime_thread_pool))
          }

          new ThreadPoolExecutor(
            Math.max(1, Runtime.getRuntime.availableProcessors() / 2),
            wc, 60, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](wc * 2), factory, new ExitRejectedExecutionHandler())
        case _ =>
          new ForkJoinPool(wc, new FailToExistForkJoinWorkerThreadFactory(DDCConfig.RUNTIME_MAX_THREAD_COUNT), null, false);
      }

      val incr = new AtomicInteger(0);
      val newDP = Array.fill(ddc)(0).map { x => new DDCDispatcher(dcname + "(" + incr.incrementAndGet() + ")", newQ, newWTC); }
      newDP.map { f => new Thread(f).start() }
      specQDW.put(dcname, (newQ, newDP, newWTC))
    }

    DDCConfig.spectActors().map { x =>
      val gcmd = x._1;
      val dcname = x._2;
      val dqw = specQDW.get(dcname)
      if (dqw != null) {
        specQ.put(gcmd, (dcname, dqw._1))
      }
    }
    log.info("DDC-Startup[OK]: defaultWTC=" + DDCConfig.DEFAULT_WORKER_THREAD_COUNT + ",daemonsWTC=" + DDCConfig.DAEMON_WORKER_THREAD_COUNT);
  }
  @Invalidate
  def destroy() {
    running.set(false)
    daemonsWTC.shutdown();
    defaultWTC.shutdown()
    val nullWorker = new SessionModules[Message] {
      override def onPBPacket(pack: FramePacket, pbo: Message, handler: CompleteHandler) = {
      }
    };
    defaultQ.offer(WorkerObjectPool.borrow("quit", null, null, nullWorker, "default"))
    val ele = specQDW.elements();
    while (ele.hasMoreElements()) {
      val f = ele.nextElement()
      val q = f._1;
      val d = f._2;
      val w = f._3;
      q.offer(WorkerObjectPool.borrow("quit", null, null, nullWorker, "default"));
      d.map { x => x.running.set(false) }
      w.shutdown();
    }
  }
  /**
   * run in seconds at fix delays
   */
  def scheduleWithFixedDelaySecond(run: Runnable, initialDelay: Long, period: Long) = {
    daemonsWTC.scheduleWithFixedDelay(run, initialDelay, period, TimeUnit.SECONDS)
  }
  def scheduleWithFixedDelay(run: Runnable, initialDelay: Long, period: Long, tu: TimeUnit) = {
    daemonsWTC.scheduleWithFixedDelay(run, initialDelay, period, tu)
  }
  def post(pack: FramePacket, handler: CompleteHandler, sm: PBActor[Message]) = {
    val (dname, q) = if (specQ.containsKey(pack.getModuleAndCMD)) specQ.get(pack.getModuleAndCMD)
    else {
      ("default", defaultQ)
    }
    if (q.size() < DDCConfig.DEFAULT_WORKER_QUEUE_MAXSIZE) {
      q.offer(WorkerObjectPool.borrow(pack.getModuleAndCMD, pack, handler, sm, dname));
    } else {
      log.error("drop actor exec for pool size exceed:" + q.size() + "==>" + DDCConfig.DEFAULT_WORKER_QUEUE_MAXSIZE);
    }
  }

  def post(pack: FramePacket, runner: Runnable) = {
    val (dname, q) = if (specQ.containsKey(pack.getModuleAndCMD)) specQ.get(pack.getModuleAndCMD)
    else {
      ("default", defaultQ)
    }
    if (q.size() < DDCConfig.DEFAULT_WORKER_QUEUE_MAXSIZE) {
      q.offer(WorkerObjectPool.borrow(pack.getModuleAndCMD, runner, dname));
    } else {
      log.error("drop actor exec for pool size exceed:" + q.size() + "==>" + DDCConfig.DEFAULT_WORKER_QUEUE_MAXSIZE);
    }
  }
  def executeNow(pack: FramePacket, handler: CompleteHandler, sm: PBActor[Message]): Unit = {
    val (dname, q) = if (specQ.containsKey(pack.getModuleAndCMD)) specQ.get(pack.getModuleAndCMD)
    else {
      ("default", defaultQ)
    }
    q.addFirst(WorkerObjectPool.borrow(pack.getModuleAndCMD, pack, handler, sm, dname));

  }
  def executeNow(pack: FramePacket, runner: Runnable): Unit = {
    val (dname, q) = if (specQ.containsKey(pack.getModuleAndCMD)) specQ.get(pack.getModuleAndCMD)
    else {
      ("default", defaultQ)
    }
    q.addFirst(WorkerObjectPool.borrow(pack.getModuleAndCMD, runner, dname));
  }

  def postWithTimeout(pack: FramePacket, handler: CompleteHandler, sm: PBActor[Message], timeoutMS: Long): Unit = {
    val runner = new TimeLimitRunner(timeoutSCH, timeoutMS, handler) {
      @Override
      def runOnce() = {
        sm.onPacket(pack, handler);
      }
    };
    post(pack, handler, sm);
  }

  def postWithTimeout(pack: FramePacket, runner: Runnable, timeoutMS: Long, handler: CompleteHandler): Unit = {
    val tlrunner = new TimeLimitRunner(timeoutSCH, timeoutMS, handler) {
      def runOnce() = {
        runner.run();
      }
    };
    post(pack, tlrunner);
  }

  def executeNowWithTimeout(pack: FramePacket, handler: CompleteHandler, sm: PBActor[Message], timeoutMS: Long): Unit = {
    val runner = new TimeLimitRunner(timeoutSCH, timeoutMS, handler) {
      @Override
      def runOnce() = {
        sm.onPacket(pack, handler);
      }
    };
    executeNow(pack, handler, sm);
  }

  def executeNowWithTimeout(pack: FramePacket, runner: Runnable, timeoutMS: Long, handler: CompleteHandler): Unit = {
    val tlrunner = new TimeLimitRunner(timeoutSCH, timeoutMS, handler) {
      def runOnce() = {
        runner.run();
      }
    };
    executeNow(pack, tlrunner);
  }

  def getExecutorService(poolname: String): ExecutorService = {
    val v = specQDW.get(poolname)
    if (v != null) {
      v._3;
    } else {
      defaultWTC
    }
  }
  def getExecutorServiceOrDefault(poolname: String, defaultpool: String): ExecutorService = {
    val v = specQDW.get(poolname)
    if (v != null) {
      v._3;
    } else {
      getExecutorService(defaultpool)
    }
  }
}