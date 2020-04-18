package org.fc.zippo.scheduler

import java.util.concurrent.ForkJoinPool
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

import onight.oapi.scala.traits.OLog
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.LinkedBlockingDeque
import java.util.concurrent.ExecutorService
import java.util.concurrent.ThreadPoolExecutor

case class DDCDispatcher(name: String, q: LinkedBlockingDeque[Worker], threadPool: ExecutorService, running: AtomicBoolean = new AtomicBoolean(true)) extends Runnable with OLog {

  def run() {
    Thread.currentThread().setName("DDC-Dispatcher-" + name)
    var lastlogTime: Long = 0;

    while (running.get) {
      try {
        if (System.currentTimeMillis() - lastlogTime > DDCConfig.LOGINFO_DISPATCHER_TIMESEC * 1000) {
          if (threadPool.isInstanceOf[ForkJoinPool]) {
            val fjth = threadPool.asInstanceOf[ForkJoinPool];
            log.error("DDC-Dispatcher:" + name + ",tp[A=" + fjth.getActiveThreadCount + ",Q=" + fjth.getQueuedTaskCount + ",C=" + fjth.getPoolSize
              + ",M=" + fjth.getParallelism
              + ",S=" + fjth.getStealCount + ",F=" + fjth.getRunningThreadCount
              + "],defQ.size=" + q.size());
          }
          else if (threadPool.isInstanceOf[ThreadPoolExecutor]) {
            val fjth = threadPool.asInstanceOf[ThreadPoolExecutor];
            log.error("DDC-Dispatcher:" + name + ",tp[A=" + fjth.getActiveCount + ",Q=" + fjth.getQueue.size() + ",C=" + fjth.getPoolSize
              + ",Core=" + fjth.getCorePoolSize
              + ",Max=" + fjth.getMaximumPoolSize + ",Largest=" + fjth.getLargestPoolSize
              +",taskcc="+fjth.getTaskCount
              + "],defQ.size=" + q.size());
          }
          lastlogTime = System.currentTimeMillis();
        }
        val task = q.poll(DDCConfig.DEFAULT_DISPATCHER_QUEUE_WAIT_MS, TimeUnit.MILLISECONDS);
        threadPool.submit(task);
      } catch {
        case oom: java.lang.OutOfMemoryError =>
          log.error("oom in dispatching:" + name + ",q.size=" + q.size(), oom);
          System.exit(-1);
        case npe: NullPointerException          =>
        case reject: RejectedExecutionException =>
        case t: Throwable =>
          log.error("error in dispatching:" + name + ",q.size=" + q.size() + t, t)
      }
    }

    //    log.error("DDC-Dispatcher [ Quit ]:" + name + ",tp[A=" + threadPool.getActiveThreadCount + ",Q=" + threadPool.getQueuedTaskCount + ",C=" + threadPool.getPoolSize
    //      + ",M=" + threadPool.getParallelism
    //      + ",S=" + threadPool.getStealCount + ",F=" + threadPool.getRunningThreadCount
    //      + "],defQ.size=" + q.size());

  }
}