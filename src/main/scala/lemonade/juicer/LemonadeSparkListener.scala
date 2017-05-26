package lemonade.juicer.spark

import lemonade.util.Logging
import lemonade.util.PrivateMethodExposer.p

import java.io.OutputStreamWriter

import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.SparkContext
import org.apache.spark.scheduler._
import org.apache.spark.util.JsonProtocol

/** Juicer Lemonade Task Info */

case class JuicerListenerTaskStart(jsonEvent: String) 
  extends SparkListenerEvent

case class JuicerListenerTaskEnd(jsonEvent: String)
  extends SparkListenerEvent

case class JuicerListenerJobStart(jsonEvent: String) 
  extends SparkListenerEvent

case class JuicerListenerJobEnd(jsonEvent: String)
  extends SparkListenerEvent

/** */

class LemonadeSparkListener extends SparkListener with Logging {

  def sparkContext: SparkContext = LemonadeSparkListener.sparkContext

  private lazy val jsonProtocol: Object = {
    val clazz = Class.forName("org.apache.spark.util.JsonProtocol$")
    clazz.getField("MODULE$").get(clazz)
  }

  def sparkEventToJson(event: SparkListenerEvent): JValue = {
    p(jsonProtocol)('sparkEventToJson)(event).asInstanceOf[JValue]
  }

  lazy val outputStream: OutputStreamWriter = {
    val fs = FileSystem.get(sparkContext.hadoopConfiguration)
    val eventLogDir = sparkContext.getConf.get(
      "lemonade.juicer.eventLog.dir", "/tmp")
    val logPath = new Path(eventLogDir,
      s"${sparkContext.applicationId}-lemonade")
    new OutputStreamWriter(fs.create(logPath))
  }

  /** Log the event as JSON. */
  private def logEvent(event: SparkListenerEvent) {
    val eventJson = sparkEventToJson(event)
    outputStream.write(compact(render(eventJson)))
    outputStream.write("\n")
  }
  
  override def onStageCompleted(
      stageCompleted: SparkListenerStageCompleted): Unit = {
    logEvent(stageCompleted)
  }

  override def onStageSubmitted(
      stageSubmitted: SparkListenerStageSubmitted): Unit = {
    logEvent(stageSubmitted)
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    logEvent(taskStart)
  }

  override def onTaskGettingResult(
      taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    logEvent(taskGettingResult)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    logEvent(taskEnd)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logEvent(jobStart)
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logEvent(jobEnd)
  }

  override def onEnvironmentUpdate(
      environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    logEvent(environmentUpdate)
  }

  override def onBlockManagerAdded(
      blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    logEvent(blockManagerAdded)
  }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    logEvent(blockManagerRemoved)
  }

  override def onUnpersistRDD(
      unpersistRDD: SparkListenerUnpersistRDD): Unit = {
    logEvent(unpersistRDD)
  }

  override def onApplicationStart(
      applicationStart: SparkListenerApplicationStart): Unit = {
    logEvent(applicationStart)
  }

  override def onApplicationEnd(
      applicationEnd: SparkListenerApplicationEnd): Unit = {
    logEvent(applicationEnd)
    outputStream.flush()
    outputStream.close()
  }

  override def onExecutorMetricsUpdate(
      executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    logEvent(executorMetricsUpdate)
  }

  override def onExecutorAdded(
      executorAdded: SparkListenerExecutorAdded): Unit = {
    logEvent(executorAdded)
  }

  override def onExecutorRemoved(
      executorRemoved: SparkListenerExecutorRemoved): Unit = {
    logEvent(executorRemoved)
  }

  /*
  override def onExecutorBlacklisted(
      executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = { }

  override def onExecutorUnblacklisted(
      executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = { }

  override def onNodeBlacklisted(
      nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = { }

  override def onNodeUnblacklisted(
      nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = { }

  override def onBlockUpdated(
      blockUpdated: SparkListenerBlockUpdated): Unit = {
  }
  */

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case ev: JuicerListenerJobStart =>
      onJuicerJobStart(ev)
    case ev: JuicerListenerJobEnd =>
      onJuicerJobEnd(ev)
    case ev: JuicerListenerTaskStart =>
      onJuicerTaskStart(ev)
    case ev: JuicerListenerTaskEnd =>
      onJuicerTaskEnd(ev)
    case _ =>
      logEvent(event)
  }

  /**
   * Lemonade Juicer specific event handlers
   */
  private def onJuicerJobStart(event: JuicerListenerJobStart): Unit = {
    logInfo (s"Received juicer job start: ${event}")
    outputStream.write(event.jsonEvent)
    outputStream.write("\n")
  }
  
  private def onJuicerJobEnd(event: JuicerListenerJobEnd): Unit = {
    logInfo (s"Received juicer job end: ${event}")
    outputStream.write(event.jsonEvent)
    outputStream.write("\n")
    outputStream.flush()
  }

  private def onJuicerTaskStart(event: JuicerListenerTaskStart): Unit = {
    logInfo (s"Received juicer task start: ${event}")
    outputStream.write(event.jsonEvent)
    outputStream.write("\n")
  }
  
  private def onJuicerTaskEnd(event: JuicerListenerTaskEnd): Unit = {
    logInfo (s"Received juicer task end: ${event}")
    outputStream.write(event.jsonEvent)
    outputStream.write("\n")
  }
}

object LemonadeSparkListener {

  // event types
  val JOB_START = "job_start"
  val JOB_END = "job_end"
  val TASK_START = "task_start"
  val TASK_END = "task_end"

  private lazy val sparkContext: SparkContext = SparkContext.getOrCreate()

  /**
   * Bypass spark context to post additional events to the default listener bus
   */
  private def postToBus(sc: SparkContext) = {
    p(p(sc)('listenerBus)().asInstanceOf[AnyRef])('post)
  }

  /**
   * Post a custom event to the spark listener bus
   */
  def post(eventType: String, eventData: String): Unit = {
    val event = eventType match {
      case JOB_START =>
        JuicerListenerJobStart(eventData)
      case JOB_END =>
        JuicerListenerJobEnd(eventData)
      case TASK_START =>
        JuicerListenerTaskStart(eventData)
      case TASK_END =>
        JuicerListenerTaskEnd(eventData)
      case _ =>
        throw new RuntimeException(s"Unknown event type: ${eventType}")
    }
    postToBus(sparkContext)(event)
  }
}
