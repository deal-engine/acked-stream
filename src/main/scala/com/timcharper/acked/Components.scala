package com.timcharper.acked

import akka.NotUsed
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._

import scala.concurrent._
import scala.collection.mutable.{Buffer, LinkedHashMap}

object Components {

  /**
    Request bundling buffer.

    Borrowed heavily from Akka-stream 2.0-M1 implementation. Works
    like a normal buffer; however, duplicate items in the buffer get
    bundled, rather than queued; when the item into which the
    duplicate item was bundled gets acked, the duplicate item (and all
    other cohort bundled items) are acked.

    FIFO, except when duplicate items are bundled into items later in
    the queue.

    In order for bundling to work, items MUST be comparable by value
    (IE case classes) and MUST be immutable (IE case classes that
    don't use var). Ultimately, the input item is used as a key in a
    hashmap.

    @param size The size of the buffer. Bundled items do not count against the size.
    @param overflowStrategy How should we handle buffer overflow? Note: items are failed with DroppedException.

    @return An AckedFlow which runs the bundling buffer component.
    */
  def bundlingBuffer[T](
    size: Int,
    overflowStrategy: OverflowStrategy
  ): AckedFlow[T, T, NotUsed] = AckedFlow {
    Flow[(Promise[Unit], T)].via(BundlingBuffer(size, overflowStrategy))
  }

  abstract class BundlingBufferException(msg: String)
      extends RuntimeException(msg)
  case class BufferOverflowException(msg: String)
      extends BundlingBufferException(msg)
  case class DroppedException(msg: String) extends BundlingBufferException(msg)
  case class DownstreamFinishedException(msg: String)
      extends BundlingBufferException(msg)

  /* we have to pull these out again and make the capitals for
   * pattern matching. Akka is the ultimate hider of useful
   * types. */
  private val DropHead = OverflowStrategy.dropHead
  private val DropTail = OverflowStrategy.dropTail
  private val DropBuffer = OverflowStrategy.dropBuffer
  private val DropNew = OverflowStrategy.dropNew
  private val Backpressure = OverflowStrategy.backpressure
  private val Fail = OverflowStrategy.fail

  case class BundlingBuffer[U](size: Int, overflowStrategy: OverflowStrategy)
      extends GraphStage[FlowShape[(Promise[Unit], U), (Promise[Unit], U)]] {
    type T = (Promise[Unit], U)

    val in = Inlet[T]("BundlingBuffer.in")
    val out = Outlet[T]("BundlingBuffer.out")

    override def shape: FlowShape[T, T] = FlowShape.of(in, out)
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) with InHandler with OutHandler
      with StageLogging {

        private val promises =
          scala.collection.mutable.LinkedHashMap.empty[U, Promise[Unit]]
        private val buffer = scala.collection.mutable.Buffer.empty[U]

        def bufferIsFull = buffer.length >= size
        def bufferWillFull = buffer.length + 1 >= size

        def enqueue(v: T): Unit =
          promises.get(v._2) match {
            case Some(p) =>
              v._1.completeWith(p.future)
            case None =>
              promises(v._2) = v._1
              buffer.append(v._2)
          }

        def dequeue(): (Promise[Unit], U) = {
          val v = buffer.remove(0)
          (promises.remove(v).get, v)
        }

        def fail(values: Seq[U], failure: Throwable): Unit =
          values.foreach { i =>
            promises
              .remove(i)
              .map(_.tryFailure(failure))
          }

        def drop(values: U*): Unit =
          fail(
            values,
            DroppedException(
              s"message was dropped due to buffer overflow; size = $size"
            )
          )

        override def onUpstreamFinish(): Unit =
          super.onUpstreamFinish()

        override def onUpstreamFailure(ex: Throwable): Unit =
          super.onUpstreamFailure(ex)

        override def onDownstreamFinish(): Unit =
          super.onDownstreamFinish()

        override def preStart(): Unit = pull(in)

        override def onPull(): Unit =
          if (buffer.nonEmpty) push(out, dequeue())
          else if (isAvailable(in)) {
            onPush()
            onPull()
          } else if (!hasBeenPulled(in)) pull(in)

        override def onPush(): Unit =
          if (!bufferIsFull) {
            // if (isAvailable(out))
            enqueue(grab(in))
          } else
            overflowStrategy match {
              case DropNew  => grab(in) // take element but ignore it
              case DropHead => drop(buffer.remove(0))
              case DropTail => drop(buffer.remove(size - 1))
              case DropBuffer =>
                drop(buffer: _*)
                buffer.clear()
              case Fail =>
                failStage(
                  new BufferOverflowException(
                    s"Buffer overflow (max capacity was: $size)!"
                  )
                )
              case Backpressure =>
              // TODO handle properly
            }

        setHandlers(in, out, handler = this)
      }
  }

//          new InHandler {
//            override def onPush(): Unit = {
//              if (isAvailable(in) && isAvailable(out)) {
//                enqueue(grab(in))
//                pull(in)
//                push(out, dequeue())
//              } else if (isAvailable(in)) {
//                if (bufferIsFull)
//                  overflowStrategy match {
//                    case DropHead => dropped(buffer.remove(0))
//                    case DropTail => dropped(buffer.remove(buffer.length - 1))
//                    case DropBuffer =>
//                      dropped(buffer: _*)
//                      buffer.clear()
//                    case Fail =>
//                      failStage(
//                        new BufferOverflowException(
//                          s"Buffer overflow (max capacity was: $size)!"
//                        )
//                      )
//                    case _ => () // Other cases don't modify the buffer
//                  }
//                if (!bufferIsFull || overflowStrategy != Backpressure) {
//                  enqueue(grab(in))
//                  pull(in)
//                }
//              }
//            }
//          }
//        )
//
//        setHandler(out, new OutHandler {
//
//          override def onDownstreamFinish(): Unit = {
//            if (buffer.nonEmpty) {
//              downstreamFinished(buffer: _*)
//              buffer.clear()
//            }
//            super.onDownstreamFinish()
//          }
//
//          override def onPull(): Unit = {
//            if (buffer.isEmpty)
//            else push(out, dequeue())
//
////            if (buffer.nonEmpty) {
////              push(out, dequeue())
////              if (isAvailable(in)) {
////                enqueue(grab(in))
////                pull(in)
////              }
////            }
//          }
//        })
//      }
//  }
}
