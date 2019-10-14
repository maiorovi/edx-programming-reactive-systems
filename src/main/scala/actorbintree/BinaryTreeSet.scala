/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case c: Contains => root ! c
    case insert: Insert => root ! insert
    case remove: Remove => root ! remove
    case GC => {
      val oldRoot = root
      val newRoot = createRoot
      root = newRoot
      context.become(garbageCollecting(newRoot))
      oldRoot ! CopyTo(newRoot)
    }
  }


  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case op: Operation => {
      pendingQueue = pendingQueue.enqueue(op)
    }
    case GC =>
    case CopyFinished => {
      val oldQueue = pendingQueue
      oldQueue.foreach(self ! _)
      pendingQueue = Queue.empty[Operation]
      context.unbecome()
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case c: Contains => {
      performContains(c)
    }
    case insertion: Insert => {
      performInsertion(insertion)
    }
    case remove: Remove => performRemove(remove)
    case CopyTo(newRoot) => {
      val values = subtrees.values.toSet

      if (!removed) {
        newRoot ! Insert(self, 0, elem)
      }

      if (values.nonEmpty) {
        values.foreach( actorRef => actorRef ! CopyTo(newRoot))
      }

      checkCopyingDone(values, removed)
    }

    case _ => ???
  }

  private def performRemove(remove: Remove): Unit = {
    if (this.elem == remove.elem) {
      removed = true
      remove.requester ! OperationFinished(remove.id)
      return
    }

    val position = findPositionToProcess(remove.elem)

    processOperationRequest(position,
      actorRef => actorRef ! remove,
      () => {
        remove.requester ! OperationFinished(remove.id)
      })
  }

  private def performInsertion(insert: Insert): Unit = {
    if (this.elem == insert.elem) {
      removed = false
      insert.requester ! OperationFinished(insert.id)
      return
    }

    val position = findPositionToProcess(insert.elem)

    processOperationRequest(position,
      actorRef => actorRef ! insert,
      () => {
        val actor = context.actorOf(BinaryTreeNode.props(insert.elem, false))
        subtrees += (position -> actor)
        insert.requester ! OperationFinished(insert.id)
    })
  }


  private def performContains(c: Contains):Unit = {
    if (this.elem == c.elem ) {
      val isContains = if (removed) false else true
      c.requester ! ContainsResult(c.id, isContains)
      return
    }
    val position = findPositionToProcess(c.elem)

    processOperationRequest(position, actorRef => actorRef ! c, () => c.requester ! ContainsResult(c.id, false))
  }

  private def findPositionToProcess(elem: Int):Position = if (this.elem < elem) Right else Left

  private def processOperationRequest(pos: Position, onPresent: ActorRef => Unit, onAbsent: () => Unit): Unit = subtrees get pos match {
      case Some(actorRef) =>onPresent(actorRef)
      case None => onAbsent()
    }



  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case CopyFinished => checkCopyingDone(expected - sender(), insertConfirmed)
    case OperationFinished(id) => checkCopyingDone(expected, true)

  }

  def checkCopyingDone(expected: Set[ActorRef], insertConfirmed: Boolean):Unit = {
    if (expected.isEmpty && insertConfirmed) {
      context.parent ! CopyFinished
      self ! PoisonPill
    } else context.become(copying(expected, insertConfirmed))
  }

}
