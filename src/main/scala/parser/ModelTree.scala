package parser

case class Node(children: Seq[Node], value: String)

class ModelTree private (private val root: Node) {
  def nodeNames: Seq[String] = nodeNames(root)

  private val predecessorsMap: Map[String, String] = {
    def collect(parent: Option[Node], child: Node): Seq[(String, String)] = {
      val result = child.children.flatMap(ch => collect(Option(child), ch))

      parent match {
        case Some(parent) => (child.value -> parent.value) +: result
        case None => result
      }
    }

    collect(Option.empty, root).toMap
  }

  def getPathToPredecessor(ancestor: String, predecessor: String): Seq[String] = {
    if (ancestor == predecessor) Seq.empty
    else {
      predecessorsMap.get(ancestor) match {
        case Some(parent) => parent +: getPathToPredecessor(parent, predecessor)
        case None => throw new IllegalArgumentException("Path doesn't exist")
      }
    }
  }

  private def nodeNames(node: Node): Seq[String] = {
    node.value +: node.children.flatMap(n => nodeNames(n))
  }
}

object ModelTree {
  def apply(transitions: Seq[(String, String)]): ModelTree = {
    val fromNames = transitions.map(p => p._1).toSet
    val toNames = transitions.map(p => p._2).toSet
    val rootNames = fromNames diff toNames

    require(rootNames.size == 1, "Must be only one root")

    def createNode(nodeName: String): Node = {
      Node(
        children = transitions
          .filter(p => p._1 == nodeName)
          .map(p => createNode(p._2)),
        value = nodeName
      )
    }

    new ModelTree(createNode(rootNames.head))
  }
}
