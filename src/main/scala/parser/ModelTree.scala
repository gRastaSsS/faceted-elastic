package parser

case class ConfigNode(name: String, children: Seq[String], idField: String = "_id", parentIdField: String = "_parent")

private case class Node(name: String, children: Seq[Node], idField: String, parentIdField: String)

class ModelTree private (private val root: Node) {
  def nodeNames: Seq[String] = nodeNames(root)

  private val predecessorsMap: Map[String, String] = {
    def collect(parent: Option[Node], child: Node): Seq[(String, String)] = {
      val result = child.children.flatMap(ch => collect(Option(child), ch))

      parent match {
        case Some(parent) => (child.name -> parent.name) +: result
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
    node.name +: node.children.flatMap(n => nodeNames(n))
  }
}

object ModelTree {
  def apply(conf: Seq[ConfigNode]): ModelTree = {
    val nameToConf = conf map (p => p.name -> p) toMap

    val fromNames = conf.map(p => p.name).toSet
    val toNames = conf.flatMap(p => p.children).toSet
    val rootNames = fromNames diff toNames

    require(rootNames.size == 1, "Must be only one root")

    def createNode(confNode: ConfigNode): Node = {
      Node(
        name = confNode.name,
        children = confNode.children.map(p => createNode(nameToConf(p))),
        idField = confNode.idField,
        parentIdField = confNode.parentIdField
      )
    }

    new ModelTree(createNode(nameToConf(rootNames.head)))
  }
}
