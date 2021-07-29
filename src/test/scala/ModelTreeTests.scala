import org.scalatest.FunSuite
import org.scalatest.Matchers.{an, be}
import parser.{ConfigNode, ModelTree}

class ModelTreeTests extends FunSuite {
  test("ModelTree.getAllPaths") {
    val tree = ModelTree(Seq(
      ConfigNode("study", Seq("patient", "fund")),
      ConfigNode("fund", Seq()),
      ConfigNode("patient", Seq("sample")),
      ConfigNode("sample", Seq("tissue")),
      ConfigNode("tissue", Seq()),
    ))

    val result = ModelTree.constructPaths(tree.root).map(path => path.map(node => node.name)).toSet

    assert(result === Set(Seq("study", "fund"), Seq("study", "patient", "sample", "tissue")))
  }

  test("ModelTree.constructPredecessorsPath") {
    val tree = ModelTree(Seq(
      ConfigNode("study", Seq("patient")),
      ConfigNode("patient", Seq("sample")),
      ConfigNode("sample", Seq("tissue")),
      ConfigNode("tissue", Seq()),
    ))

    assert(tree.getPathToPredecessor("patient", "study") === Seq("study"))
    assert(tree.getPathToPredecessor("tissue", "study") === Seq("sample", "patient", "study"))
    an [IllegalArgumentException] should be thrownBy {
      tree.getPathToPredecessor("patient", "tissue")
    }
  }
}