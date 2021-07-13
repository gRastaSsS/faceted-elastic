import org.scalatest.FunSuite
import org.scalatest.Matchers.{an, be}
import parser.{ConfigNode, ModelTree}

class ModelTreeTests extends FunSuite {
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