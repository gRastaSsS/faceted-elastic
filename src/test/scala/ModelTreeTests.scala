import org.scalatest.FunSuite
import org.scalatest.Matchers.{an, be}
import parser.ModelTree

class ModelTreeTests extends FunSuite {
  test("ModelTree.constructPredecessorsPath") {
    val tree = ModelTree(Seq(
      "study" -> "patient",
      "patient" -> "sample",
      "sample" -> "tissue"
    ))

    assert(tree.getPathToPredecessor("patient", "study") === Seq("study"))
    assert(tree.getPathToPredecessor("tissue", "study") === Seq("sample", "patient", "study"))
    an [IllegalArgumentException] should be thrownBy {
      tree.getPathToPredecessor("patient", "tissue")
    }
  }
}