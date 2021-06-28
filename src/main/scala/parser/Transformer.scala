package parser

case class RootProperty(name: String)

case class SuccessorProperties(successorType: String, names: Seq[String])

case class SuccessorAggregation(successorType: String, exportName: String, funcs: Seq[String])

case class RootFacts(hasProperties: Seq[RootProperty] = Seq.empty,
                     hasSuccessorsWithProperties: Seq[SuccessorProperties] = Seq.empty,
                     aggregates_successors: Seq[SuccessorAggregation] = Seq.empty)

case class Transformer(name: String, output: String, root: String, rootFacts: RootFacts)