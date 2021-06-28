package generators

import java.io.PrintWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.util.Random

object JsonGenerator {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val description = Map("study" -> args(1).toInt, "patient" -> 100, "sample" -> 100)
    val order = args(2)
    val writer = new PrintWriter(path)

    order match {
      case "depth-first" => writeDepthFirst(description, writer, mapper)
      case "breadth-first" => writeBreadthFirst(description, writer, mapper)
    }
  }

  private def writeBreadthFirst(description: Map[String, Int], writer: PrintWriter, mapper: ObjectMapper): Unit = {
    try {
      val patientSize = description("study") * description("patient")
      val sampleSize = description("study") * description("patient") * description("sample")

      val data = (generateStudies(description("study"))
        ++ generatePatients(patientSize, description("patient"))
        ++ generateSamples(sampleSize, description("sample")))

      data.foreach(e => {
        val jsonBody = mapper.writeValueAsString(e)
        writer.write(jsonBody + "\n")
      })
    } finally {
      writer.close()
    }
  }

  private def writeDepthFirst(description: Map[String, Int], writer: PrintWriter, mapper: ObjectMapper): Unit = {
    try {
      (0 until description("study"))
        .foreach(studyId => {
          val study = generateDepthFirstStudy(studyId, description)
          study.foreach(data => {
            val jsonBody = mapper.writeValueAsString(data)
            writer.write(jsonBody + "\n")
          })
        })

    } finally {
      writer.close()
    }
  }

  private def generateDepthFirstStudy(id: Int, levelSizes: Map[String, Int], usePartitionKey: Boolean = true): Stream[Map[String, Any]] = {
    val partition = if (usePartitionKey) id else -1
    val study = generateEntity("study", id, partition = partition)

    def gen(level: String, levelSize: Int, parentId: Int, partition: Int = -1): Stream[Map[String, Any]] = {
      level match {
        case "patient" =>
          if (levelSize == levelSizes("patient")) Stream.empty
          else {
            val id = parentId * levelSizes("patient") + levelSize
            (generateEntity("patient", id, parentId, partition = partition)
              #:: gen("sample", 0, parentId = id, partition = partition)
              #::: gen("patient", levelSize + 1, parentId, partition = partition))
          }

        case "sample" =>
          if (levelSize == levelSizes("sample")) Stream.empty
          else {
            val id = parentId * levelSizes("sample") + levelSize
            (generateEntity("sample", id, parentId, partition = partition)
              #:: gen("sample", levelSize + 1, parentId, partition = partition))
          }
      }
    }

    study #:: gen("patient", 0, parentId = id, partition = partition)
  }

  private def generateStudies(size: Int): Seq[Map[String, Any]] = {
    (0 until size by 1)
      .map(id => generateEntity("study", id))
  }

  private def generatePatients(size: Int, perParent: Int): Seq[Map[String, Any]] = {
    (0 until size by 1)
      .map(id => generateEntity("patient", id, id / perParent))
  }

  private def generateSamples(size: Int, perParent: Int): Seq[Map[String, Any]] = {
    (0 until size by 1)
      .map(id => generateEntity("sample", id, id / perParent))
  }

  private def generateEntity(level: String, id: Int, parentId: Int = -1, partition: Int = -1): Map[String, Any] = {
    val data = level match {
      case "study" =>
        val countryValues = Seq("UK", "US", "Germany", "Japan", "Russia")
        val typeValues = Seq("clinical", "other")
        Map(
          "_table" -> "study",
          "_id" -> id,
          "country" -> countryValues(Random.nextInt(countryValues.length)),
          "type" -> typeValues(Random.nextInt(typeValues.length))
        )

      case "patient" =>
        val sexValues = Seq("male", "female")
        Map(
          "_table" -> "patient",
          "_id" -> id,
          "_parentId" -> parentId,
          "age" -> Random.nextInt(80),
          "sex" -> sexValues(Random.nextInt(sexValues.length))
        )

      case "sample" =>
        val typeValues = Seq("Type_1", "Type_2", "Type_3")
        val organValues = Seq("Liver", "Lung", "Heart", "Brain")
        Map(
          "_table" -> "sample",
          "_id" -> id,
          "_parentId" -> parentId,
          "type" -> typeValues(Random.nextInt(typeValues.length)),
          "organ" -> organValues(Random.nextInt(organValues.length))
        )
    }

    if (partition < 0) data
    else data + ("_partition" -> partition)
  }
}


