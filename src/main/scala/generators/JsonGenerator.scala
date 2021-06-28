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

    val studySize = args(1).toInt
    val patientsPerStudy = 100
    val samplesPerPatient = 100
    val data = getData(studySize, patientsPerStudy, samplesPerPatient, args(2))

    new PrintWriter(path) {
      for (entity <- data) {
        val jsonBody = mapper.writeValueAsString(entity)
        write(jsonBody + "\n")
      }
      close()
    }
  }

  private def getData(studySize: Int, patientsPerStudy: Int, samplesPerPatient: Int, order: String): Seq[Map[String, Any]] = {
    val patientSize = studySize * patientsPerStudy
    val sampleSize = studySize * patientsPerStudy * samplesPerPatient

    order match {
      case "depth-first" =>
        getDepthFirstData(Map(
          "study" -> studySize,
          "patient" -> patientsPerStudy,
          "sample" -> samplesPerPatient
        ))
      case "breadth-first" =>
        generateStudies(studySize) ++ generatePatients(patientSize, patientsPerStudy) ++ generateSamples(sampleSize, samplesPerPatient)
    }
  }

  private def getDepthFirstData(levelSizes: Map[String, Int]): Stream[Map[String, Any]] = {

    def gen(level: String, levelSize: Int, parentId: Int = 0): Stream[Map[String, Any]] = {
      level match {
        case "study" =>
          if (levelSize == levelSizes("study")) Stream.empty
          else (
            generateStudy(levelSize)
              #:: gen("patient", 0, parentId = levelSize)
              #::: gen("study", levelSize + 1)
            )
        case "patient" =>
          if (levelSize == levelSizes("patient")) Stream.empty
          else {
            val id = parentId * levelSizes("patient") + levelSize
            (generatePatient(id, parentId)
              #:: gen("sample", 0, parentId = id)
              #::: gen("patient", levelSize + 1, parentId)
              )
          }
        case "sample" =>
          if (levelSize == levelSizes("sample")) Stream.empty
          else {
            val id = parentId * levelSizes("sample") + levelSize
            (generateSample(id, parentId)
              #:: gen("sample", levelSize + 1, parentId)
              )
          }
      }
    }

    gen("study", 0)
  }

  private def generateStudies(size: Int): Seq[Map[String, Any]] = {
    (0 until size by 1)
      .map(id => generateStudy(id))
  }

  private def generatePatients(size: Int, perParent: Int): Seq[Map[String, Any]] = {
    (0 until size by 1)
      .map(id => generatePatient(id, id / perParent))
  }

  private def generateSamples(size: Int, perParent: Int): Seq[Map[String, Any]] = {
    (0 until size by 1)
      .map(id => generateSample(id, id / perParent))
  }

  private def generateStudy(id: Int): Map[String, Any] = {
    val countryValues = Seq("UK", "US", "Germany", "Japan", "Russia")
    val typeValues = Seq("clinical", "other")

    Map(
      "_table" -> "study",
      "_id" -> id,
      "country" -> countryValues(Random.nextInt(countryValues.length)),
      "type" -> typeValues(Random.nextInt(typeValues.length))
    )
  }

  private def generatePatient(id: Int, parentId: Int): Map[String, Any] = {
    val sexValues = Seq("male", "female")

    Map(
      "_table" -> "patient",
      "_id" -> id,
      "_parentId" -> parentId,
      "age" -> Random.nextInt(80),
      "sex" -> sexValues(Random.nextInt(sexValues.length))
    )
  }

  private def generateSample(id: Int, parentId: Int): Map[String, Any] = {
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
}


