package generators

import java.io.PrintWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.mutable
import scala.util.Random

object JsonProducer {
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
        getDataDepthFirst(studySize, patientsPerStudy, samplesPerPatient)
      case "breadth-first" =>
        generateStudies(studySize) ++ generatePatients(patientSize, patientsPerStudy) ++ generateSamples(sampleSize, samplesPerPatient)
    }
  }

  private def getDataDepthFirst(studySize: Int, patientsPerStudy: Int, samplesPerPatient: Int): Seq[Map[String, Any]] = {
    val result = mutable.Buffer[Map[String, Any]]()
    var patientId = 0
    var sampleId = 0

    for (studyId <- 0 until studySize) {
      result.append(generateStudy(studyId))

      for (_ <- 0 until patientsPerStudy) {
        result.append(generatePatient(patientId, studyId))

        for (_ <- 0 until samplesPerPatient) {
          result.append(generateSample(sampleId, patientId))
          sampleId += 1
        }

        patientId += 1
      }
    }

    result
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


