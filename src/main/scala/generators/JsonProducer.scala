package generators

import java.io.PrintWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.util.Random

object JsonProducer {
  def main(args: Array[String]): Unit = {
    val path = args(0)
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val studySize = args(1).toInt
    val patientsPerStudy = 100
    val samplesPerStudy = 100
    val patientSize = studySize * patientsPerStudy
    val sampleSize = studySize * patientsPerStudy * samplesPerStudy

    val data = generateStudies(studySize) ++ generatePatients(patientSize, patientsPerStudy) ++ generateSamples(sampleSize, samplesPerStudy)

    new PrintWriter(path) {
      for (entity <- data) {
        val jsonBody = mapper.writeValueAsString(entity)
        write(jsonBody + "\n")
      }
      close()
    }
  }

  private def generateStudies(size: Int): Seq[Map[String, Any]] = {
    val countryValues = Seq("UK", "US", "Germany", "Japan", "Russia")
    val typeValues = Seq("clinical", "other")

    (0 until size by 1)
      .map(id => Map(
        "_table" -> "study",
        "_id" -> id,
        "country" -> countryValues(Random.nextInt(countryValues.length)),
        "type" -> typeValues(Random.nextInt(typeValues.length))
      ))
  }

  private def generatePatients(size: Int, perParent: Int): Seq[Map[String, Any]] = {
    val sexValues = Seq("male", "female")

    (0 until size by 1)
      .map(id => Map(
        "_table" -> "patient",
        "_id" -> id,
        "_parentId" -> id / perParent,
        "age" -> Random.nextInt(80),
        "sex" -> sexValues(Random.nextInt(sexValues.length))
      ))
  }

  private def generateSamples(size: Int, perParent: Int): Seq[Map[String, Any]] = {
    val typeValues = Seq("Type_1", "Type_2", "Type_3")
    val organValues = Seq("Liver", "Lung", "Heart", "Brain")

    (0 until size by 1)
      .map(id => Map(
        "_table" -> "sample",
        "_id" -> id,
        "_parentId" -> id / perParent,
        "type" -> typeValues(Random.nextInt(typeValues.length)),
        "organ" -> organValues(Random.nextInt(organValues.length))
      ))
  }
}


