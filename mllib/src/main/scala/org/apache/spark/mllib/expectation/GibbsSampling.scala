package org.apache.spark.mllib.expectation

import org.jblas.DoubleMatrix

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.mllib.clustering.{LDAParams, Document}
import java.util.Random

class GibbsSampling

object GibbsSampling extends Logging {
  import LDAParams._

  /**
   * A uniform distribution sampler, which is only used for initialization.
   */
  private def uniformDistSampler(rand: Random, dimension: Int): Int = rand.nextInt(dimension)

  /**
   * Main function of running a Gibbs sampling method.
   * It contains two phases of total Gibbs sampling:
   * first is initialization, second is real sampling.
   */
  def runGibbsSampling(
      data: RDD[Document],
      numOuterIterations: Int,
      numInnerIterations: Int,
      numTerms: Int,
      numDocs: Int,
      numTopics: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double)
    : LDAParams =
  {
    // construct topic assignment RDD
    logInfo("Start initialization")

    val cpInterval = System.getProperty("spark.gibbsSampling.checkPointInterval", "10").toInt

    val sc = data.context
    val zeroParams = LDAParams(numDocs, numTopics, numTerms)
    val initialParams = sc.accumulable(zeroParams)

    val initialChosenTopics = data.map { case Document(docId, content) =>
      content.map { term =>
        val topic = uniformDistSampler(new Random(docId ^ term), numTopics)
        initialParams += (docId, term, topic, 1)
        topic
      }
    }.cache()

    initialChosenTopics.foreach(_ => ())

    // Gibbs sampling
    val (params, _, _) = Iterator.iterate((initialParams, initialChosenTopics, 0)) {
      case (lastParams, lastChosenTopics, i) =>
        logInfo("Start Gibbs sampling")

        val params = sc.accumulable(lastParams.value)
        val chosenTopics = data.zip(lastChosenTopics).map {
          case (Document(docId, content), topics) =>
            content.zip(topics).map { case (term, topic) =>
              lastParams += (docId, term, topic, -1)

              val seed = docId ^ term + i
              val chosenTopic = lastParams.localValue.dropOneDistSampler(
                docTopicSmoothing, topicTermSmoothing, term, docId, seed)

              lastParams += (docId, term, chosenTopic, 1)
              params += (docId, term, chosenTopic, 1)

              chosenTopic
            }
        }.cache()

        if (i + 1 % cpInterval == 0) {
          chosenTopics.checkpoint()
        }

        chosenTopics.foreach(_ => ())
        lastChosenTopics.unpersist()

        (params, chosenTopics, i + 1)
    }.drop(1 + numOuterIterations).next()

    params.value
  }

  /**
   * We use LDAParams to infer parameters Phi and Theta.
   */
  def solvePhiAndTheta(
      params: LDAParams,
      numTopics: Int,
      numTerms: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double)
    : (DoubleMatrix, DoubleMatrix) =
  {
    val docCount = params.docCounts.add(docTopicSmoothing * numTopics)
    val topicCount = params.topicCounts.add(topicTermSmoothing * numTerms)
    val docTopicCount = params.docTopicCounts.add(docTopicSmoothing)
    val topicTermCount = params.topicTermCounts.add(topicTermSmoothing)
    (topicTermCount.divColumnVector(topicCount), docTopicCount.divColumnVector(docCount))
  }

  /**
   * Perplexity is a kind of evaluation method of LDA. Usually it is used on unseen data.
   * But here we use it for current documents, which is also OK.
   * If using it on unseen data, you must do an iteration of Gibbs sampling before calling this.
   * Small perplexity means good result.
   */
  def perplexity(data: RDD[Document], phi: DoubleMatrix, theta: DoubleMatrix): Double = {
    val (termProb, totalNum) = data.flatMap { case Document(docId, content) =>
      val currentTheta = theta.getRow(docId).mmul(phi)
      content.map(x => (math.log(currentTheta.get(x)), 1))
    }.reduce { (left, right) =>
      (left._1 + right._1, left._2 + right._2)
    }
    math.exp(-1 * termProb / totalNum)
  }
}
