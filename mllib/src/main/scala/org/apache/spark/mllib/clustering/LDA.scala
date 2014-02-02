package org.apache.spark.mllib.clustering

import org.jblas.DoubleMatrix

import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.expectation.GibbsSampling
import org.apache.spark.rdd.RDD
import org.apache.spark.{AccumulableParam, SparkContext, Logging}
import java.util.Random

case class LDAParams (
    docCounts: DoubleMatrix,
    topicCounts: DoubleMatrix,
    docTopicCounts: DoubleMatrix,
    topicTermCounts: DoubleMatrix)
  extends Serializable {

  def update(docId: Int, term: Int, topic: Int, inc: Int) = {
    docCounts.put(docId, 0, docCounts.get(docId, 0) + inc)
    topicCounts.put(topic, 0, topicCounts.get(topic, 0) + inc)
    docTopicCounts.put(docId, topic, docTopicCounts.get(docId, topic) + inc)
    topicTermCounts.put(topic, term, topicTermCounts.get(topic, term) + inc)
    this
  }

  def merge(other: LDAParams) = {
    docCounts.addi(other.docCounts)
    topicCounts.addi(other.topicCounts)
    docTopicCounts.addi(other.docTopicCounts)
    topicTermCounts.addi(other.topicTermCounts)
    this
  }

  def dropOneDistSampler(
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      term: Int,
      docId: Int,
      seed: Int)
    : Int =
  {
    val (numTopics, numTerms) = (topicCounts.length, topicTermCounts.columns)
    val topicThisTerm, topicThisDoc = DoubleMatrix.zeros(numTopics)
    val fraction = topicCounts.add(numTerms * topicTermSmoothing)

    topicTermCounts.getColumn(term, topicThisTerm)
    docTopicCounts.getRow(docId, topicThisDoc)

    topicThisTerm.addi(topicTermSmoothing)
    topicThisDoc.addi(docTopicSmoothing)

    topicThisTerm
      .divi(fraction)
      .muli(topicThisDoc)
      .divi(topicThisTerm.sum)

    multinomialDistSampler(topicThisTerm, seed)
  }

  /**
   * A multinomial distribution sampler, using roulette method to sample an Int back.
   */
  private def multinomialDistSampler(dist: DoubleMatrix, seed: Int): Int = {
    val roulette = new Random(seed).nextDouble()

    def loop(index: Int, accum: Double): Int = {
      val sum = accum + dist.get(index)
      if (sum >= roulette) index else loop(index + 1, sum)
    }

    loop(0, 0.0)
  }
}

object LDAParams {
  implicit val ldaParamsAP = new LDAParamsAccumulableParam

  def apply(numDocs: Int, numTopics: Int, numTerms: Int) = new LDAParams(
    DoubleMatrix.zeros(numDocs),
    DoubleMatrix.zeros(numTopics),
    DoubleMatrix.zeros(numDocs, numTopics),
    DoubleMatrix.zeros(numTopics, numTerms))
}

class LDAParamsAccumulableParam extends AccumulableParam[LDAParams, (Int, Int, Int, Int)] {
  def addAccumulator(r: LDAParams, t: (Int, Int, Int, Int)) = {
    val (docId, term, topic, inc) = t
    r.update(docId, term, topic, inc)
  }

  def addInPlace(r1: LDAParams, r2: LDAParams): LDAParams = r1.merge(r2)

  def zero(initialValue: LDAParams): LDAParams = initialValue
}

class LDA private (
    var numTopics: Int,
    var docTopicSmoothing: Double,
    var topicTermSmoothing: Double,
    var numIteration: Int,
    var numDocs: Int,
    var numTerms: Int)
  extends Serializable with Logging {

  def run(input: RDD[Document]): LDAParams = {
    GibbsSampling.runGibbsSampling(
      input,
      numIteration,
      1,
      numTerms,
      numDocs,
      numTopics,
      docTopicSmoothing,
      topicTermSmoothing)
  }
}

object LDA {

  def train(
      data: RDD[Document],
      numTopics: Int,
      docTopicSmoothing: Double,
      topicTermSmoothing: Double,
      numIterations: Int,
      numDocs: Int,
      numTerms: Int)
    : (DoubleMatrix, DoubleMatrix) =
  {
    val lda = new LDA(numTopics,
      docTopicSmoothing,
      topicTermSmoothing,
      numIterations,
      numDocs,
      numTerms)
    val model = lda.run(data)
    GibbsSampling.
      solvePhiAndTheta(model, numTopics, numTerms, docTopicSmoothing, topicTermSmoothing)
  }

  def main(args: Array[String]) {
    if (args.length != 5) {
      println("Usage: LDA <master> <input_dir> <k> <max_iterations> <mini-split>")
      System.exit(1)
    }

    val (master, inputDir, k, iters, minSplit) =
      (args(0), args(1), args(2).toInt, args(3).toInt, args(4).toInt)
    val checkPointDir = System.getProperty("spark.gibbsSampling.checkPointDir", "/tmp/lda-cp")
    val sc = new SparkContext(master, "LDA")
    sc.setCheckpointDir(checkPointDir)
    val (data, wordMap, docMap) = MLUtils.loadCorpus(sc, inputDir, minSplit)
    val numDocs = docMap.size
    val numTerms = wordMap.size

    val (phi, theta) = LDA.train(data, k, 0.01, 0.01, iters, numDocs, numTerms)
    val pp = GibbsSampling.perplexity(data, phi, theta)
    // println(s"final model Phi is $phi")
    // println(s"final model Theta is $theta")
    println(s"final mode perplexity is $pp")
  }
}
