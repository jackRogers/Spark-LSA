/*
 * Copyright 2015 Sanford Ryza, Uri Laserson, Sean Owen and Joshua Wills
 *
 * See LICENSE file for further information.
 */

package com.cloudera.datascience.lsa


import com.cloudera.datascience.common.XmlInputFormat

import edu.stanford.nlp.ling.CoreAnnotations.{LemmaAnnotation, SentencesAnnotation, TokensAnnotation}
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}

import edu.umd.cloud9.collection.zendesk._

import java.io.{FileOutputStream, PrintStream}
import java.util.Properties

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConversions._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

import breeze.linalg.{DenseMatrix => BDenseMatrix, DenseVector => BDenseVector,
SparseVector => BSparseVector}

import com.cloudera.datascience.lsa.ParseZen._

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.rdd.RDD

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer

object RunLSA {
  def main() {
    val k = 100
    val numTerms = 50000
    val sampleSize = 0.1

    //@transient val conf = new SparkConf().setAppName("Zen LSA")
    //conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //val sc = new SparkContext(conf)

    val (termDocMatrix, termIds, docIds, idfs) = preprocessing(sampleSize, numTerms, sc)
    termDocMatrix.cache()
    val mat = new RowMatrix(termDocMatrix)
    val svd = mat.computeSVD(k, computeU=true)

    println("Singular values: " + svd.s)
    val topConceptTerms = topTermsInTopConcepts(svd, 10, 10, termIds)
    val topConceptDocs = topDocsInTopConcepts(svd, 10, 10, docIds)
    for ((terms, docs) <- topConceptTerms.zip(topConceptDocs)) {
      println("Concept terms: " + terms.map(_._1).mkString(", "))
      println("Concept docs: " + docs.map(_._1).mkString(", "))
      println()
    }
  }
	
def termDocumentMatrix(docs: RDD[(String, Seq[String])], stopWords: Set[String], numTerms: Int,
      sc: SparkContext): (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {
    val docTermFreqs = docs.mapValues(terms => {
      val termFreqsInDoc = terms.foldLeft(new HashMap[String, Int]()) {
        (map, term) => map += term -> (map.getOrElse(term, 0) + 1)
      }
      termFreqsInDoc
    })

    docTermFreqs.cache()
    val docIds = docTermFreqs.map(_._1).zipWithUniqueId().map(_.swap).collectAsMap()

    val docFreqs = documentFrequenciesDistributed(docTermFreqs.map(_._2), numTerms)
    println("Number of terms: " + docFreqs.size)
    saveDocFreqs("docfreqs.tsv", docFreqs)

    val numDocs = docIds.size

    val idfs = inverseDocumentFrequencies(docFreqs, numDocs)

    // Maps terms to their indices in the vector
    val idTerms = idfs.keys.zipWithIndex.toMap
    val termIds = idTerms.map(_.swap)

    val bIdfs = sc.broadcast(idfs).value
    val bIdTerms = sc.broadcast(idTerms).value

    val vecs = docTermFreqs.map(_._2).map(termFreqs => {
      val docTotalTerms = termFreqs.values.sum
      val termScores = termFreqs.filter {
        case (term, freq) => bIdTerms.contains(term)
      }.map{
        case (term, freq) => (bIdTerms(term), bIdfs(term) * termFreqs(term) / docTotalTerms)
      }.toSeq
      Vectors.sparse(bIdTerms.size, termScores)
    })
    (vecs, termIds, docIds, idfs)
  }

  def documentFrequencies(docTermFreqs: RDD[HashMap[String, Int]]): HashMap[String, Int] = {
    val zero = new HashMap[String, Int]()
    def merge(dfs: HashMap[String, Int], tfs: HashMap[String, Int])
      : HashMap[String, Int] = {
      tfs.keySet.foreach { term =>
        dfs += term -> (dfs.getOrElse(term, 0) + 1)
      }
      dfs
    }
    def comb(dfs1: HashMap[String, Int], dfs2: HashMap[String, Int])
      : HashMap[String, Int] = {
      for ((term, count) <- dfs2) {
        dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
      }
      dfs1
    }
    docTermFreqs.aggregate(zero)(merge, comb)
  }

  def documentFrequenciesDistributed(docTermFreqs: RDD[HashMap[String, Int]], numTerms: Int)
      : Array[(String, Int)] = {
    val docFreqs = docTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _, 15)
    val ordering = Ordering.by[(String, Int), Int](_._2)
    docFreqs.top(numTerms)(ordering)
  }

  def trimLeastFrequent(freqs: Map[String, Int], numToKeep: Int): Map[String, Int] = {
    freqs.toArray.sortBy(_._2).take(math.min(numToKeep, freqs.size)).toMap
  }

  def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Int)
    : Map[String, Double] = {
    docFreqs.map{ case (term, count) => (term, math.log(numDocs.toDouble / count))}.toMap
  }

  def readFile(path: String, sc: SparkContext): RDD[String] = {
    val conf = new Configuration()
    conf.set(XmlInputFormat.START_TAG_KEY, "<ticket>")
    conf.set(XmlInputFormat.END_TAG_KEY, "</ticker>")
    val rawXmls = sc.newAPIHadoopFile(path, classOf[XmlInputFormat], classOf[LongWritable],
      classOf[Text], conf)
    rawXmls.map(p => p._2.toString)
  }

  /**
   * Returns a (title, content) pair
   */
def ZenXmlToPlainText(pageXml: String): Option[(String, String)] = {
    val page = new ZendeskTicket()
    ZendeskTicket.readPage(page, pageXml)
    if (page.isEmpty) { 
	None
    } else {
	Some((page.getTitle, page.getContent))
	}
}

  def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
  }

  def plainTextToLemmas(text: String, stopWords: Set[String], pipeline: StanfordCoreNLP)
    : Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences; token <- sentence.get(classOf[TokensAnnotation])) {
      val lemma = token.get(classOf[LemmaAnnotation])
      if (lemma.length > 2 && !stopWords.contains(lemma) && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
      }
    }
    lemmas
  }

  def isOnlyLetters(str: String): Boolean = {
    // While loop for high performance
    var i = 0
    while (i < str.length) {
      if (!Character.isLetter(str.charAt(i))) {
        return false
      }
      i += 1
    }
    true
  }

  def loadStopWords(path: String) = scala.io.Source.fromFile(path).getLines().toSet

  def saveDocFreqs(path: String, docFreqs: Array[(String, Int)]) {
    val ps = new PrintStream(new FileOutputStream(path))
    for ((doc, freq) <- docFreqs) {
      ps.println(s"$doc\t$freq")
    }
    ps.close()
  }

  /**
   * Returns an RDD of rows of the term document matrix, a mapping of column indices to terms, and a
   * mapping of row IDs to document titles.
   */
  def preprocessing(sampleSize: Double, numTerms: Int, sc: SparkContext)
      : (RDD[Vector], Map[Int, String], Map[Long, String], Map[String, Double]) = {
    //val pages = readFile("hdfs://10.10.1.101:8020/user/jrogers/tickets.xml", sc)
    //  .sample(false, sampleSize, 11L)

	@transient val conf = new Configuration()
	conf.set(XmlInputFormat.START_TAG_KEY, "<ticket>")
	conf.set(XmlInputFormat.END_TAG_KEY, "</ticket>")	
	conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 val pages = readFile("hdfs://10.10.1.101:8020/user/jrogers/tickets.xml", sc)
      .sample(false, sampleSize, 11L)

    val plainText = pages.filter(_ != null).flatMap(ZenXmlToPlainText)


    val stopWords = sc.broadcast(loadStopWords("/home/jrogers/aas-master/ch06-lsa/target/classes/stopwords.txt")).value

    val lemmatized = plainText.mapPartitions(iter => {
      val pipeline = createNLPPipeline()
      iter.map{ case(title, contents) => (title, plainTextToLemmas(contents, stopWords, pipeline))}
    })

    val filtered = lemmatized.filter(_._2.size > 1)

    termDocumentMatrix(filtered, stopWords, numTerms, sc)
  }

  def topTermsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numTerms: Int, termIds: Map[Int, String]): Seq[Seq[(String, Double)]] = {
    val v = svd.V
    val topTerms = new ArrayBuffer[Seq[(String, Double)]]()
    val arr = v.toArray
    for (i <- 0 until numConcepts) {
      val offs = i * v.numRows
      val termWeights = arr.slice(offs, offs + v.numRows).zipWithIndex
      val sorted = termWeights.sortBy(-_._1)
      topTerms += sorted.take(numTerms).map{case (score, id) => (termIds(id), score)}
    }
    topTerms
  }

  def topDocsInTopConcepts(svd: SingularValueDecomposition[RowMatrix, Matrix], numConcepts: Int,
      numDocs: Int, docIds: Map[Long, String]): Seq[Seq[(String, Double)]] = {
    val u  = svd.U
    val topDocs = new ArrayBuffer[Seq[(String, Double)]]()
    for (i <- 0 until numConcepts) {
      val docWeights = u.rows.map(_.toArray(i)).zipWithUniqueId
      topDocs += docWeights.top(numDocs).map{case (score, id) => (docIds(id), score)}
    }
    topDocs
  }

  /**
   * Selects a row from a matrix.
   */
  def row(mat: BDenseMatrix[Double], index: Int): Seq[Double] = {
    (0 until mat.cols).map(c => mat(index, c))
  }

  /**
   * Selects a row from a matrix.
   */
  def row(mat: Matrix, index: Int): Seq[Double] = {
    val arr = mat.toArray
    (0 until mat.numCols).map(i => arr(index + i * mat.numRows))
  }

  /**
   * Selects a row from a distributed matrix.
   */
  def row(mat: RowMatrix, id: Long): Array[Double] = {
    mat.rows.zipWithUniqueId.map(_.swap).lookup(id).head.toArray
  }

  /**
   * Finds the product of a dense matrix and a diagonal matrix represented by a vector.
   * Breeze doesn't support efficient diagonal representations, so multiply manually.
   */
  def multiplyByDiagonalMatrix(mat: Matrix, diag: Vector): BDenseMatrix[Double] = {
    val sArr = diag.toArray
    new BDenseMatrix[Double](mat.numRows, mat.numCols, mat.toArray)
      .mapPairs{case ((r, c), v) => v * sArr(c)}
  }

  /**
   * Finds the product of a distributed matrix and a diagonal matrix represented by a vector.
   */
  def multiplyByDiagonalMatrix(mat: RowMatrix, diag: Vector): RowMatrix = {
    val sArr = diag.toArray
    new RowMatrix(mat.rows.map(vec => {
      val vecArr = vec.toArray
      val newArr = (0 until vec.size).toArray.map(i => vecArr(i) * sArr(i))
      Vectors.dense(newArr)
    }))
  }

  /**
   * Returns a matrix where each row is divided by its length.
   */
  def rowsNormalized(mat: BDenseMatrix[Double]): BDenseMatrix[Double] = {
    val newMat = new BDenseMatrix[Double](mat.rows, mat.cols)
    for (r <- 0 until mat.rows) {
      val length = math.sqrt((0 until mat.cols).map(c => mat(r, c) * mat(r, c)).sum)
      (0 until mat.cols).map(c => newMat.update(r, c, mat(r, c) / length))
    }
    newMat
  }

  /**
   * Returns a distributed matrix where each row is divided by its length.
   */
  def rowsNormalized(mat: RowMatrix): RowMatrix = {
    new RowMatrix(mat.rows.map(vec => {
      val length = math.sqrt(vec.toArray.map(x => x * x).sum)
      Vectors.dense(vec.toArray.map(_ / length))
    }))
  }

  /**
   * Finds terms relevant to a term. Returns the term IDs and scores for the terms with the highest
   * relevance scores to the given term.
   */
  def topTermsForTerm(normalizedVS: BDenseMatrix[Double], termId: Int): Seq[(Double, Int)] = {
    // Look up the row in VS corresponding to the given term ID.
    val termRowVec = new BDenseVector[Double](row(normalizedVS, termId).toArray)

    // Compute scores against every term
    val termScores = (normalizedVS * termRowVec).toArray.zipWithIndex

    // Find the terms with the highest scores
    termScores.sortBy(-_._1).take(10)
  }

  /**
   * Finds docs relevant to a doc. Returns the doc IDs and scores for the docs with the highest
   * relevance scores to the given doc.
   */
  def topDocsForDoc(normalizedUS: RowMatrix, docId: Long): Seq[(Double, Long)] = {
    // Look up the row in US corresponding to the given doc ID.
    val docRowArr = row(normalizedUS, docId)
    val docRowVec = Matrices.dense(docRowArr.length, 1, docRowArr)

    // Compute scores against every doc
    val docScores = normalizedUS.multiply(docRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId

    // Docs can end up with NaN score if their row in U is all zeros.  Filter these out.
    allDocWeights.filter(!_._1.isNaN).top(10)
  }

  /**
   * Finds docs relevant to a term. Returns the doc IDs and scores for the docs with the highest
   * relevance scores to the given term.
   */
  def topDocsForTerm(US: RowMatrix, V: Matrix, termId: Int): Seq[(Double, Long)] = {
    val termRowArr = row(V, termId).toArray
    val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)

    // Compute scores against every doc
    val docScores = US.multiply(termRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.top(10)
  }

  def termsToQueryVector(terms: Seq[String], idTerms: Map[String, Int], idfs: Map[String, Double])
    : BSparseVector[Double] = {
    val indices = terms.map(idTerms(_)).toArray
    val values = terms.map(idfs(_)).toArray
    new BSparseVector[Double](indices, values, idTerms.size)
  }

  def topDocsForTermQuery(US: RowMatrix, V: Matrix, query: BSparseVector[Double])
    : Seq[(Double, Long)] = {
    val breezeV = new BDenseMatrix[Double](V.numRows, V.numCols, V.toArray)
    val termRowArr = (breezeV.t * query).toArray

    val termRowVec = Matrices.dense(termRowArr.length, 1, termRowArr)

    // Compute scores against every doc
    val docScores = US.multiply(termRowVec)

    // Find the docs with the highest scores
    val allDocWeights = docScores.rows.map(_.toArray(0)).zipWithUniqueId
    allDocWeights.top(10)
  }

  def printTopTermsForTerm(normalizedVS: BDenseMatrix[Double],
      term: String, idTerms: Map[String, Int], termIds: Map[Int, String]) {
    printIdWeights(topTermsForTerm(normalizedVS, idTerms(term)), termIds)
  }

  def printTopDocsForDoc(normalizedUS: RowMatrix, doc: String, idDocs: Map[String, Long],
      docIds: Map[Long, String]) {
    printIdWeights(topDocsForDoc(normalizedUS, idDocs(doc)), docIds)
  }

  def printTopDocsForTerm(US: RowMatrix, V: Matrix, term: String, idTerms: Map[String, Int],
      docIds: Map[Long, String]) {
    printIdWeights(topDocsForTerm(US, V, idTerms(term)), docIds)
  }

  def printIdWeights[T](idWeights: Seq[(Double, T)], entityIds: Map[T, String]) {
    println(idWeights.map{case (score, id) => (entityIds(id), score)}.mkString(", "))
  }
}
