//create rdd
import com.cloudera.datascience.common.XmlInputFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import org.apache.spark.SparkContext._

//load zendesk ticket class
//import com.star2star.zendesk._
import edu.umd.cloud9.collection.zendesk._

//lemmatization
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._

//need properties class
import java.util.Properties

//need array buffer class
import scala.collection.mutable.ArrayBuffer

//need rdd class
import org.apache.spark.rdd.RDD

//need foreach 
import scala.collection.JavaConversions._

//computing the tf-idfs
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.mllib.linalg.distributed.RowMatrix

val path = "hdfs://10.10.1.101:8020/user/jrogers/tickets.xml"
@transient val conf = new Configuration()
conf.set(XmlInputFormat.START_TAG_KEY, "<ticket>")
conf.set(XmlInputFormat.END_TAG_KEY, "</ticket>")

//org.apache.spark.rdd.NewHadoopRDD
val kvs = sc.newAPIHadoopFile(path, classOf[XmlInputFormat],
classOf[LongWritable], classOf[Text], conf)

//org.apache.spark.rdd.MapPartitionsRDD
val rawXmls = kvs.map(p => p._2.toString)


def zenXmlToPlainText(xml: String): Option[(String, String)] = {
    val page = new ZendeskTicket()
    ZendeskTicket.readPage(page, xml)
    if (page.isEmpty) None
    else Some((page.getTitle, page.getContent))
}
//org.apache.spark.rdd.MapPartitionsRDD
val plainText = rawXmls.flatMap(zenXmlToPlainText)

def createNLPPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.put("annotators", "tokenize, ssplit, pos, lemma")
    new StanfordCoreNLP(props)
}
def isOnlyLetters(str: String): Boolean = {
    str.forall(c => Character.isLetter(c))
}
def plainTextToLemmas(text: String, stopWords: Set[String],
    pipeline: StanfordCoreNLP): Seq[String] = {
    val doc = new Annotation(text)
    pipeline.annotate(doc)
    val lemmas = new ArrayBuffer[String]()
    val sentences = doc.get(classOf[SentencesAnnotation])
    for (sentence <- sentences;
    token <- sentence.get(classOf[TokensAnnotation])) {
        val lemma = token.get(classOf[LemmaAnnotation])
        if (lemma.length > 2 && !stopWords.contains(lemma)
            && isOnlyLetters(lemma)) {
        lemmas += lemma.toLowerCase
        }
    }
    lemmas
}

// scala.collection.immutable.HashSet$HashTrieSet
val stopWords = sc.broadcast(
    scala.io.Source.fromFile("/home/jrogers/aas-master/ch06-lsa/target/classes/stopwords.txt").getLines().toSet).value

//org.apache.spark.rdd.MapPartitionsRDD
val lemmatized: RDD[Seq[String]] = plainText.mapPartitions(it => {
    val pipeline = createNLPPipeline()
    it.map { case(title, contents) =>
        plainTextToLemmas(contents, stopWords, pipeline)
    }
})

//org.apache.spark.rdd.MapPartitionsRDD
val docTermFreqs = lemmatized.map(terms => {
    val termFreqs = terms.foldLeft(new HashMap[String, Int]()) {
        (map, term) => {
            map += term -> (map.getOrElse(term, 0) + 1)
            map
        }
    }
    termFreqs
})

docTermFreqs.cache()

//org.apache.spark.rdd.ShuffledRDD
//org.apache.spark.rdd.RDD[(String, Int)]
val docFreqs = docTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _)
val numTerms = 50000
//scala.math.Ordering$$anon$9
val ordering = Ordering.by[(String, Int), Int](_._2)

//[Lscala.Tuple2;
//val topDocFreqs = docFreqs.top(numTerms)(ordering)

//topDocFreqs.saveAsObjectFile('/home/jrogers/topDocFreqs')
//val topDocFreqs = sc.objectFile('/home/jrogers/topDocFreqs')

val numDocs = docTermFreqs.count()

//val idfs = docFreqs.map{
//    case (term, count) => (term, math.log(numDocs.toDouble / count))
//}.collectAsMap()

def inverseDocumentFrequencies(docFreqs: Array[(String, Int)], numDocs: Int)
    : Map[String, Double] = {
    docFreqs.map{ case (term, count) => (term, math.log(numDocs.toDouble / count))}.toMap
}

val idfs = inverseDocumentFrequencies(docFreqs, numDocs)

val termIds = idfs.keys.zipWithIndex.toMap

val bTermIds = sc.broadcast(termIds).value
val bIdfs = sc.broadcast(idfs).value

val vecs = docTermFreqs.map(termFreqs => {
    val docTotalTerms = termFreqs.values().sum
    val termScores = termFreqs.filter {
        case (term, freq) => bTermIds.containsKey(term)
    }.map{
        case (term, freq) => (bTermIds(term),
            bIdfs(term) * termFreqs(term) / docTotalTerms)
    }.toSeq
    Vectors.sparse(bTermIds.size, termScores)
})

vecs.cache()



val mat = new RowMatrix(vecs)
val k = 10
val svd = mat.computeSVD(k, computeU=true)


//val zero = new HashMap[String, Int]()
//def merge(dfs: HashMap[String, Int], tfs: HashMap[String, Int])
    //: HashMap[String, Int] = {
    //tfs.keySet.foreach { term =>
        //dfs += term -> (dfs.getOrElse(term, 0) + 1)
    //}
    //dfs
//}
//def comb(dfs1: HashMap[String, Int], dfs2: HashMap[String, Int])
    //: HashMap[String, Int] = {
    //for ((term, count) <- dfs2) {
        //dfs1 += term -> (dfs1.getOrElse(term, 0) + count)
    //}
    //dfs1
//}
//docTermFreqs.aggregate(zero)(merge, comb)

//val docFreqs = docTermFreqs.flatMap(_.keySet).map((_, 1)).reduceByKey(_ + _)
//val numTerms = 50000
//val ordering = Ordering.by[(String, Int), Int](_._2)
//val topDocFreqs = docFreqs.top(numTerms)(ordering)

//docTermFreqs.flatMap(_.keySet).distinct().count()

