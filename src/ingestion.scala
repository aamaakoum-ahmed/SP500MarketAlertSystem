import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

val sparkSession = spark
import sparkSession.implicits._

println("\n>>> SPRINT 1 : INGESTION & NETTOYAGE <<<")

// 1. Schemas
val priceSchema = StructType(Array(
  StructField("Date", TimestampType, true),
  StructField("Open", DoubleType, true),
  StructField("High", DoubleType, true),
  StructField("Low", DoubleType, true),
  StructField("Close", DoubleType, true),
  StructField("Volume", LongType, true),
  StructField("Stock Splits", DoubleType, true)
))

val pathPrices = "/home/SP500MarketAlertSystem/data/sp500_stock_price.csv"
val pathShares = "/home/SP500MarketAlertSystem/data/sharesOutstanding.csv"

// 2. Chargement (Tout sur une seule ligne pour eviter l'erreur de Reader)
val rawPrices = spark.read.option("header", "true").schema(priceSchema).csv(pathPrices)
val rawShares = spark.read.option("header", "true").option("inferSchema", "true").csv(pathShares)

// 3. Nettoyage (Verifie bien que rawPrices est bien un DataFrame maintenant)
val cleanedPrices = rawPrices.withColumn("Symbol", lit("AAPL")).withColumn("Date", col("Date").cast(DateType)).filter(col("Volume") > 0 && col("Open") > 0).drop("Stock Splits").na.drop()
val cleanedShares = rawShares.select(col("Symbol"), col("shareOutstanding").cast(DoubleType)).na.drop()

// 4. Statistiques

try {
    val countPrices = cleanedPrices.count()
    println(s"\n[OK] Lignes de prix chargees : $countPrices")
    
    println("\n[INFO] Apercu des prix :")
    cleanedPrices.show(5)
    
    println("\n[INFO] Apercu des Shares :")
    cleanedShares.show(5)
} catch {
    case e: Exception => println("\n[ERREUR] Erreur lors de l'execution : " + e.getMessage)
}

println("\n[INFO] Schema final :")
cleanedPrices.printSchema()
