import org.apache.spark.sql.functions._

println("\n>>> SPRINT 2 : CALCUL DES METRIQUES FINANCIERES <<<")

//1 Calcul du RVI (Volatilite Relative en %)
// Formule : ((High - Low) / Open) * 100

val dfWithMetrics = cleanedPrices.withColumn("RVI", ((col("High") - col("Low")) / col("Open")) * 100).withColumn("LogVolume", log10(col("Volume") + 1))


//2 Jointure avec les Shares Outstanding (uniquement si Symbol est present)

val dfEnriched = dfWithMetrics.join(cleanedShares, "Symbol")

// Calcul du Turnover Ratio (Volume / Actions en circulation)
//3 Et du Score de Risque (RVI * LogVolume)

val dfFinalMetrics = dfEnriched.withColumn("TurnoverRatio", col("Volume") / col("shareOutstanding")).withColumn("RiskScore", col("RVI") * col("LogVolume"))

// Apercu des resultats

println("\n[RESULTAT] Top 5 des scores de risque calcules :")
dfFinalMetrics.select("Date", "Symbol", "RVI", "TurnoverRatio", "RiskScore").orderBy(desc("RiskScore")).show(5)

// Mise en cache pour le Sprint 3
dfFinalMetrics.cache()
println("\n[OK] Metriques calculees et mises en cache.")
