import org.apache.spark.sql.functions._

println("\n>>> SPRINT 2 : CALCUL DES METRIQUES FINANCIERES <<<")

// 1. Calcul du RVI (Volatilite Relative en %)
// Formule : ((High - Low) / Open) * 100

val dfWithMetrics = cleanedPrices.withColumn("RVI", ((col("High") - col("Low")) / col("Open")) * 100).withColumn("LogVolume", log10(col("Volume") + 1))


// 3. Jointure avec les Shares Outstanding (uniquement si Symbol est present)

val dfEnriched = dfWithMetrics.join(cleanedShares, "Symbol")

// 4. Calcul du Turnover Ratio (Volume / Actions en circulation)
// Et du Score de Risque (RVI * LogVolume)

val dfFinalMetrics = dfEnriched.withColumn("TurnoverRatio", col("Volume") / col("shareOutstanding")).withColumn("RiskScore", col("RVI") * col("LogVolume"))

// 5. Apercu des resultats

println("\n[RESULTAT] Top 5 des scores de risque calcules :")
dfFinalMetrics.select("Date", "Symbol", "RVI", "TurnoverRatio", "RiskScore").orderBy(desc("RiskScore")).show(5)

// Mise en cache pour le Sprint 3
dfFinalMetrics.cache()
println("\n[OK] Metriques calculees et mises en cache.")
