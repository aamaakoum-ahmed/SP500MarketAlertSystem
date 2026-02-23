import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

println("\n>>> SPRINT 3: DETECTION STATISTIQUE DES ANOMALIES <<<")

// 1. Ajouter la colonne Year pour grouper les statistiques
val dfWithYear = dfFinalMetrics.withColumn("Year", year(col("Date")))

// 2. Definir la fenetre de calcul par annee
val yearWindow = Window.partitionBy("Year")

// 3. Calculer les statistiques globales par annee
val dfStats = dfWithYear.withColumn("YearlyMean", avg(col("RiskScore")).over(yearWindow)).withColumn("YearlyStdDev", stddev(col("RiskScore")).over(yearWindow))

// 4. Identifier les anomalies (Outliers)
// Seuil = Moyenne + 2 * Ecart-Type

val dfAlerts = dfStats.filter(col("RiskScore") > (col("YearlyMean") + (col("YearlyStdDev") * 2))).withColumn("Severity", (col("RiskScore") - col("YearlyMean")) / col("YearlyStdDev"))

// 5. Trier par annee et severite

val finalAlerts = dfAlerts.select("Year", "Date", "Symbol", "RiskScore", "YearlyMean", "Severity").orderBy(desc("Year"), desc("Severity"))

println("\n[RESULTAT] Top 10 des Alertes de Marche detectees :")
finalAlerts.show(10)

// Sauvegarde du resultat pour le Sprint 4
finalAlerts.cache()
println("\n[OK] Detection terminee. Les alertes sont pretes pour l'export.")
