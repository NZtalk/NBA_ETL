import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_date, struct}


object Transform {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NBA ETL")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val matchsPaths = Seq(
      "/home/ubuntu/FirstProject/NBA_ETL/Atlanta Hawks-matches.json",
      "/home/ubuntu/FirstProject/NBA_ETL/Los Angeles Clippers-matches.json",
      "/home/ubuntu/FirstProject/NBA_ETL/Milwaukee Bucks-matches.json",
      "/home/ubuntu/FirstProject/NBA_ETL/Phoenix Suns-matches.json"
    )

    val statsPaths = Seq(
      "/home/ubuntu/FirstProject/NBA_ETL/Atlanta Hawks-stats.json",
      "/home/ubuntu/FirstProject/NBA_ETL/Los Angeles Clippers-stats.json",
      "/home/ubuntu/FirstProject/NBA_ETL/Milwaukee Bucks-stats.json",
      "/home/ubuntu/FirstProject/NBA_ETL/Phoenix Suns-stats.json"
    )

    val matchColumns = Seq("id", "date", "home_team", "home_team_score", "visitor_team", "visitor_team_score")
    val statsColumns = Seq("id", "ast", "blk", "dreb", "fg3_pct", "fg3a", "fg3m", "fg_pct", "fga", "fgm",
      "ft_pct", "fta", "ftm", "game", "min", "oreb", "pf", "player", "pts", "reb", "stl", "team", "turnover")

    val matchsDFs = matchsPaths.map(path => spark.read.format("json").load(path).withColumn("match_index", monotonically_increasing_id()))
    val statsDFs = statsPaths.map(path => spark.read.format("json").load(path).withColumn("stats_index", monotonically_increasing_id()))

    // Affichage des DataFrames avant transformation
    matchsDFs.foreach(_.show())
    statsDFs.foreach(_.show())

val matchsTransformedDF = matchsDFs.reduce(_.union(_))
  .withColumn("match_data", explode(col("data")))
  .select(
    col("match_data.id").as("match_id"),
    to_date(col("match_data.date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").as("date"),
    col("match_data.home_team.full_name").as("home_team"),
    col("match_data.visitor_team.full_name").as("visitor_team"),
    col("match_data.home_team_score").as("home_team_score"),
    col("match_data.visitor_team_score").as("visitor_team_score")
  )

val statsTransformedDF = statsDFs.reduce(_.union(_))
  .withColumn("stats_data", explode(col("data")))
  .select(
    col("stats_data.id").as("stats_id"),
    col("stats_data.ast").as("ast"),
    col("stats_data.blk").as("blocks"),
    col("stats_data.dreb").as("defensive_rebounds"),
    col("stats_data.fg3_pct").as("three_point_pct"),
    col("stats_data.fg_pct").as("field_goal_pct"),
    col("stats_data.ft_pct").as("free_throw_pct"),
    col("stats_data.min").as("min"),
    col("stats_data.oreb").as("oreb"),
    col("stats_data.pf").as("pf"),
    col("stats_data.player.id").as("player_id"),
    col("stats_data.player.first_name").as("first_name"),
    col("stats_data.player.last_name").as("last_name"),
    col("stats_data.player.position").as("position"),
    col("stats_data.player.team_id").as("team_id"),
    col("stats_data.reb").as("reb"),
    col("stats_data.stl").as("stl"),
    col("stats_data.turnover").as("turnover"),
    col("stats_data.game.id").as("game_id"),
    to_date(col("stats_data.game.date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").as("game_date")
  )
  
    // Affichage des DataFrames après transformation
    matchsTransformedDF.show()
    statsTransformedDF.show()

val joinedDF = matchsTransformedDF.join(statsTransformedDF,
  matchsTransformedDF("date") === statsTransformedDF("game_date"),
  "left"
).select(
  matchsTransformedDF("match_id"),
  matchsTransformedDF("date"),
  matchsTransformedDF("home_team"),
  matchsTransformedDF("visitor_team"),
  matchsTransformedDF("home_team_score"),
  matchsTransformedDF("visitor_team_score"),
  statsTransformedDF("ast"),
  statsTransformedDF("blocks"),
  statsTransformedDF("defensive_rebounds"),
  statsTransformedDF("three_point_pct"),
  statsTransformedDF("field_goal_pct"),
  statsTransformedDF("free_throw_pct"),
  statsTransformedDF("min"),
  statsTransformedDF("oreb"),
  statsTransformedDF("pf"),
  concat(col("first_name"), lit(" "), col("last_name")).as("player"),
  statsTransformedDF("player_id"),
  statsTransformedDF("team_id"),
  statsTransformedDF("reb"),
  statsTransformedDF("stl"),
  statsTransformedDF("turnover")
)



    // Affichage du DataFrame après jointure et transformation supplémentaire
    joinedDF.show()

val outputDir = "/home/ubuntu/FirstProject/NBA_ETL/output"
val outputPath = s"$outputDir/final.csv"

// Créer le répertoire de sortie s'il n'existe pas
new java.io.File(outputDir).mkdirs()

joinedDF.write
  .format("csv")
  .option("header", "true")
  .mode("overwrite")
  .save(outputPath)

    spark.stop()
  }
}





