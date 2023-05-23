import scala.io.Source
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.DefaultFormats

object NBA_ETL {
  def main(args: Array[String]): Unit = {
    // Définition de l'URL de base pour les requêtes API
    val baseUrl = "https://www.balldontlie.io/api/v1"

    // Equipes cibles
    val teams = Map(
      "Phoenix Suns" -> 24,
      "Atlanta Hawks" -> 1,
      "Los Angeles Clippers" -> 13,
      "Milwaukee Bucks" -> 17
    )

    // Paramètres de pagination et de filtrage
    val page = 0
    val perPage = 100
    val startDate = "2021-01-01"
    val endDate = "2022-12-31"

    // Implicite pour les formats JSON
    implicit val formats: DefaultFormats = DefaultFormats

    // Liste pour stocker les données JSON des matchs et des statistiques des joueurs
    var gamesData = List[String]()
    var statsData = List[String]()

    // Récupération des matchs de la saison 2021-2022 pour les équipes cibles
    teams.foreach { case (teamName, teamId) =>
      val teamGamesUrl = s"$baseUrl/games?team_ids[]=$teamId&seasons[]=2021&page=$page&per_page=$perPage&start_date=$startDate&end_date=$endDate"
      val teamGamesJsonData = fetchData(teamGamesUrl)
      gamesData = gamesData :+ teamGamesJsonData
    }

    // Récupération des statistiques des matchs de la saison 2021-2022 pour les équipes cibles
    teams.foreach { case (_, teamId) =>
      val teamStatsUrl = s"$baseUrl/stats?team_ids[]=$teamId&seasons[]=2021&page=$page&per_page=$perPage&start_date=$startDate&end_date=$endDate"
      val teamStatsJsonData = fetchData(teamStatsUrl)
      statsData = statsData :+ teamStatsJsonData
    }

    // Enregistrement des données des matchs dans des fichiers JSON
    teams.keys.zip(gamesData).foreach { case (teamName, jsonData) =>
      val filename = s"$teamName-matches.json"
      saveDataToFile(jsonData, filename)
    }

    // Enregistrement des données des statistiques des matchs dans des fichiers JSON
    teams.keys.zip(statsData).foreach { case (teamName, jsonData) =>
      val filename = s"$teamName-stats.json"
      saveDataToFile(jsonData, filename)
    }
  }

  def fetchData(url: String): String = {
    val response = Source.fromURL(url).mkString
    response
  }

  def saveDataToFile(data: String, filename: String): Unit = {
    val file = new java.io.PrintWriter(filename)
    try {
      file.write(data)
    } finally {
      file.close()
    }
  }
}


