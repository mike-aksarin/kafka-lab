package example.connector

object VersionUtil {

  def version = {
    try {
      classOf[GitHubSourceConnector].getPackage.getImplementationVersion
    } catch {
      case _: Exception => "0.0.0.0"
    }
  }

}
