package Lab11.Datawriter

import org.apache.spark.sql.DataFrame

object Datawriter {
  def writedf(dt:DataFrame,path: String)= {
    dt.write.format("csv").save(path);
  }
}