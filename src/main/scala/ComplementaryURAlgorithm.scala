import com.actionml.{PreparedData, RecsModels, URAlgorithm, URAlgorithmParams}
import org.apache.predictionio.data.storage.NullModel
import org.apache.spark.SparkContext

class ComplementaryURAlgorithm(ap : URAlgorithmParams)
  extends URAlgorithm(ap){

  override
  def train(sc: SparkContext, data: PreparedData): NullModel = {

    recsModel match {
      case RecsModels.All => calcAll(data)(sc)
      case RecsModels.CF  => calcAll(data, calcPopular = false)(sc)
      case RecsModels.BF  => calcPop(data)(sc)
      // error, throw an exception
      case unknownRecsModel =>
        throw new IllegalArgumentException(
          s"""
             |Bad algorithm param recsModel=[$unknownRecsModel] in engine definition params, possibly a bad json value.
             |Use one of the available parameter values ($recsModel).""".stripMargin)
    }
  }



}
