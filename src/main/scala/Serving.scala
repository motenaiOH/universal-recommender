/*
 * Copyright ActionML, LLC under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * ActionML licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.actionml

import breeze.stats.mean
import breeze.stats.meanAndVariance
import breeze.stats.MeanAndVariance

import org.apache.predictionio.controller.LServing

class Serving
  extends LServing[Query, PredictedResult] {


  override
  def serve(query: Query,
            predictedResults: Seq[PredictedResult]): PredictedResult = {

    val standard: Seq[Array[ItemScore]] = query.engine.getOrElse("standard") match {
      case "standard" => {
        predictedResults.map(_.itemScores)
      }

      case "complementary" => {
        val mvList: Seq[MeanAndVariance] = predictedResults.map { pr =>
          meanAndVariance(pr.itemScores.map(_.score))
        }

        predictedResults.zipWithIndex
          .map { case (pr, i) =>
            pr.itemScores.map { is =>
              val score = if (mvList(i).stdDev == 0) {
                0
              } else {
                (is.score - mvList(i).mean) / mvList(i).stdDev
              }

              ItemScore(is.item, score)
            }
          }
      }
    }


    val combined = standard.flatten
      .groupBy(_.item)
      .mapValues(itemScores => itemScores.map(_.score).reduce(_ + _))
      .toArray
      .sortBy(_._2)(Ordering.Double.reverse)
      .take(query.num.getOrElse(6))
      .map { case (k, v) => ItemScore(k, v) }


    PredictedResult(combined)
  }
}
