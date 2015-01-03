package controllers

import play.api._
import play.api.mvc._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import utils.PagesViewed

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits._

object Application extends Controller {

  def index = Action {
    Future{PagesViewed.PagesViewedUtil}
    Ok(views.html.index("I'm laughing " ))
  }

}