package controllers

import play.api._
import play.api.mvc._
import org.apache.spark._

object Application extends Controller {

  def index = Action {
    Ok(views.html.index("the kekus amongus"))
  }

}