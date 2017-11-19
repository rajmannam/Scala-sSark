# Scala-Spark-TopNProducts-Function


val l = ("Bike & Skate Shop", Iterable("933,42,Nike VR_S Covert Driver,,179.99,http://images.acmesports.sports/Nike+VR_S+Covert+Driver", 
"934,42,Callaway X Hot Driver,,0.0,http://images.acmesports.sports/Callaway+X+Hot+Driver", 
"935,42,TaylorMade RocketBallz Stage 2 Driver,,169.99,http://images.acmesports.sports/TaylorMade+RocketBallz+Stage+2+Driver", 
"936,42,Cleveland Golf Classic XL Driver,,119.99,http://images.acmesports.sports/Cleveland+Golf+Classic+XL+Driver", 
"937,42,Cobra AMP Cell Driver - Orange,,169.99,http://images.acmesports.sports/Cobra+AMP+Cell+Driver+-+Orange"))

def topNProducts(rec: (String, Iterable[String]), topN: Int): Iterable[(String, String)] = {
  rec._2.toList.sortBy(k => -k.split(",")(4).toFloat).take(topN).map(r => (rec._1, r))
}

val products = sc.textFile("/public/retail_db/products")
val productsFiltered = products.filter(rec => rec.split(",")(4) != "")
val productsMap = productsFiltered.map(rec => (rec.split(",")(1).toInt, rec))

val categories = sc.textFile("/public/retail_db/categories").
  map(rec => (rec.split(",")(0).toInt, rec.split(",")(2)))

val productsJoin = productsMap.
  join(categories).
  map(rec => (rec._2._2, rec._2._1))
val productsGroupByCategory = productsJoin.groupByKey()
productsGroupByCategory.
  flatMap(rec => topNProducts(rec, 3)).
  collect().
  foreach(println)
