import org.apache.spark.sql.{Column, DataFrame}

// función para describir la cantidad de registros nulos, vacíos o NaN y no nulos de una entidad entegando también el porcentaje de nulos respecto del total
def perfil_entidad(df: DataFrame): DataFrame = {
    var listFinal: List[(String, Long, Long, Long, Double)] = List()

    for (col <- df.columns) {
        val nulls = df.filter(df(col).isNull || df(col).isNaN || df(col) === "").count()
        val total = df.count()
        val notNulls = total - nulls
        var perOfNulls = (nulls.toDouble / total.toDouble) * 100.0
        perOfNulls = Math.round(perOfNulls * 100.0) / 100.0
        
        var list = (col, nulls, notNulls, total, perOfNulls)
        listFinal = listFinal :+ list
    }
    return listFinal.toDF("atributo", "numero_nulls", "numero_no_nulls", "total_registros", "porcentaje_nulls")
}