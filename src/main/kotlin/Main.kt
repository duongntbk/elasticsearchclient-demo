import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.SearchResponse
import co.elastic.clients.json.JsonData
import java.io.FileInputStream
import java.util.Properties

fun main(args: Array<String>) {
    val (login, password, fingerprint) = loadConfigKeys()

    ElasticsearchClientWrapper(login, password, fingerprint).use { client ->
        val helloWorldRequest = buildHelloWorldQuery()
        val helloWorldResponse = client.search(helloWorldRequest, Footballer::class.java)
        println("Age: ${helloWorldResponse.hits().hits().first().source()?.age}")

        val positionRequest = buildPositionQuery()
        val positionResponse = client.search(positionRequest, Footballer::class.java)
        printResults(positionResponse)

        val searchRequest3 = buildQueryByClass()
        val response3 = client.search(searchRequest3, Footballer::class.java)
        printResults(response3)

        val searchRequest4 = buildQueryByDsl()
        val response4 = client.search(searchRequest4, Footballer::class.java)
        printResults(response4)

        val searchRequest5 = buildQueryAssorted()
        val response5 = client.search(searchRequest5, Footballer::class.java)
        printResults(response5)
    }
}

private fun buildHelloWorldQuery(): SearchRequest = SearchRequest.of { s -> s
    .index("footballer")
    .query { q -> q
        .match { t -> t
            .field("name")
            .query("Rashford")
        }
    }
}

private fun buildPositionQuery(): SearchRequest {
    val term1 = TermQuery.Builder().field("position").value("lw").build()._toQuery()
    val term2 = TermQuery.Builder().field("position").value("rw").boost(2F).build()._toQuery()
    val boolQuery = BoolQuery.Builder()
        .should(term1, term2)
        .build()
        ._toQuery()

    return SearchRequest.Builder()
        .index("footballer")
        .query(boolQuery)
        .build()
}

private fun buildQueryByClass(): SearchRequest {
    val positionTerm1 = TermQuery.Builder().field("position").value("rw").boost(2F).build()._toQuery()
    val positionTerm2 = TermQuery.Builder().field("position").value("lw").build()._toQuery()
    val positionQuery = BoolQuery.Builder()
        .should(positionTerm1, positionTerm2)
        .build()
        ._toQuery()

    val ageRange = RangeQuery.Builder().field("age").lte(JsonData.of(23)).build()._toQuery()
    val salaryRange = RangeQuery.Builder().field("salary").lte(JsonData.of(200)).boost(2F).build()._toQuery()
    val ageAndSalaryQuery = BoolQuery.Builder()
        .should(ageRange, salaryRange)
        .build()
        ._toQuery()

    val query = BoolQuery.Builder()
        .must(positionQuery, ageAndSalaryQuery)
        .build()
        ._toQuery()

    return SearchRequest.Builder()
        .index("footballer")
        .query(query)
        .build()
}

private fun buildQueryByDsl(): SearchRequest = SearchRequest.of { s -> s
    .index("footballer")
    .query { q -> q
        .bool { b -> b
            .must { m -> m
                .bool { b -> b
                    .should { s -> s
                        .term { t -> t
                            .field("position")
                            .value("rw")
                            .boost(2F)
                        }
                    }
                    .should { s -> s
                        .term { t -> t
                            .field("position")
                            .value("lw")
                        }
                    }
                }
            }
            .must { m -> m
                .bool { b -> b
                    .should { s -> s
                        .range { r -> r
                            .field("age")
                            .lte(JsonData.of(23))
                        }
                    }
                    .should { s -> s
                        .range { r -> r
                            .field("salary")
                            .lte(JsonData.of(200))
                            .boost(2F)
                        }
                    }
                }
            }
        }
    }
}

private fun buildQueryAssorted(): SearchRequest {
    val positionTerm1 = TermQuery.Builder().field("position").value("rw").boost(2F).build()._toQuery()
    val positionTerm2 = TermQuery.Builder().field("position").value("lw").build()._toQuery()
    val ageRange = RangeQuery.Builder().field("age").lte(JsonData.of(23)).build()._toQuery()
    val salaryRange = RangeQuery.Builder().field("salary").lte(JsonData.of(200)).boost(2F).build()._toQuery()

    return SearchRequest.of { s -> s
        .index("footballer")
        .query { q -> q
            .bool { b -> b
                .must { m -> m
                    .bool { b -> b
                        .should(positionTerm1)
                        .should(positionTerm2)
                    }
                }
                .must { m -> m
                    .bool { b -> b
                        .should(ageRange)
                        .should(salaryRange)
                    }
                }
            }
        }
    }
}

private fun printResults(response: SearchResponse<Footballer>) {
    println("==========================================================")
    println("Hits: ${response.hits().total()?.value()}")
    for (hit in response.hits().hits()) {
        println("Name: ${hit.source()?.name}, Score: ${hit.score()}")
    }
}

private fun loadConfigKeys(): Triple<String, String, String> {
    val properties = Properties()

    val localFile = "src/main/resources/application-env-local.yml"
    val file = if (java.io.File(localFile).exists()) localFile else "src/main/resources/application.yml"

    FileInputStream(file).use { inputStream ->
        properties.load(inputStream)
    }

    val login = properties.getProperty("login")
    val password = properties.getProperty("password")
    val fingerprint = properties.getProperty("fingerprint")
    return Triple(login, password, fingerprint)
}
