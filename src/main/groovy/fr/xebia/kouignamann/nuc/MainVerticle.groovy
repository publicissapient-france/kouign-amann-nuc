package fr.xebia.kouignamann.nuc

import fr.xebia.kouignamann.nuc.db.PersistenceVerticle
import fr.xebia.kouignamann.nuc.vote.DataManagementVerticle
import org.vertx.groovy.core.http.HttpServer
import org.vertx.groovy.core.http.HttpServerRequest
import org.vertx.groovy.core.http.RouteMatcher
import org.vertx.groovy.platform.Verticle
import org.vertx.java.core.json.impl.Json
import org.vertx.java.core.logging.Logger

class MainVerticle extends Verticle {
    Logger logger

    def start() {
        logger = container.logger
        logger.info "Starting"
        container.deployWorkerVerticle('groovy:' + DataManagementVerticle.class.name, container.config, 1)
        container.deployWorkerVerticle('groovy:' + PersistenceVerticle.class.name, container.config, 1)

        startHttpServer(container.config.listen, container.config.port)
    }

    private HttpServer startHttpServer(String listeningInterface, Integer listeningPort) {
        logger.info "Start -> starting HTTP Server. Listening on: ${listeningInterface}:${listeningPort}"

        HttpServer server = vertx.createHttpServer()
        server.requestHandler(buildRestRoutes().asClosure())
        server.listen(listeningPort, listeningInterface)
    }

    /**
     * Rest routes building.
     * @return @RouteMatcher
     */
    private RouteMatcher buildRestRoutes() {
        RouteMatcher matcher = new RouteMatcher()

        matcher.get('/aggregate/note') { final HttpServerRequest serverRequest ->
            logger.info "HTTP -> ${serverRequest}"

            def msg = [status: "Next"]
            logger.info("Bus -> fr.xebia.kouignamann.nuc.central.getNoteRepartition ${msg}")
            vertx.eventBus.send("fr.xebia.kouignamann.nuc.central.getNoteRepartition", msg) {message ->
                logger.info "Process -> fr.xebia.kouignamann.nuc.central.getNoteRepartition replied ${message.body.status}"

                serverRequest.response.putHeader('Content-Type', 'application/json')
                serverRequest.response.chunked = true
                serverRequest.response.end(Json.encode([result: message.body.result]))
            }

        }

        matcher.get('/printers') {
            final HttpServerRequest serverRequest ->
                logger.info "HTTP -> ${serverRequest}"
                try {
                    serverRequest.pause()
                    serverRequest.expectMultiPart = true
                    serverRequest.endHandler {

                        vertx.eventBus.send(Intent.LIST_PRINTERS.getValue(), [:]) { printResponse ->

                            try {
                                serverRequest.response.putHeader('Content-Type', 'application/json')
                                serverRequest.response.chunked = true
                                serverRequest.response.end(Json.encode([printers: printResponse.body.printers]))

                            } catch (Exception e) {
                                e.printStackTrace()
                                respondHttpError(serverRequest)
                            }
                        }
                    }
                    serverRequest.resume()
                } catch (Exception e) {
                    e.printStackTrace()
                    respondHttpError(serverRequest)
                }
        }

        matcher.post('/print') { final HttpServerRequest serverRequest ->
            try {
                logger.info "HTTP -> ${serverRequest}"
                serverRequest.pause()
                serverRequest.expectMultiPart = true
                serverRequest.endHandler {

                    vertx.eventBus.send(Intent.PRINT_CARD.getValue(), pickPrintParamsFrom(serverRequest.formAttributes)) { printResponse ->

                        try {
                            if (printResponse.body.error) {
                                return respondHttpError(serverRequest, printResponse.body.error)
                            }

                            serverRequest.response.putHeader('Content-Type', 'application/json')
                            serverRequest.response.chunked = true
                            serverRequest.response.end(Json.encode([
                                    cardUid: printResponse.body.cardUid,
                                    userId: printResponse.body.userId
                            ]))
                        } catch (Exception e) {
                            e.printStackTrace()
                            respondHttpError(serverRequest)
                        }
                    }
                }
                serverRequest.resume()
            } catch (Exception e) {
                e.printStackTrace()
                respondHttpError(serverRequest)
            }
        }

        return matcher
    }

    void respondHttpError(HttpServerRequest request, String message = 'Server Error') {

        String response = Json.encode([error: message])

        request.response.statusCode = 500
        request.response.end(response)

        logger.info "HTTP <- ${response}"

    }
}
