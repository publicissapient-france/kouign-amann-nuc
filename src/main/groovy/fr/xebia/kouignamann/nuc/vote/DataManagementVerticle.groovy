package fr.xebia.kouignamann.nuc.vote

import org.vertx.groovy.core.eventbus.Message
import org.vertx.groovy.platform.Verticle

class DataManagementVerticle extends Verticle {
    def logger


    def start() {
        logger = container.logger
        logger.info "Initialize handler";
        [
                "fr.xebia.kouignamann.nuc.central.processSingleVote": this.&processSingleVote,
        ].each {
            eventBusAddress, handler ->
                vertx.eventBus.registerHandler(eventBusAddress, handler)
        }

        logger.info "Done initialize handler";
    }

    def processSingleVote(Message incomingMsg) {
        logger.info("Bus <- fr.xebia.kouignamann.nuc.central.processSingleVote ${incomingMsg}")
        def outcomingMsg = [
                "nfcId": incomingMsg.body.nfcId,
                "voteTime": incomingMsg.body.voteTime,
                "note": incomingMsg.body.note,
                "hardwareUid": incomingMsg.body.hardwareUid
        ]
        logger.info("Bus -> fr.xebia.kouignamann.nuc.central.storeVote ${outcomingMsg}")
        vertx.eventBus.send("fr.xebia.kouignamann.nuc.central.storeVote", outcomingMsg)
    }
}
