package fr.xebia.kouignamann.nuc.db

import com.sleepycat.je.DatabaseException
import com.sleepycat.je.Environment
import com.sleepycat.je.EnvironmentConfig
import com.sleepycat.persist.EntityStore
import com.sleepycat.persist.StoreConfig
import org.vertx.groovy.core.eventbus.Message
import org.vertx.groovy.platform.Verticle

class PersistenceVerticle extends Verticle {
    static File envHome = new File("/tmp")

    private Environment devEnv
    private EntityStore store

    def voteIdx

    def logger

    def start() {
        boolean readOnly = false
        logger = container.logger
        logger.info "Start -> Initializing DB"
        [
                "fr.xebia.kouignamann.nuc.central.storeVote": this.&storeVote,
                "fr.xebia.kouignamann.nuc.central.getNoteRepartition": this.&getNoteRepartition,
        ].each { eventBusAddress, handler ->
            vertx.eventBus.registerHandler(eventBusAddress, handler)
        }

        try {
            EnvironmentConfig myEnvConfig = new EnvironmentConfig()
            StoreConfig storeConfig = new StoreConfig()

            myEnvConfig.setAllowCreate(!readOnly)
            storeConfig.setAllowCreate(!readOnly)

            // Open the environment and entity store
            devEnv = new Environment(envHome, myEnvConfig)
            store = new EntityStore(devEnv, "EntityStore", storeConfig)

            // Create the index
            voteIdx = store.getPrimaryIndex(Long.class, Vote.class);

            // Fake datas
            def vote = new Vote()
            vote.note = 1
            vote.nfcId = "00 00 00 00"
            vote.voteTime = new Date().getTime()

            voteIdx.put(vote)

            vote = new Vote()
            vote.note = 2
            vote.nfcId = "00 00 00 00"
            vote.voteTime = new Date().getTime()

            voteIdx.put(vote)

            vote = new Vote()
            vote.note = 2
            vote.nfcId = "00 00 00 00"
            vote.voteTime = new Date().getTime()

            voteIdx.put(vote)

        } catch (DatabaseException dbe) {
            logger.error "Start -> Error opening environment and store: ${dbe.toString()}"
        }
        logger.info "Start -> Done initializing DB"
    }

    def storeVote(Message message) {
        logger.info("Bus <- fr.xebia.kouignamann.nuc.central.storeVote ${message}")
        Vote vote = new Vote()
        vote.nfcId = message.body.nfcId
        vote.voteTime = message.body.voteTime
        vote.note = message.body.note

        voteIdx.put(vote)
        logger.info("Process -> Storing '${vote}'")

        message.reply([
                status: "OK"
        ])
    }

    def getNoteRepartition(Message message) {
        logger.info("Bus <- fr.xebia.kouignamann.nuc.central.getNoteRepartition ${message}")
        Map result = [:]
        def cursor = voteIdx.entities()
        for (Vote vote : cursor) {
            logger.info "Process -> vote ${vote.voteUid} note ${vote.note}"
            def value = result.get(vote.note) ?: 0
            logger.info "Process -> value ${value}"
            result.put(vote.note, value + 1)
        }
        logger.info "Process -> result ${result}"

        message.reply([
                status: "OK",
                result: result
        ])
    }

    def stop() {
        if (store) {
            try {
                store.close();
            } catch (DatabaseException dbe) {
                logger.error "Stop -> Error closing store: ${dbe.toString()}"
            }
        }

        if (devEnv) {
            try {
                // Finally, close environment.
                devEnv.close();
            } catch (DatabaseException dbe) {
                logger.error "Stop -> Error closing MyDbEnv: ${dbe.toString()} "
            }
        }
    }
}
