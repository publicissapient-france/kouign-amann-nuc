package fr.xebia.kouignamann.nuc.db

import com.sleepycat.persist.model.Entity
import com.sleepycat.persist.model.PrimaryKey

@Entity
class Vote {
    // The primary key must be unique in the database.
    @PrimaryKey(sequence="Seq_Vote")
    private long voteUid;

    private String nfcId
    private int voteTime
    private int note
}
