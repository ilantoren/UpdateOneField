import com.mongodb.client.model.Aggregates.limit
import com.mongodb.client.model.Aggregates.match
import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.Filters.gt
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.client.model.Updates.set
import com.mongodb.kotlin.client.coroutine.MongoClient
import com.mongodb.kotlin.client.coroutine.MongoCollection
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.collectIndexed
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.yield
import org.bson.Document
import org.bson.types.ObjectId
import java.math.BigInteger
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.logging.log4j.kotlin.logger

val mongoClient = MongoClient.create("mongodb://localhost")
val collection: MongoCollection<Document> = mongoClient.getDatabase("gdelt2").getCollection("gdelt2")
val logger = logger("MainKt")
/**
 * Basic Kotlin script/program  that ties in aggregation with
 * co-routines.   First reads, then writes because as the app
 * is reading the collection will have locks.  Trying to read and write
 * simultaneously from/to the same collection will lose the cursor.
 * Read speed is much faster than write speed
 */
fun main() {


    // create a mutable list to hold the interim results
    val data = mutableListOf<Pair<ObjectId, Int>>()

    val running = AtomicBoolean(true)

    runBlocking {


        val action = launch {
            getData().buffer(10000).onCompletion { running.set(false) }.collectIndexed { x, it ->

                if (x % 1000 == 0) {
                    logger.info("At record $x")
                }
                val hex = it.getString("hex")
                val id = it.getObjectId("_id")

                /**
                 * This operation isn't possible from within an aggregation
                 * https://www.mongodb.com/docs/v6.0/reference/operator/aggregation/toInt/
                 */
                val decimal = BigInteger(hex, 16)
                val key = decimal.mod(BigInteger.valueOf(43L))

                // Add the data to the buffer. After all the reads process the writes
                data.add(Pair(id, key.toInt()))
            }
        }
        while (running.get()) {
            delay(3000)
            logger.info("Waiting to finish")
        }

        action.join().run {

            launch {
                delay(5000L)
                logger.info(" Start writing updates ")
                while (running.get() || data.isEmpty()) {
                    delay(1000L)
                    yield()
                }
                updateCollection(data, collection)
            }
        }

    }
}


/**
 * Basic aggregation pipeline to extract the _id as a hex string
 * This was created in Compass then exported as Java
 * The IDE converted it into Kotlin (plus some manual editing)
 */
val pipeline = listOf(
    match(gt("Day", 20230901)),
    limit(1000000),
    set(
        "hex",
        Document("\$toString", "\$_id")
    )
)


fun getData(): Flow<Document> {
    logger.info("Starting the aggregation")
    return collection.aggregate(pipeline).allowDiskUse(true).batchSize(1000)
}

fun updateCollection(data: MutableList<Pair<ObjectId, Int>>, collection: MongoCollection<Document>) {
    val buffer = mutableListOf<UpdateOneModel<Document>>()
    data.forEach {
        buffer.add(UpdateOneModel(eq("_id", it.first), set("key", it.second)))
        // At intermittent steps send updates to server as a batch update
        if (buffer.size > 5000) {
            val buf = buffer.take(5000)
            buffer.removeAll(buf)
            updateBatch(buf, collection)
        }
    }
    // Number of records isn't a multiple of 5,000 so catch the remaining
    updateBatch(buffer, collection)
}

/**
 * Avoid code duplication
 */
fun updateBatch(buf: List<UpdateOneModel<Document>>, collection: MongoCollection<Document>) = runBlocking {
    collection.bulkWrite(buf).also {
        logger.info("${it.modifiedCount} records updated")
        println("${it.modifiedCount} records updated")
    }


}



