import com.mongodb.client.model.Filters.eq
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.client.model.Updates.set
import com.mongodb.reactivestreams.client.MongoClients
import com.mongodb.reactivestreams.client.MongoCollection
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.runBlocking
import org.bson.Document
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import java.math.BigInteger

/**
 * Basic Kotlin script/program  that ties in aggregation with
 * co-routines.   First reads, then writes because as the app
 * is reading the collection will have locks.  Trying to read and write
 * simultaneously from/to the same collection will lose the cursor.
 * Read speed is much faster than write speed
 */
fun main(args: Array<String>) {

    /**
     * Basic aggregation pipeline to extract the _id as a hex string
     * This was created in Compass then exported as Java
     * The IDE converted it into Kotlin (plus some manual editing)
     */
    val pipeline = listOf<Bson>(
        Document(
            "\$addFields",
            Document(
                "hex",
                Document("\$toString", "\$_id")
            )
        ),
        Document(
            "\$project",
            Document("hex", 1L)
        ),
    )

    // create a mutable list to hold the interim results
    val data = mutableListOf<Pair<ObjectId, Int>>()

    val mongoClient = MongoClients.create("mongodb://localhost")
    val collection = mongoClient.getDatabase("gdelt").getCollection("data")

    runBlocking {
        collection.aggregate(pipeline).allowDiskUse(true).batchSize(1000).asFlow().collect {

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

        updateCollection(data, collection)

    }

}

suspend fun updateCollection(data: MutableList<Pair<ObjectId, Int>>, collection: MongoCollection<Document>) {
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
    collection.bulkWrite(buf).asFlow().collect {
        println(it)
    }
}
