import { MongoClient } from 'mongodb';

const url = 'mongodb://localhost:27017/';
const dbName = 'hw5Tweets'; 
const collectionName = 'tweets'; 

async function findTopTweeter() {
    const client = await MongoClient.connect(url);
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const pipeline = [
        {
            $group: {
                _id: "$user.screen_name", 
                tweetCount: { $sum: 1 }, 
            }
        },
        { $sort: { "tweetCount": -1 } }, 
        { $limit: 1 }, 
        { $project: { _id: 0, screenName: "$_id", tweetCount: 1 } } 
    ];
    

    try {
        const cursor = collection.aggregate(pipeline);
        const result = await cursor.toArray();

        console.log("User with the most tweets:");
        console.log(result);
    } catch (e) {
        console.error("An error occurred while fetching the top screen names:", e);
    } finally {
        await client.close();
    }
}

findTopTweeter();
