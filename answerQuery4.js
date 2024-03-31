import { MongoClient } from 'mongodb';

const url = 'mongodb://localhost:27017/';
const dbName = 'hw5Tweets';
const collectionName = 'tweets'; 

async function findTopTweetersByAverageRetweets() {
    const client = await MongoClient.connect(url);
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const pipeline = [
        {
            $group: {
                _id: "$user.screen_name",
                tweetCount: { $sum: 1 },
                averageRetweetCount: { $avg: "$retweet_count" }
            }
        },
        {
            $match: { tweetCount: { $gt: 3 } }
        },
        { $sort: { "averageRetweetCount": -1 } },
        { $limit: 10 },
        { $project: { _id: 0, screenName: "$_id", averageRetweetCount: 1 } }
    ];

    try {
        const cursor = collection.aggregate(pipeline);
        const result = await cursor.toArray();

        console.log("Top 10 users by average retweets, after tweeting more than 3 times:");
        console.log(result);
    } catch (e) {
        console.error("An error occurred while fetching the top screen names:", e);
    } finally {
        await client.close();
    }
}

findTopTweetersByAverageRetweets();
