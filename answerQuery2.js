import { MongoClient } from 'mongodb';

const url = 'mongodb://localhost:27017/';
const dbName = 'ieeevisTweets'; 
const collectionName = 'tweets'; 

async function findTopScreenNamesByFollowers() {
    const client = await MongoClient.connect(url);
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const pipeline = [
        {
            $group: {
                _id: "$user.screen_name", 
                followersCount: { $max: "$user.followers_count" }, 
                screenName: { $first: "$user.screen_name" } 
            }
        },
        { $sort: { "followersCount": -1 } }, 
        { $limit: 10 }, 
        { $project: { _id: 0, screenName: 1, followersCount: 1 } }
    ];

    try {
        const cursor = collection.aggregate(pipeline);
        const result = await cursor.toArray();

        console.log("Top 10 screen names by number of followers:");
        console.log(result);
    } catch (e) {
        console.error("An error occurred while fetching the top screen names:", e);
    } finally {
        await client.close();
    }
}

findTopScreenNamesByFollowers();
