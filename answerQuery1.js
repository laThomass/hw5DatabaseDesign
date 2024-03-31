import { MongoClient } from 'mongodb';

const url = 'mongodb://localhost:27017/';
const dbName = 'hw5Tweets'; 
const collectionName = 'tweets'; 

async function findTweets() {
    const client = await MongoClient.connect(url);
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

    const filter = {
        "retweeted_status": { "$exists": false }, 
        "in_reply_to_status_id": null,
        "in_reply_to_user_id": null,
        "in_reply_to_screen_name": null 
    };

    try {
        const cursor = collection.find(filter);
        const result = await cursor.toArray();

        console.log("I've found", result.length, "tweets that are not retweets or replies.");
    } catch (e) {
        console.error("An error occurred while fetching data:", e);
    } finally {
        await client.close();
    }
}

findTweets();
