import { MongoClient } from 'mongodb';

const url = 'mongodb://localhost:27017/';
const dbName = 'hw5Tweets'; 

async function createUserCollectionAndReferenceTweets() {
  const client = new MongoClient(url);
  try {
    await client.connect();
    console.log('Connected correctly to server');
    const db = client.db(dbName);

    const usersPipeline = [
      { $group: { _id: "$user.id", userDoc: { $first: "$user" } } },
      { $replaceRoot: { newRoot: "$userDoc" } },
      { $out: "Users" }
    ];
    await db.collection('tweets').aggregate(usersPipeline).toArray();
    console.log('Users collection created');

    const tweetsPipeline = [
      { 
        $project: { 
          created_at: 1, 
          text: 1, 
          entities: 1,
          user_id: { $toString: "$user.id" }
        } 
      },
      { $out: "Tweets_Only" }
    ];
    await db.collection('tweets').aggregate(tweetsPipeline).toArray();
    console.log('Tweets_Only collection created');

    const usersCount = await db.collection('Users').countDocuments();
    console.log(`Users collection contains ${usersCount} documents.`);

    const tweetsCount = await db.collection('Tweets_Only').countDocuments();
    console.log(`Tweets_Only collection contains ${tweetsCount} documents.`);

    const sampleUser = await db.collection('Users').findOne();
    console.log('Sample user document:', JSON.stringify(sampleUser, null, 2));

    const sampleTweet = await db.collection('Tweets_Only').findOne();
    console.log('Sample tweet document:', JSON.stringify(sampleTweet, null, 2));

  } catch (err) {
    console.error('An error occurred:', err);
  } finally {
    await client.close();
    console.log('Connection closed');
  }
}

createUserCollectionAndReferenceTweets();
