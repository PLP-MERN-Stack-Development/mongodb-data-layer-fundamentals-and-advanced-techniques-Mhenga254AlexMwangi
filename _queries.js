// queries.js - Advanced MongoDB queries for the PLP Bookstore

const { MongoClient } = require('mongodb');

const uri = "mongodb+srv://alexnjagi123_db_user:0114329940Ba@mern-stack.xuzrza6.mongodb.net/plp_bookstore?retryWrites=true&w=majority&appName=Mern-stack";
const dbName = "plp_bookstore";
const collectionName = "books";

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("Connected to MongoDB Atlas");

    const db = client.db(dbName);
    const books = db.collection(collectionName);

    // 1. Find all books in a specific genre (Fiction)
    console.log("\nBooks in the Fiction genre:");
    console.log(await books.find({ genre: "Fiction" }).toArray());

    // 2. Find books published after 1950
    console.log("\nBooks published after 1950:");
    console.log(await books.find({ published_year: { $gt: 1950 } }).toArray());

    // 3. Find books by a specific author
    console.log("\nBooks by George Orwell:");
    console.log(await books.find({ author: "George Orwell" }).toArray());

    // 4. Update the price of a specific book
    console.log("\nUpdating price of '1984'...");
    await books.updateOne({ title: "1984" }, { $set: { price: 15.99 } });
    console.log("Updated '1984' successfully.");

    // 5. Delete a book by title
    console.log("\nDeleting 'Animal Farm'...");
    await books.deleteOne({ title: "Animal Farm" });
    console.log("'Animal Farm' deleted successfully.");

    // 6. Find books that are in stock and published after 2010
    console.log("\nBooks in stock and published after 2010:");
    console.log(await books.find({ in_stock: true, published_year: { $gt: 2010 } }).toArray());

    // 7. Projection: return only title, author, and price
    console.log("\nProjection: Only title, author, and price:");
    console.log(await books.find({}, { projection: { title: 1, author: 1, price: 1, _id: 0 } }).toArray());

    // 8. Sorting by price
    console.log("\nBooks sorted by price (ascending):");
    console.log(await books.find().sort({ price: 1 }).toArray());

    console.log("\nBooks sorted by price (descending):");
    console.log(await books.find().sort({ price: -1 }).toArray());

    // 9. Pagination (5 books per page)
    console.log("\nPage 1:");
    console.log(await books.find().limit(5).toArray());

    console.log("\nPage 2:");
    console.log(await books.find().skip(5).limit(5).toArray());

  } catch (err) {
    console.error("Error:", err);
  } finally {
    await client.close();
    console.log("\nConnection closed.");
  }
}

runQueries().catch(console.error);
// Aggregation Pipelines
async function runAggregations() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("\nRunning Aggregations...");

    const db = client.db(dbName);
    const books = db.collection(collectionName);

    // 1. Average price by genre
    console.log("\nAverage price by genre:");
    const avgPrice = await books.aggregate([
      { $group: { _id: "$genre", averagePrice: { $avg: "$price" } } }
    ]).toArray();
    console.log(avgPrice);

    // 2. Author with most books
    console.log("\nAuthor with the most books:");
    const topAuthor = await books.aggregate([
      { $group: { _id: "$author", totalBooks: { $sum: 1 } } },
      { $sort: { totalBooks: -1 } },
      { $limit: 1 }
    ]).toArray();
    console.log(topAuthor);

    // 3. Group books by decade
    console.log("\nBooks grouped by publication decade:");
    const byDecade = await books.aggregate([
      {
        $group: {
          _id: { $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10] },
          totalBooks: { $sum: 1 }
        }
      },
      { $sort: { _id: 1 } }
    ]).toArray();
    console.log(byDecade);

  } catch (err) {
    console.error("Error in aggregation:", err);
  } finally {
    await client.close();
    console.log("\nAggregation complete. Connection closed.");
  }
}

runAggregations().catch(console.error);
// Indexing demonstration
async function createIndexes() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log("\nCreating indexes...");

    const db = client.db(dbName);
    const books = db.collection(collectionName);

    // 1. Create index on title
    await books.createIndex({ title: 1 });
    console.log("Index created on title.");

    // 2. Compound index on author and published_year
    await books.createIndex({ author: 1, published_year: 1 });
    console.log("Compound index created on author and published_year.");

    // 3. Explain query performance
    console.log("\nExplain query performance:");
    const explainResult = await books.find({ title: "1984" }).explain("executionStats");
    console.log(JSON.stringify(explainResult.executionStats, null, 2));

  } catch (err) {
    console.error("Indexing error:", err);
  } finally {
    await client.close();
    console.log("Indexing process completed. Connection closed.");
  }
}

createIndexes().catch(console.error);
