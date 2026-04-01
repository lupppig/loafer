// MongoDB Seed Script: ~50,000 documents for ETL demo
// Run: mongosh mongodb://loafer:loafer@localhost:27017/admin bin/setup_mongo.js

const dbName = "loafer_source";
db = db.getSiblingDB(dbName);

// Drop existing collections
db.analytics.drop();
db.users.drop();

// Seed users
const firstNames = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Iris", "Jack"];
const lastNames = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"];
const countries = ["US", "UK", "CA", "DE", "FR", "JP", "AU", "BR", "IN", "MX"];
const tiers = ["free", "basic", "premium", "enterprise"];

const userDocs = [];
for (let i = 1; i <= 20000; i++) {
    const fn = firstNames[i % 10];
    const ln = lastNames[Math.floor(i / 10) % 10];
    userDocs.push({
        name: fn + " " + ln,
        email: (fn + "." + ln + i + "@example.com").toLowerCase(),
        country: countries[i % 10],
        tier: tiers[i % 4],
        age: 18 + (i % 60),
        signup_date: new Date(Date.now() - Math.random() * 730 * 24 * 60 * 60 * 1000),
        preferences: {
            newsletter: i % 3 === 0,
            notifications: i % 2 === 0,
            theme: i % 2 === 0 ? "dark" : "light"
        },
        tags: ["user", i % 5 === 0 ? "vip" : "regular"]
    });
}
db.users.insertMany(userDocs);
print("Inserted " + db.users.countDocuments() + " users");

// Seed analytics events
const eventTypes = ["page_view", "click", "scroll", "form_submit", "purchase", "logout", "login", "search"];
const pages = ["/home", "/products", "/cart", "/checkout", "/profile", "/settings", "/search", "/help"];
const devices = ["desktop", "mobile", "tablet"];
const browsers = ["Chrome", "Firefox", "Safari", "Edge", "Opera"];

const batchSize = 1000;
let totalInserted = 0;

for (let batch = 0; batch < 30; batch++) {
    const docs = [];
    for (let i = 0; i < batchSize; i++) {
        const idx = batch * batchSize + i;
        docs.push({
            user_id: 1 + (idx % 20000),
            event_type: eventTypes[idx % 8],
            page: pages[idx % 8],
            duration_seconds: Math.floor(Math.random() * 600),
            device: devices[idx % 3],
            browser: browsers[idx % 5],
            occurred_at: new Date(Date.now() - Math.random() * 90 * 24 * 60 * 60 * 1000),
            metadata: {
                session_id: "sess_" + idx,
                version: 1 + (idx % 5),
                referrer: ["google", "direct", "email", "social", "ads"][idx % 5]
            },
            geo: {
                lat: (Math.random() * 180 - 90).toFixed(4),
                lng: (Math.random() * 360 - 180).toFixed(4)
            }
        });
    }
    db.analytics.insertMany(docs);
    totalInserted += docs.length;
    print("Batch " + (batch + 1) + "/30 inserted. Total: " + totalInserted);
}

print("Inserted " + db.analytics.countDocuments() + " analytics events");

// Create indexes
db.users.createIndex({ email: 1 });
db.users.createIndex({ country: 1 });
db.analytics.createIndex({ user_id: 1 });
db.analytics.createIndex({ event_type: 1 });

print("MongoDB seed complete!");
