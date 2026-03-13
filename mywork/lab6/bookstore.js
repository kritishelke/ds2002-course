// Task 2: use database
use bookstore

// Task 3: insert first author
db.authors.insertOne({
  name: "Jane Austen",
  nationality: "British",
  bio: {
    short: "English novelist known for novels about the British landed gentry.",
    long: "Jane Austen was an English novelist whose works critique and comment upon the British landed gentry."
  }
})

// Task 4: update to add birthday
db.authors.updateOne(
  { name: "Jane Austen" },
  { $set: { birthday: "1775-12-16" } }
)

// Task 5: insert four more authors
db.authors.insertMany([
{
  name: "Charles Dickens",
  nationality: "British",
  bio: {
    short: "English writer and social critic.",
    long: "Charles Dickens created some of the world's best-known fictional characters."
  },
  birthday: "1812-02-07"
},
{
  name: "Mark Twain",
  nationality: "American",
  bio: {
    short: "American writer and humorist.",
    long: "Mark Twain was an American writer known for The Adventures of Tom Sawyer."
  },
  birthday: "1835-11-30"
},
{
  name: "Victor Hugo",
  nationality: "French",
  bio: {
    short: "French Romantic writer.",
    long: "Victor Hugo was a French novelist best known for Les Misérables."
  },
  birthday: "1802-02-26"
},
{
  name: "Haruki Murakami",
  nationality: "Japanese",
  bio: {
    short: "Japanese contemporary writer.",
    long: "Haruki Murakami is known for surreal novels like Norwegian Wood."
  },
  birthday: "1949-01-12"
}
])

// Task 6: total count
db.authors.countDocuments()

// Task 7: British authors sorted by name
db.authors.find({ nationality: "British" }).sort({ name: 1 })
