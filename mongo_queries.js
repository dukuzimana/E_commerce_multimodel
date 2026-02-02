// MongoDB Aggregation Queries

//Top 10 best-selling products
db.transactions.aggregate([
  { $unwind: "$items" },
  { $group: { _id: "$items.product_id", totalSold: { $sum: "$items.quantity" }, revenue: { $sum: "$items.subtotal" } } },
  { $sort: { totalSold: -1 } },
  { $limit: 10 }
])


// Revenue by category
db.transactions.aggregate([
  { $unwind: "$items" },
  {
    $lookup: {
      from: "products",
      localField: "items.product_id",
      foreignField: "product_id",
      as: "product"
    }
  },
  { $unwind: "$product" },
  { $group: { _id: "$product.category_id", totalRevenue: { $sum: "$items.subtotal" } } },
  { $sort: { totalRevenue: -1 } }
])

//User segmentation by purchase frequency
db.transactions.aggregate([
  { $group: { _id: "$user_id", purchaseCount: { $sum: 1 }, totalSpent: { $sum: "$total" } } },
  { $sort: { purchaseCount: -1 } }
])



