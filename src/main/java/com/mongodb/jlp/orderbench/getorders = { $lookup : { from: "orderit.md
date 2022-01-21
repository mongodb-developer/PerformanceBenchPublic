getorders = { $lookup : { from: "orderitem", 
let : { customerId: "$customerId", orderId: "$orderId"}, 
pipeline: [{$match:{$expr:{$and:[{$eq:["$customerId","$$customerId"]},
                                 {$eq:["$orderId","$$orderId"]}]}}}],
as:"orders"}}

getinvoices = { $lookup : { from: "invoice", 
let : { customerId: "$customerId", orderId: "$orderId"}, 
pipeline: [{$match:{$expr:{$and:[{$eq:["$customerId","$$customerId"]},
                                 {$eq:["$orderId","$$orderId"]}]}}}],
as:"invoice"}}

getshipments  = { $lookup : { from: "shipment", 
let : { customerId: "$customerId", orderId: "$orderId"}, 
pipeline: [{$match:{$expr:{$and:[{$eq:["$customerId","$$customerId"]},
                                 {$eq:["$orderId","$$orderId"]}]}}}],
as:"shipments"}}

db.order.aggregate([{$match:{type:"order"}},getorders,getinvoices,getshipments,{$limit:10},{$out:"embedded"}])