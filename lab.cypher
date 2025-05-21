LOAD CSV WITH HEADERS FROM 'file:///creditcard_fraud_graph_dataset.csv' AS row

// Create or match sender and receiver users
MERGE (sender:User {name: row.sender})
MERGE (receiver:User {name: row.receiver})

// Create or match the card and link it to sender
MERGE (card:Card {card_number: row.card_number})
SET card.card_type = row.card_type,
    card.card_limit = toFloat(row.card_limit)
MERGE (sender)-[:OWNS]->(card)

// Create or match the bank
MERGE (bank:Bank {name: row.bank})

// Create or match the device
MERGE (device:Device {device_id: row.device})

// Create the transaction node with all properties
MERGE (txn:Transaction {tx_id: row.txn_id})
SET txn.amount = toFloat(row.amount),
    txn.timestamp = datetime(row.timestamp),
    txn.is_fraud = CASE row.fraud WHEN '1' THEN true ELSE false END

// Create relationships between transaction and other entities
MERGE (card)-[:USED_IN]->(txn)
MERGE (txn)-[:TO]->(receiver)
MERGE (txn)-[:PROCESSED_BY]->(bank)
MERGE (txn)-[:MADE_ON]->(device);
//LOADING THE CSV FILE above

//Show Fraud Transactions and Connections
MATCH (txn:Transaction {fraud: 1})-[r]->(n)
RETURN txn, r, n
LIMIT 10

//Devices Used in Multiple Fraud Transactions
MATCH (txn:Transaction {fraud: 1})-[:LOGGED_IN_FROM]->(d:Device)
RETURN d.id AS Device_ID, count(*) AS Fraud_Count
ORDER BY Fraud_Count DESC
LIMIT 10

//Detect Basic Fraud Ring (Triangular Pattern)
MATCH p = (a:Person)<-[:SENT_FROM]-(t1:Transaction)-[:SENT_TO]->(b:Person),
           (b)<-[:SENT_FROM]-(t2:Transaction)-[:SENT_TO]->(c:Person),
           (c)<-[:SENT_FROM]-(t3:Transaction)-[:SENT_TO]->(a:Person)
WHERE t1.fraud = 1 AND t2.fraud = 1 AND t3.fraud = 1
RETURN p
LIMIT 5

//Highest Fraud Receiver
MATCH (txn:Transaction {fraud: 1})-[:SENT_TO]->(p:Person)
RETURN p.name AS Receiver, count(*) AS Fraud_Received
ORDER BY Fraud_Received DESC
LIMIT 5

//Full Fraud Network Graph View
MATCH (txn:Transaction {fraud: 1})--(n)
RETURN txn, n
LIMIT 50

// Devices Used in Fraudulent Transactions
MATCH (txn:Transaction {fraud: 1})-[:LOGGED_IN_FROM]->(d:Device)
RETURN d.id AS Device_ID, count(*) AS Fraud_Txns
ORDER BY Fraud_Txns DESC

//To check properties of each node
MATCH (txn:Transaction)--(n)
RETURN txn, n
LIMIT 50

 //Devices Involved in Multiple Fraud Transactions
MATCH (txn:Transaction {fraud: 1})-[:LOGGED_IN_FROM]->(d:Device)
WITH d, count(txn) AS fraud_count
WHERE fraud_count > 1
MATCH (t:Transaction {fraud: 1})-[:LOGGED_IN_FROM]->(d)
RETURN t, d
LIMIT 50

//Fraud Senders Who Used the Same Device
MATCH (p1:Person)<-[:SENT_FROM]-(t1:Transaction {fraud: 1})-[:LOGGED_IN_FROM]->(d:Device)<-[:LOGGED_IN_FROM]-(t2:Transaction {fraud: 1})-[:SENT_FROM]->(p2:Person)
WHERE p1 <> p2
RETURN DISTINCT p1, t1, d, t2, p2
LIMIT 50

//Person involved in multiple fraud transactions
MATCH (p:Person)<-[:SENT_FROM]-(t:Transaction {fraud: 1})
WITH p, count(t) AS fraud_txns
WHERE fraud_txns > 1
MATCH (p)<-[:SENT_FROM]-(t:Transaction {fraud: 1})
RETURN p, t
LIMIT 50

//To find person involved in fraud transaction and device
MATCH (person:Person)<-[:SENT_FROM]-(txn:Transaction {fraud: 1})-[:LOGGED_IN_FROM]->(device:Device)
RETURN person.name AS Person, device.id AS Device_ID, txn.id AS Txn_ID
LIMIT 50

// Detect shared card usage across different users (potential fraud rings)
MATCH (u1:User)-[:OWNS]->(c:Card)<-[:OWNS]-(u2:User)
WHERE u1 <> u2
RETURN u1, u2, c
LIMIT 10;


//Users involved in multiple fraud transactions 
MATCH (u:User)<-[:TO]-(t:Transaction)
WHERE t.is_fraud = true
WITH u, COUNT(t) AS fraud_count
WHERE fraud_count > 1
RETURN u, fraud_count
LIMIT 10;

//All Fraud transactions and their receivers
MATCH (t:Transaction {is_fraud: true})-[:TO]->(u:User)
RETURN t, u
LIMIT 25;

//Fraud Transactions Made by Accounts Sharing the Same Device
MATCH (u1:User)-[:OWNS]->(c1:Card)-[:USED_IN]->(t1:Transaction)-[:MADE_ON]->(d:Device),
      (u2:User)-[:OWNS]->(c2:Card)-[:USED_IN]->(t2:Transaction)-[:MADE_ON]->(d)
WHERE u1 <> u2 AND t1.is_fraud = true AND t2.is_fraud = true
RETURN u1, u2, d, t1, t2
LIMIT 10;

//Bank Fraud detection 
MATCH (c:Card)-[]->(t:Transaction)-[:PROCESSED_BY]->(b:Bank)
WHERE t.fraud = "1" OR t.fraud = 1 OR t.is_fraud = true
RETURN b.name AS Bank, COUNT(t) AS Fraud_Tx_Count
ORDER BY Fraud_Tx_Count DESC
LIMIT 5;

// Visualize all fraud transactions and their direct connections
MATCH (t:Transaction {is_fraud: true})--(n)
RETURN t, n
LIMIT 50;

// Loop Detection
MATCH p = (a:User)<-[:TO]-(t1:Transaction)-[:TO]->(b:User),
           (b)<-[:TO]-(t2:Transaction)-[:TO]->(a)
WHERE t1.is_fraud = true AND t2.is_fraud = true
RETURN p
LIMIT 5;

// Devices that are connected to multiple fraud transactions
MATCH (t:Transaction {is_fraud: true})-[:MADE_ON]->(d:Device)
WITH d, COUNT(t) AS fraud_count
WHERE fraud_count > 1
RETURN d.device_id AS Device, fraud_count
ORDER BY fraud_count DESC
LIMIT 10;












