1. kreiranje docker-compose i onda docker-compose up -d 

u docker desktopu, pristup u cannandra container i otvaranje terminala
komanda:
	cqlsh
otvara kasandru i tu se kucaju komande

kreiranje tabele i podataka:
-kreiranje kispejsa(baze)
	CREATE KEYSPACE IF NOT EXISTS bikesdb WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

opciono USE bikesdb

-- Create a table
CREATE TABLE IF NOT EXISTS bikesdb.bikes (timestamp timestamp, avg(Duration) int);


-- Create a keyspace
CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

-- Create a table
CREATE TABLE IF NOT EXISTS demo.bikes (
timestamp timestamp,
average_duration int
);

-- Insert some data
INSERT INTO store.shopping_cart
(timestamp, average_duration)
VALUES (toTimeStamp(now()), 20);
