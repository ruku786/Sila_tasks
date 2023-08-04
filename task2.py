#2.Design a simple data model that represents the data downloaded in task 1 along
#with the result of your operations on the data.

#CREATE keyspace data_model with replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
#cqlsh> use data_model;
#cqlsh:data_model> CREATE TABLE IF NOT EXISTS products (
#              ...   product_id UUID PRIMARY KEY,
#              ...   name TEXT,
#              ...   description TEXT,
#              ...   price DECIMAL,
#              ...   stock_quantity INT
#              ... );

#3.Write Python code to insert, update, and retrieve data from the NoSQL database
#(Kindly consider bulk operations as well).
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel

def __init__(self):
   self.cluster = Cluster(contact_points=["scylla-node1", "scylla-node2", "scylla-node3"])
   self.session = self.cluster.connect(keyspace="catalog")
   self.session.default_consistency_level = ConsistencyLevel.QUORUM

   def add_mutant(self, first_name, last_name, address, picture_location):
       print(f"\nAdding {first_name} {last_name}...")
       self.session.execute(f"INSERT INTO mutant_data (first_name, last_name, address, picture_location) "
                            f"VALUES ('{first_name}','{last_name}','{address}','{picture_location}')")
       print("Added.\n")


def delete_mutant(self, first_name, last_name):
    print(f"\nDeleting {first_name} {last_name}...")
    self.session.execute(f"DELETE FROM mutant_data WHERE last_name = '{last_name}' and first_name = '{first_name}'")
    print("Deleted.\n")


if __name__ == "__main__":
    app = App()
    app.show_mutant_data()
    app.add_mutant(first_name='Peter', last_name='Parker',
                   address='1515 Main St', picture_location='http://www.facebook.com/Peter-Parker/')
    app.show_mutant_data()
    app.delete_mutant(first_name="Peter", last_name="Parker")
    app.show_mutant_data()
    app.stop()