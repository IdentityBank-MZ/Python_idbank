# Python project - Identity Bank data driver

[Identity Bank](https://www.identitybank.eu)

### idbank
The Identity Bank database engine is responsible for storing and securing customers data. That main component and it’s configuration decide how and where data is stored. The main design was to create a system which separates data systems from portals using our protocol and engine based on the configuration and storage architecture to fit storage around business needs. The first phase implements data separation for metadata and customers data and indexes. We have implemented the metadata at the AWS dynamo DB and all search indexes and data storage at the Postgres database. That project will be extended to adding support for multiple database types and other systems depending on security requirements or access speed.
