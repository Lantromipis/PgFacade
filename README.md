# PgFacade — cloud-native Docker All-In-One solution for high availability Postgres

### What is PgFacade

PgFacade is a one and only solution you will need to achieve Postgres High Availability. Key features of PgFacade:
1. Docker integration (PgFacade is like a Kubernetes operator, but works in Docker)
2. Fully autonomous solution with autoscaling of Postgres and PgFacade nodes
3. Automatic failover
4. Raft consensus algorithm which prevents split-brain
5. REST API for Postgres and PgFacade control
6. Integrated connection pool
7. Integrated archiver which periodically creates backups and continuously streams WAL to storage
8. S3 integration

### How to install 
See [installation notes](.docs/installation-notes.md)