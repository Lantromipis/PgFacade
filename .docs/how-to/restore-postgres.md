# How to restore postgres from backup

## Start PgFacade in recovery mode
PgFacade will start in recovery mode in two cases:
1. Previous container with Primary can not be found AND can not be obtained from Raft leader
2. There is no Primary configuration stored in pod AND configuration can not obtained from Raft leader

So to start PgFacade in recovery mode you can:
1. Start new PgFacade when all other instances are turned off (So new instance will not be able to get info from Raft)
2. Remove container with previous primary, stop all PgFacade instances and start new PgFacade

In recovery mode execute HTTP request to special REST API. 

If you have chosen option to use restored Postgres as new primary, then restart PgFacade after recovery completed. PgFacade will automatically scale Postgres and scale itself.

### Restore Postgres to latest available version
Request options. 

1. saveRestoredInstanceAsNewPrimary will mark restored Postgres as new primary. 

```
curl -L 'localhost:8080/api/v1/recovery/restore-postgres/to-latest' \
-H 'Content-Type: application/json' \
-d '{
"saveRestoredInstanceAsNewPrimary" : "true"
}'
```