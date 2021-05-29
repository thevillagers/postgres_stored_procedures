# postgres_stored_procedures
A collection of stored procedures for Postgres


All of the procedures are created in the schema I use, vre_funcs. Many of the procedures use each other within them, so if you want the procedures in a different schema, I'd recommend just doing a global replace off the bat to make sure you don't miss anything.

The procedures also make extensive use of the cstore_fdw foreign data wrapper. I probably need to update this to use the Citus extension. But here is the repo for cstore_fdw/Citus: https://github.com/citusdata/cstore_fdw

You'll likely need cstore_fdw installed for anything to work without some minor modifications, but I'll soon update to Citus myself and reflect that change here

Currently this is likely to only be compatible with cstore_fdw (I haven't read how the syntax changes for the Citus extension)


