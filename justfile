app_name   := "meteoswiss-nwp-arraylake"
ingest     := "meteoswiss_nwp_arraylake.ingest"

# Community-tier managed repo holding rolling-window analysis snapshots in
# zarr groups (ch2-ml, ch2-sfc, ch1-sfc, kenda-sfc). See ingest.py.
arraylake_repo := "meteoswiss-nwp-anl"

# deployment

deploy:
    modal deploy -m {{ingest}}

stop:
    modal app stop {{app_name}}

# One-time: create the managed repo. Requires a `meteoswiss-arraylake`
# Modal secret with ARRAYLAKE_TOKEN, ARRAYLAKE_ORG, ARRAYLAKE_REPO.
create-repo:
    modal run -m {{ingest}} --action create --name {{arraylake_repo}}

# Manual ingestion of a single product (ch2-ml, ch2-sfc, ch1-sfc, kenda-sfc).
ingest product:
    modal run -m {{ingest}} --action ingest --product {{product}}

# Manual expiration + garbage collection fallback (prefer the ArrayLake UI).
gc:
    modal run -m {{ingest}} --action gc
