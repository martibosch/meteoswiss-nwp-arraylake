"""Ingest MeteoSwiss OGD forecasts into an ArrayLake-managed icechunk repo.

Targets the **ArrayLake Community tier** (10 GB managed storage, 1 private
repo, 50 compute credits/month). A single managed repo holds multiple zarr
groups — one per product configuration — each with its own key-variable subset
and rolling window over ``ref_time``. ArrayLake runs expiration + garbage
collection on configured repos automatically, so the Modal job only has to
append the new snapshot and prune the dataset to the trailing window; physical
space reclamation is handled by ArrayLake's managed compute.

Products ingested (all analysis, ``horizon=P0DT0H``):

* ``ch2-ml``   — ICON-CH2-EPS multi-level   (2.1 km, every 6 h)
* ``ch2-sfc``  — ICON-CH2-EPS surface        (2.1 km, every 6 h)
* ``ch1-sfc``  — ICON-CH1-EPS surface        (1 km,   every 3 h)
* ``kenda-sfc`` — KENDA-CH1 analysis surface (1 km,  every 1 h)

E5 (KENDA-CH1 analysis) is included as a surface-only store; E4 (local
forecast, point-based) is intentionally out of scope — it is a separate store
shape (station dimension, different collection) best added as a follow-up.

.. note::

   The OGD fetching helpers below (``_probe_ref_time``, ``_fetch_snapshot``,
   ``_sanitize_attrs``) and the ``COLLECTIONS`` / ``*_VARS`` constants are
   **duplicated** from the sibling ``meteoswiss-nwp-store`` repository
   (``meteoswiss_nwp_store.ingest_icon_ogd``). This package is kept
   self-contained on purpose: it is the public, Marketplace-facing variant with
   a different audience, storage backend (ArrayLake-managed vs. Tigris) and
   release cadence, and it should not gain a dependency on the archive repo.
   If the shared logic grows, lift it into a tiny common package both repos
   depend on rather than re-coupling them.

Deployment:

    modal deploy -m meteoswiss_nwp_arraylake.ingest

One-time repo creation (run locally, authenticated via ``arraylake login``):

    modal run -m meteoswiss_nwp_arraylake.ingest::create_repo
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone

import modal

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Modal app
# ---------------------------------------------------------------------------

APP_NAME = "meteoswiss-nwp-arraylake"

app = modal.App(APP_NAME)

# A Modal secret named "meteoswiss-arraylake" must define:
#   ARRAYLAKE_TOKEN=<personal access token>
#   ARRAYLAKE_ORG=<org name, e.g. martibosch>
#   ARRAYLAKE_REPO=<repo name, e.g. meteoswiss-nwp-anl>
arraylake_secret = modal.Secret.from_name("meteoswiss-arraylake")

image = (
    modal.Image.micromamba(python_version="3.12")
    .micromamba_install("eccodes==2.47.0", "ca-certificates", channels=["conda-forge"])
    .uv_pip_install(
        "icechunk>=2.2.0,<3",
        "meteodata-lab",
        "xarray",
        "zarr",
        "arraylake>=1.3.0,<2",
        "certifi",
    )
    .env({
        "EARTHKIT_DATA_CACHE_POLICY": "temporary",
        "SSL_CERT_FILE": "/opt/conda/lib/python3.12/site-packages/certifi/cacert.pem",
    })
)

# ---------------------------------------------------------------------------
# Product configuration
# ---------------------------------------------------------------------------

#: MeteoSwiss OGD STAC collection ids (see meteodatalab.ogd_api.Collection).
COLLECTIONS = {
    "ch1": "ogd-forecasting-icon-ch1",
    "ch2": "ogd-forecasting-icon-ch2",
    "kenda": "ogd-analysis-kenda-ch1",
}

#: Key-variable subsets chosen to keep each snapshot small enough that a
#: meaningful rolling window fits the Community tier's 10 GB managed-storage
#: cap. Surface variables carry a ``z`` coordinate (height / pressure level)
#: which is preserved; multi-level variables keep the full model-level axis.
SINGLE_LEVEL_VARS = ["T_2M", "TD_2M", "U_10M", "V_10M", "PMSL", "TOT_PREC"]
MULTI_LEVEL_VARS = ["T", "U", "V"]

#: One zarr group per product: (group path, collection, level, variables,
#: window length in ref_time steps, cron schedule).
PRODUCTS = {
    "ch2-ml": dict(
        group="ch2-ml",
        collection=COLLECTIONS["ch2"],
        level="ml",
        variables=MULTI_LEVEL_VARS,
        window=6,  # 6 h cadence -> 36 h window
        schedule="15 1,7,13,19 * * *",
    ),
    "ch2-sfc": dict(
        group="ch2-sfc",
        collection=COLLECTIONS["ch2"],
        level="sfc",
        variables=SINGLE_LEVEL_VARS,
        window=6,
        schedule="20 1,7,13,19 * * *",
    ),
    "ch1-sfc": dict(
        group="ch1-sfc",
        collection=COLLECTIONS["ch1"],
        level="sfc",
        variables=SINGLE_LEVEL_VARS,
        window=8,  # 3 h cadence -> 24 h window
        schedule="25 0,3,6,9,12,15,18,21 * * *",
    ),
    "kenda-sfc": dict(
        group="kenda-sfc",
        collection=COLLECTIONS["kenda"],
        level="sfc",
        variables=SINGLE_LEVEL_VARS,
        window=24,  # hourly cadence -> 24 h window
        schedule="35 * * * *",
    ),
}

#: Default repo name; override via the ``ARRAYLAKE_REPO`` Modal secret.
DEFAULT_REPO = "meteoswiss-nwp-anl"


# ---------------------------------------------------------------------------
# OGD snapshot fetching (runs inside Modal container)
#
# NOTE: duplicated from meteoswiss-nwp-store.ingest_icon_ogd; see the module
# docstring. Changes here should be mirrored there (or extracted to a shared
# package).
# ---------------------------------------------------------------------------


def _probe_ref_time(
    collection: str,
    variable: str,
    reference_datetime: str = "latest",
    horizon: str = "P0DT0H",
) -> str | None:
    """Fetch one variable to resolve ``ref_time`` without pulling all data."""
    from datetime import datetime, timezone

    from meteodatalab import ogd_api

    req = ogd_api.Request(
        collection=collection,
        variable=variable,
        reference_datetime=reference_datetime,
        perturbed=False,
        horizon=horizon,
    )
    try:
        da = ogd_api.get_from_ogd(req)
    except Exception as exc:  # pragma: no cover - network/API errors
        log.warning("Could not probe ref_time: %s", exc)
        return None

    if "ref_time" in da.coords:
        val = da.coords["ref_time"].values.ravel()[0]
        dt = datetime.fromisoformat(str(val)).replace(tzinfo=timezone.utc)
        return dt.isoformat()
    return None


def _fetch_snapshot(
    collection: str,
    variables: Iterable[str],
    reference_datetime: str = "latest",
    horizon: str = "P0DT0H",
):
    """Fetch ``variables`` for one snapshot, return an :class:`xr.Dataset`."""
    import xarray as xr
    from meteodatalab import ogd_api

    arrays: dict[str, xr.DataArray] = {}
    for var in variables:
        req = ogd_api.Request(
            collection=collection,
            variable=var,
            reference_datetime=reference_datetime,
            perturbed=False,
            horizon=horizon,
        )
        try:
            da = ogd_api.get_from_ogd(req)
            arrays[var] = da
            log.info("Fetched %s", var)
        except Exception as exc:  # pragma: no cover - per-variable failures
            log.warning("Could not fetch %s: %s", var, exc)

    if not arrays:
        return None

    ds = xr.Dataset(arrays)

    # Drop the size-1 eps dimension — implied by perturbed=False.
    if "eps" in ds.dims and ds.sizes["eps"] == 1:
        ds = ds.squeeze("eps", drop=True)

    # Drop valid_time — it is derivable as ref_time + lead_time.
    if "valid_time" in ds.coords:
        ds = ds.drop_vars("valid_time")

    # Drop size-1 lead_time=0 — implied by the analysis (anl) product type.
    if (
        "lead_time" in ds.dims
        and ds.sizes["lead_time"] == 1
        and ds.coords["lead_time"].values[0] == 0
    ):
        ds = ds.squeeze("lead_time", drop=True)

    # Ensure ref_time is a dimension (not a scalar) for zarr append_dim.
    if "ref_time" in ds.coords and "ref_time" not in ds.dims:
        ds = ds.expand_dims("ref_time")

    # Drop string/char variables — incompatible with the Zarr V3 spec.
    str_vars = [v for v in ds.variables if ds[v].dtype.kind in ("S", "U", "O")]
    if str_vars:
        log.info("Dropping non-numeric variables: %s", str_vars)
        ds = ds.drop_vars(str_vars)

    return ds


def _sanitize_attrs(attrs: dict) -> dict:
    """Strip non-JSON-serializable attributes (e.g. meteodatalab WrappedMetadata)."""
    import json

    result = {}
    for k, v in attrs.items():
        try:
            json.dumps(v)
            result[k] = v
        except (TypeError, ValueError):
            pass
    return result


# ---------------------------------------------------------------------------
# ArrayLake-managed store access
# ---------------------------------------------------------------------------


def _open_writable():
    """Open the ArrayLake repo's ``main`` branch for writing.

    Returns ``(session, store)`` where ``store`` is the underlying icechunk
    zarr store. Authenticates with the ``ARRAYLAKE_TOKEN`` secret and resolves
    the repo from ``ARRAYLAKE_ORG`` / ``ARRAYLAKE_REPO``.
    """
    import arraylake

    client = arraylake.Client(token=os.environ["ARRAYLAKE_TOKEN"])
    repo = client.get_repo(
        f"{os.environ['ARRAYLAKE_ORG']}/{os.environ.get('ARRAYLAKE_REPO', DEFAULT_REPO)}"
    )
    session = repo.writable_session("main")
    return session, session.store


def _existing_ref_times(store, group: str):
    """Return the existing ``ref_time`` values for ``group``, or ``None``."""
    import xarray as xr
    import zarr

    try:
        group_store = zarr.open_group(store, path=group, mode="r")
        import numpy as np

        return np.asarray(
            xr.open_zarr(group_store, consolidated=False).ref_time.values,
            dtype="datetime64[ns]",
        )
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Ingestion entry point
# ---------------------------------------------------------------------------


def _ingest_product(product_key: str) -> None:
    """Fetch the latest snapshot for ``product_key`` and append to its group.

    The new snapshot is appended *after* pruning the existing group to its
    trailing window, then the pruned history and the new snapshot are written
    together in a single ``to_zarr(mode="w")``. This avoids holding two
    writable handles to the same group and keeps each commit bounded by the
    window.
    """
    import xarray as xr
    import zarr

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )

    product = PRODUCTS[product_key]
    group = product["group"]
    collection = product["collection"]
    variables = product["variables"]
    window = product["window"]

    session, store = _open_writable()

    # Probe ref_time with a single cheap request before fetching all variables.
    log.info("Probing ref_time for %s (%s)", product_key, collection)
    ref_time = _probe_ref_time(collection, variables[0])
    if ref_time is None:
        log.error("Could not resolve ref_time for %s — aborting.", product_key)
        return

    ref_dt = datetime.fromisoformat(ref_time).replace(tzinfo=None)
    existing = _existing_ref_times(store, group)
    if existing is not None and ref_dt in existing:
        log.info("ref_time %s already in %s — skipping.", ref_time, group)
        return

    log.info("Fetching %s snapshot for %s", product_key, ref_time)
    new = _fetch_snapshot(collection, variables, reference_datetime=ref_time)
    if new is None:
        log.error("No data fetched for %s — aborting.", product_key)
        return
    new = new.assign_attrs(_sanitize_attrs(new.attrs))
    for var in list(new.data_vars) + list(new.coords):
        new[var].attrs = _sanitize_attrs(new[var].attrs)

    # Combine the pruned history (trailing window-1 ref_times) with the new
    # snapshot, then write once. ``max(window-1, 0)`` leaves room for the new
    # ref_time so the group holds exactly ``window`` after this commit.
    keep = max(window - 1, 0)
    try:
        old = xr.open_zarr(
            zarr.open_group(store, path=group, mode="r"), consolidated=False
        )
    except Exception:
        old = None

    if old is not None and old.sizes["ref_time"] > 0:
        pruned = old.isel(ref_time=slice(max(old.sizes["ref_time"] - keep, 0), None))
        combined = xr.concat([pruned, new], dim="ref_time")
        oldest_dropped = (
            str(old.ref_time.values[0])[:19] if old.sizes["ref_time"] > keep else None
        )
    else:
        combined = new
        oldest_dropped = None

    combined.to_zarr(
        store, group=group, mode="w", consolidated=False
    )

    msg = f"Ingested {product_key} {ref_time}"
    if oldest_dropped is not None:
        msg += f"; pruned to window={window} (dropped {oldest_dropped})"
    snapshot = session.commit(msg)
    log.info("%s [snapshot %s]", msg, snapshot[:12])


# ---------------------------------------------------------------------------
# Scheduled functions — one per product, off-peak minute offsets
# ---------------------------------------------------------------------------


@app.function(
    image=image,
    secrets=[arraylake_secret],
    timeout=600,
    schedule=modal.Cron(PRODUCTS["ch2-ml"]["schedule"]),
)
def ingest_ch2_ml() -> None:
    """Ingest ICON-CH2-EPS multi-level (every 6 h)."""
    _ingest_product("ch2-ml")


@app.function(
    image=image,
    secrets=[arraylake_secret],
    timeout=600,
    schedule=modal.Cron(PRODUCTS["ch2-sfc"]["schedule"]),
)
def ingest_ch2_sfc() -> None:
    """Ingest ICON-CH2-EPS surface (every 6 h)."""
    _ingest_product("ch2-sfc")


@app.function(
    image=image,
    secrets=[arraylake_secret],
    timeout=600,
    schedule=modal.Cron(PRODUCTS["ch1-sfc"]["schedule"]),
)
def ingest_ch1_sfc() -> None:
    """Ingest ICON-CH1-EPS surface (every 3 h)."""
    _ingest_product("ch1-sfc")


@app.function(
    image=image,
    secrets=[arraylake_secret],
    timeout=600,
    schedule=modal.Cron(PRODUCTS["kenda-sfc"]["schedule"]),
)
def ingest_kenda_sfc() -> None:
    """Ingest KENDA-CH1 analysis surface (every 1 h)."""
    _ingest_product("kenda-sfc")


@app.function(image=image, secrets=[arraylake_secret], timeout=600)
def ingest_once(product: str) -> None:
    """One-shot ingestion of any product key in :data:`PRODUCTS`."""
    if product not in PRODUCTS:
        raise ValueError(
            f"Unknown product {product!r}; expected one of {list(PRODUCTS)}"
        )
    _ingest_product(product)


# ---------------------------------------------------------------------------
# One-time repo creation (run locally after `arraylake login`)
# ---------------------------------------------------------------------------


@app.function(image=image, secrets=[arraylake_secret], timeout=300)
def create_repo(name: str = DEFAULT_REPO) -> str:
    """Create the managed ArrayLake repo if it does not already exist.

    Uses the organization's default managed bucket (no BYOB), consistent with
    the Community tier's 10 GB managed storage.
    """
    import arraylake

    client = arraylake.Client(token=os.environ["ARRAYLAKE_TOKEN"])
    org = os.environ["ARRAYLAKE_ORG"]
    full = f"{org}/{name}"
    try:
        client.get_repo(full)
        log.info("Repo %s already exists.", full)
    except Exception:
        client.create_repo(
            full,
            description=(
                "Rolling window of MeteoSwiss OGD analysis snapshots "
                "(ICON-CH1/CH2 sfc, ICON-CH2 ml, KENDA-CH1 sfc). "
                "Community-tier managed storage."
            ),
            metadata={
                "type": ["nwp", "forecast", "analysis"],
                "source": "MeteoSwiss OGD",
                "tier": "community",
            },
        )
        log.info("Created repo %s", full)
    return full


@app.function(image=image, secrets=[arraylake_secret], timeout=300)
def expire_and_gc(older_than_hours: int = 72) -> dict:
    """Manually expire old snapshots and garbage-collect unreferenced chunks.

    This is a fallback for the **automatic** expiration + GC that ArrayLake
    runs on configured repos using its own managed compute. Prefer enabling
    those policies in the ArrayLake UI; this function exists for ad-hoc
    cleanup. It is best-effort: if the ArrayLake repo wrapper does not expose
    the underlying :class:`icechunk.Repository`, it returns guidance instead
    of guessing at private attributes.
    """
    import arraylake

    client = arraylake.Client(token=os.environ["ARRAYLAKE_TOKEN"])
    repo = client.get_repo(
        f"{os.environ['ARRAYLAKE_ORG']}/"
        f"{os.environ.get('ARRAYLAKE_REPO', DEFAULT_REPO)}"
    )

    # The icechunk Repository is exposed on the ArrayLake Repo under different
    # names across versions; try the public ones and bail out honestly if none
    # is present rather than reaching into private state.
    ic_repo = next(
        (
            getattr(repo, attr)
            for attr in ("icechunk_repo", "repo", "_repo")
            if hasattr(repo, attr) and not callable(getattr(repo, attr))
        ),
        None,
    )
    if ic_repo is None:
        return {
            "expired": 0,
            "bytes_deleted": 0,
            "chunks_deleted": 0,
            "note": (
                "Underlying icechunk.Repository not reachable from the "
                "ArrayLake repo object; enable automatic expiration + GC in "
                "the ArrayLake UI instead."
            ),
        }

    import icechunk

    cutoff = datetime.now(timezone.utc) - timedelta(hours=older_than_hours)
    expired = icechunk.Repository.expire_snapshots(ic_repo, older_than=cutoff)
    gc = icechunk.Repository.garbage_collect(ic_repo, cutoff)
    return {
        "expired": len(expired),
        "bytes_deleted": getattr(gc, "bytes_deleted", 0),
        "chunks_deleted": getattr(gc, "chunks_deleted", 0),
    }


@app.local_entrypoint()
def main(
    product: str = "ch2-ml",
    action: str = "ingest",
    name: str = DEFAULT_REPO,
) -> None:
    """Manual trigger.

    modal run -m meteoswiss_nwp_arraylake.ingest --product ch2-ml

    Use ``--action create`` to create the repo, ``--action gc`` to manually
    expire + garbage-collect.
    """
    if action == "create":
        create_repo.remote(name=name)
    elif action == "gc":
        expire_and_gc.remote()
    elif action == "ingest":
        ingest_once.remote(product=product)
    else:
        raise ValueError(f"Unknown action {action!r}; expected create|gc|ingest")
