# Reuters/LSEG API Research for Metapyle Integration

Research Date: 2026-01-14

## Executive Summary

The "Reuters" data ecosystem has undergone significant rebranding and consolidation. Thomson Reuters financial data division became Refinitiv, which is now part of the London Stock Exchange Group (LSEG). The legacy Eikon platform was retired in June 2025 and replaced by LSEG Workspace. While implementing a Metapyle source adapter for LSEG data is technically feasible, the access model is considerably more complex than Bloomberg's straightforward terminal + xbbg approach.

## Product Landscape

### Current Products (as of 2026)

The LSEG data ecosystem consists of several products serving different use cases:

**LSEG Workspace** is the primary desktop application replacing Eikon, providing access to real-time market data, news, and analytics. It runs a local proxy on port 9000 that Python libraries communicate with.

**LSEG Data Platform** is the cloud-based data delivery platform supporting both real-time streaming and historical data access. It can be accessed via desktop proxy or directly via OAuth credentials.

**Datastream** (via DSWS) is a specialized product for historical time-series data, particularly strong in economic indicators with data going back to the 1950s. It has its own API and authentication separate from the main data library.

**Tick History** (via REST API) provides bulk access to historical high-frequency tick data dating to 1996, useful for backtesting and research.

## Official Python Libraries

### lseg-data (Recommended)

The [lseg-data](https://pypi.org/project/lseg-data/) package is the current official library (v2.1.1 as of April 2025). It provides a clean, pandas-centric API similar to xbbg's approach.

```python
import lseg.data as ld

ld.open_session()

# Simple historical data retrieval
df = ld.get_history(
    universe=["IBM.N", "MSFT.O", "GOOG.O"],
    start="2024-01-01",
    end="2025-03-01",
    interval="daily"
)

# With specific fields
df = ld.get_history(
    universe="TSLA.O",
    fields=["TRDPRC_1", "BID", "ASK"],
    interval="daily",
    start="2020-01-02"
)

ld.close_session()
```

The library architecture has multiple abstraction layers. The Access layer provides simple interfaces suitable for interactive use, returning pandas DataFrames. The Content layer offers logical market data objects like historical pricing summaries. The Session layer manages connections to different access points.

### refinitiv-data (Deprecated)

The [refinitiv-data](https://pypi.org/project/refinitiv-data/) package (v1.6.2) is the predecessor to lseg-data. While still functional, users should migrate to lseg-data. The API is nearly identical.

### DatastreamDSWS

For Datastream access, the [DatastreamDSWS](https://pypi.org/project/DatastreamDSWS/) package provides historical economic and financial time-series data:

```python
import DatastreamDSWS as DSWS

ds = DSWS.Datastream(username='YOUR_USERNAME', password='YOUR_PASSWORD')
df = ds.get_data(tickers='US10YT=RR', start='BDATE', freq='D')
```

There is also an unofficial [PyDatastream](https://github.com/vfilimonov/pydatastream) wrapper with additional convenience functions.

## Session Types and Authentication

This is where LSEG diverges significantly from Bloomberg's simpler model.

### Desktop Session

The desktop session requires the LSEG Workspace (or legacy Eikon) application to be running on the same machine. The desktop app acts as a proxy, handling all authentication transparently. You only need an App Key, which can be generated through the App Key Generator in Workspace. Configuration is minimal:

```python
import lseg.data as ld
ld.open_session("desktop.workspace")  # Workspace must be running
```

This is analogous to Bloomberg's model where the Terminal must be running for blpapi/xbbg to work. However, LSEG Workspace requires an explicit app-key parameter while Bloomberg's xbbg works out of the box with a running terminal.

### Platform Session (Cloud)

The platform session connects directly to LSEG Data Platform in the cloud without requiring the desktop app. It requires OAuth2 credentials (client_id and client_secret) that must be provisioned by LSEG:

```python
import lseg.data as ld

# Configuration file or inline
ld.open_session("platform.ldp")  # Requires credentials in config
```

The platform session provides more flexibility for server-side applications but requires enterprise-level account setup. These credentials are not typically available to individual users and must be negotiated with LSEG sales.

## Comparison with Bloomberg/xbbg

| Aspect | Bloomberg/xbbg | LSEG/lseg-data |
|--------|---------------|----------------|
| Desktop Requirement | Bloomberg Terminal | LSEG Workspace (or platform creds) |
| Simple Wrapper | xbbg (community) | None equivalent |
| Official Library | blpapi | lseg-data |
| API Complexity | Simple | Multiple session types, config layers |
| Authentication | Automatic via terminal | App Key (desktop) or OAuth (platform) |
| Output Format | pandas DataFrame | pandas DataFrame |
| Symbol Format | Bloomberg tickers | RIC codes |
| Installation | `pip install xbbg` | `pip install lseg-data` |
| Zero-Config Usage | Yes (with terminal) | No (requires app-key at minimum) |

The xbbg library wraps blpapi with a remarkably simple interface where `blp.bdh('SPY', 'PX_LAST', '2020-01-01')` returns a DataFrame with minimal setup. The lseg-data library's `ld.get_history()` is conceptually similar but requires explicit session management and configuration.

## Third-Party and Community Libraries

### pyrfa

[PyRFA](https://github.com/devcartel/pyrfa) is an open-source Python API for Refinitiv Enterprise Platform (TREP-RT, Elektron, RMDS). It's designed for real-time data feeds using the OMM data model and requires infrastructure-level connectivity (config files, RMDS servers). This is not suitable for simple historical data retrieval and is much more complex than xbbg. The package supports Python up to 3.7 and hasn't been updated for newer Python versions.

### reuterspy

[reuterspy](https://pypi.org/project/reuterspy/) scrapes financial data from reuters.com, providing balance sheets, cash flow statements, income statements, and key metrics. However, it does not provide market price data or historical time-series. It's more comparable to a fundamentals scraper than a market data API.

### pydatastream

[PyDatastream](https://github.com/vfilimonov/pydatastream) is an unofficial wrapper for the Datastream Web Services API. It provides convenience functions and returns pandas DataFrames. The author is not affiliated with Thomson Reuters/LSEG. This could be useful specifically for Datastream historical data but requires a Datastream subscription.

## RIC (Reuters Instrument Code) Symbology

LSEG uses RIC codes as the primary identifier, similar to Bloomberg tickers. The format is typically `SYMBOL.EXCHANGE` (e.g., `IBM.N` for IBM on NYSE, `VOD.L` for Vodafone on LSE). LSEG provides symbology conversion APIs to translate between RIC, ISIN, CUSIP, SEDOL, and other identifier types. The [RIC Search tool](https://developers.lseg.com/en/tools-catalog/ric-search) is available on the developer portal for manual lookups.

## Pricing and Access

Refinitiv/LSEG does not publish pricing information. Subscription costs must be negotiated directly with LSEG sales. Generally:

- Workspace desktop licenses are similar in cost to Bloomberg Terminal licenses
- Platform (cloud) access requires additional enterprise agreements
- Datastream is a separate subscription
- Tick History is typically priced based on data volume

Exchange data often incurs additional fees, though delayed prices (15-20 minutes) may be available without exchange fees depending on the subscription tier.

## Implementation Assessment for Metapyle

### Feasibility: MEDIUM

A Metapyle source adapter for LSEG data is technically feasible. The lseg-data library provides a clean pandas-centric API that maps well to Metapyle's `BaseSource` interface.

### Proposed Architecture

```python
@register_source("lseg")
class LSEGSource(BaseSource):
    def __init__(self) -> None:
        # Session management: support both desktop and platform
        # Configuration via environment variables or config file
        pass

    def fetch(self, requests, start, end, **kwargs):
        # Use ld.get_history() with RICs
        # Return DataFrame with columns named symbol::field
        pass
```

### Key Challenges

**Session Complexity**: Unlike Bloomberg where xbbg just works with a running terminal, LSEG requires explicit session management. The adapter would need to handle desktop vs platform sessions, app-key configuration, and potential authentication failures gracefully.

**No xbbg Equivalent**: There is no community wrapper that simplifies the LSEG API to the same degree xbbg simplifies blpapi. Users would need to configure credentials and session types.

**Multiple Products**: LSEG's fragmented product line (Workspace, Datastream, Tick History) means different APIs for different data types. A single "lseg" source might be insufficient; separate sources for `lseg_workspace`, `datastream`, and `tick_history` might be needed.

**Testing Infrastructure**: Integration tests would require either a Workspace desktop license or platform credentials, making CI/CD testing difficult without dedicated test infrastructure.

### Recommended Approach

If implementing LSEG support, consider a phased approach:

Phase 1 would implement a basic `lseg` source using lseg-data for desktop session users. This mirrors the Bloomberg model where users must have the desktop app running. Catalog entries would use RIC codes as symbols with optional field specifications.

Phase 2 would add platform session support for server-side deployments, with credentials managed via environment variables or configuration files.

Phase 3 would add optional Datastream support as a separate `datastream` source for historical economic time-series.

### Catalog Entry Format

```yaml
my_series:
  source: lseg
  symbol: IBM.N
  field: TRDPRC_1  # Close price

my_fx_rate:
  source: lseg
  symbol: EUR=
  field: BID
```

## Conclusion

Implementing LSEG support in Metapyle is achievable but requires more complexity than Bloomberg/xbbg. The lseg-data library provides suitable pandas-based APIs, but users must handle session configuration and authentication explicitly. There is no simple "pip install and go" equivalent to xbbg. Organizations with existing LSEG/Refinitiv subscriptions and Workspace licenses would benefit from such an adapter, but the barrier to entry is higher than Bloomberg.

For users seeking a lightweight Reuters-like experience without an enterprise subscription, the ecosystem does not offer a comparable solution. The reuterspy package only provides fundamental data, and pyrfa requires infrastructure-level setup.

## Sources

- [LSEG Data Library for Python](https://developers.lseg.com/en/api-catalog/lseg-data-platform/lseg-data-library-for-python)
- [lseg-data on PyPI](https://pypi.org/project/lseg-data/)
- [refinitiv-data on PyPI](https://pypi.org/project/refinitiv-data/)
- [LSEG Configuration Process Guide](https://developers.lseg.com/en/article-catalog/article/configuration-process)
- [GitHub: LSEG Data Library Examples](https://github.com/LSEG-API-Samples/Example.DataLibrary.Python)
- [PyRFA on GitHub](https://github.com/devcartel/pyrfa)
- [PyDatastream on GitHub](https://github.com/vfilimonov/pydatastream)
- [reuterspy on PyPI](https://pypi.org/project/reuterspy/)
- [DatastreamDSWS on PyPI](https://pypi.org/project/DatastreamDSWS/)
- [LSEG Tick History REST API](https://developers.lseg.com/en/api-catalog/refinitiv-tick-history/refinitiv-tick-history-rth-rest-api)
- [RIC Search Tool](https://developers.lseg.com/en/tools-catalog/ric-search)
