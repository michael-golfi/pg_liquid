# Example Domains

Use these domains when you want runnable public examples without relying on the
internal AI or memory fixtures that ship in regression SQL.

## Library Catalog

- taxonomy: `LibraryItem -> Book -> ReferenceBook`
- instances: holdings, editions, curated tags
- useful for: ontology validation, provenance compounds, catalog lookups

## Supply Chain Traceability

- graph edges: `shipment -> contains -> pallet -> contains -> lot`
- compounds: `Inspection@(site, lot, status, inspector)`
- useful for: recursive traversal, audit claims, row normalizers over ERP tables

## IT Asset Inventory

- graph edges: `employee -> assigned_device -> laptop`
- CLS model: `CompoundReadByRole@(compound_type="AssetAccess", role="viewer")`
- useful for: `read_as(...)`, access-scoped compounds, lifecycle metadata

## Public Transit Reachability

- graph edges: `station -> next_stop -> station`
- useful for: shortest-path-shaped reachability stress and recursive closure

## How To Use These Examples

- start with `liquid.query(...)` to seed the smallest graph
- add `validate_taxonomy(...)` and `validate_instances(...)` when a taxonomy is involved
- move repeated relational feeds into row normalizers once the graph model is stable
- keep privileged write APIs and normalizer registration behind trusted operator paths
