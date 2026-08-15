# medcold-handoff-ledger

Developer reference for the 医药冷链本地交接账本 (medical cold-chain handoff
ledger). See `BENZHI_README.md` for the end-to-end overview in Chinese.

## Build & test

```bash
go mod tidy
go vet ./...
gofmt -w .
go test ./...
go test -race ./...
```

## Run the service

```bash
go run ./cmd/ledgerd --data-dir ./data --addr :8080 \
    --max-past 72h --max-future 1h
```

Submit an event:

```bash
curl -sS -X POST localhost:8080/v1/events -H 'Content-Type: application/json' -d '{
  "protocol_version": "1.0",
  "idempotency_key": "k1",
  "terminal_id": "T1",
  "terminal_sequence": 1,
  "box_id": "B1",
  "event_type": "box_created",
  "role": "pharmacy",
  "occurred_at": "2026-01-01T09:00:00Z",
  "payload": {
    "product_name": "Insulin", "batch": "B1", "origin": "DEPOT",
    "destination": "PHARM", "carrier_id": "C1",
    "lower_bound": 2.0, "upper_bound": 8.0
  }
}'
```

## Verify integrity

```bash
go run ./cmd/ledgerctl verify --data-dir ./data
```

## Package layout

| Package | Responsibility |
|---------|----------------|
| `internal/domain` | Pure state machine, box projection, payloads + validation |
| `internal/protocol` | Strict versioned JSON decoding (rejects unknown/dup/trailing/NaN) |
| `internal/store` | bbolt transactional persistence (single-tx linearisation) |
| `internal/coordinator` | Idempotency, causal deps, out-of-order replay, queries |
| `internal/ledger` | Facade: Open/Submit/Verify/Rebuild + integrity checks |
| `internal/api` | HTTP transport, error→status mapping |
| `internal/apperr` | Machine-readable error taxonomy |
| `internal/infra` | Clock, ID source, fault injection seams |
| `cmd/ledgerd` | Service entrypoint |
| `cmd/ledgerctl` | Read-only `verify` command |

## Determinism guarantees

Audit ordering is the stable tuple `(occurred_at, terminal_id,
terminal_sequence, event_id)`; pagination cursors encode that tuple so a
restart yields byte-identical page sequences. Replaying a fixed out-of-order
orchestration produces identical final state and event classifications across
runs.
