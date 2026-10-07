.PHONY: atlas test test-go test-vet test-java test-js test-e2e fmt search-demo kv-demo analytics-demo impact-demo video-demo tsdb-demo collab-demo collab-load

atlas:
	go run ./cmd/atlas -listen :8088 -data data/atlas

test: test-go test-vet test-java test-js test-e2e

test-go:
	go test -race ./...

test-vet:
	go vet ./...

test-java:
	cd engineering-labs/pulse-analytics && mvn --batch-mode test

test-js:
	for file in ot collab docs app; do node --check "internal/atlas/ui/$$file.js"; done
	node --test internal/atlas/uitests/*.test.js

test-e2e:
	node internal/atlas/uitests/e2e.js

fmt:
	gofmt -w $$(find cmd internal engineering-labs -name '*.go')

search-demo:
	./scripts/search-demo.sh

kv-demo:
	./scripts/kv-demo.sh

analytics-demo:
	docker compose -f engineering-labs/pulse-analytics/docker-compose.yml up --build

impact-demo:
	go run ./cmd/impact-demo

video-demo:
	./scripts/video-demo.sh

tsdb-demo:
	./scripts/tsdb-demo.sh

collab-demo:
	./scripts/collab-demo.sh

collab-load:
	node internal/atlas/uitests/load.js
