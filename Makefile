.PHONY: atlas test test-go test-java fmt search-demo kv-demo analytics-demo video-demo tsdb-demo

atlas:
	go run ./cmd/atlas -listen :8088 -data data/atlas

test: test-go test-java

test-go:
	go test -race ./...

test-java:
	cd event-analytics && mvn test

fmt:
	gofmt -w $$(find cmd internal -name '*.go')

search-demo:
	./scripts/search-demo.sh

kv-demo:
	./scripts/kv-demo.sh

analytics-demo:
	docker compose -f event-analytics/docker-compose.yml up --build

video-demo:
	./scripts/video-demo.sh

tsdb-demo:
	./scripts/tsdb-demo.sh
