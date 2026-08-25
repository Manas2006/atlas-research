FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.work ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/atlas ./cmd/atlas
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/video-worker ./cmd/video-worker

FROM alpine:3.22
RUN apk add --no-cache ca-certificates ffmpeg
COPY --from=build /out/atlas /usr/local/bin/atlas
COPY --from=build /out/video-worker /usr/local/bin/video-worker
EXPOSE 8088
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/atlas"]
CMD ["-listen", ":8088", "-data", "/data"]
