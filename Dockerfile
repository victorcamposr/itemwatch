# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/itemwatch ./cmd/itemwatch \
 && CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/ledgersim ./cmd/ledgersim

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/itemwatch /usr/local/bin/itemwatch
COPY --from=build /out/ledgersim /usr/local/bin/ledgersim
USER nonroot:nonroot
EXPOSE 8080
HEALTHCHECK --interval=30s --timeout=5s --retries=3 CMD ["/usr/local/bin/itemwatch", "healthcheck"]
ENTRYPOINT ["/usr/local/bin/itemwatch"]
