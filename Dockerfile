FROM golang:1.25.3-alpine AS build

WORKDIR /src

COPY go.mod ./
COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/sr .

FROM alpine:3.22

RUN adduser -D -H -u 10001 sr

COPY --from=build /out/sr /usr/local/bin/sr

USER sr
ENTRYPOINT ["sr"]
