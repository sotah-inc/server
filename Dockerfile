# building
FROM golang:1.11 as build-env

# misc
ENV APP_PROJECT github.com/ihsw/sotah-server/app

# working dir
WORKDIR /srv/app

# gathering deps
COPY ./app/go.mod .
COPY ./app/go.sum .
RUN go mod download

# copying in source
COPY ./app .

# building the project
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -a -installsuffix cgo -o /go/bin/app


# running
FROM debian as runtime-env

# installing ssl libs
RUN apt-get update -y \
  && apt-get install -yq ca-certificates

# runtime dir
RUN mkdir /srv/app
WORKDIR /srv/app

# copying in built app
COPY --from=build-env /go/bin/app /go/bin/app

ENTRYPOINT ["/go/bin/app"]
