# building
FROM golang

# misc
ENV APP_PROJECT github.com/ihsw/sotah-server/app

# copying in source
COPY ./app ./src/$APP_PROJECT

# building the project
RUN go get ./src/$APP_PROJECT/... \
  && go install $APP_PROJECT


# running
FROM debian

# installing ssl libs
RUN apt-get update -y \
  && apt-get install -yq ca-certificates

# runtime dir
RUN mkdir /srv/app
WORKDIR /srv/app

# copying in built app
COPY --from=0 /go/bin/app .

ENTRYPOINT ["./app"]
