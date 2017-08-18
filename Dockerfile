# building
FROM golang

# misc
ENV APP_PROJECT github.com/ihsw/sotah-server/app

# copying in source
COPY ./app ./src/$APP_PROJECT

# building the project
RUN go get ./src/$APP_PROJECT/... \
  && CGO_ENABLED=0 go install $APP_PROJECT


# running
FROM debian

# runtime dir
RUN mkdir /srv/app
WORKDIR /srv/app

# copying in built app
COPY --from=0 /go/bin/app .

ENTRYPOINT ["./app"]
