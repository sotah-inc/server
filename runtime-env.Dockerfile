# running
FROM debian as runtime-env

# # installing ssl libs
RUN apt-get update -y \
  && apt-get install -yq ca-certificates

# # runtime dir
RUN mkdir /srv/app
WORKDIR /srv/app

# # copying in built app
COPY --from=ihsw/sotah-server/build /go/bin/app /go/bin/app

ENTRYPOINT ["/go/bin/app"]
