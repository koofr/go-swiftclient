go-swiftclient
==============

Go OpenStack Swift client.

[![GoDoc](https://godoc.org/github.com/koofr/go-swiftclient?status.png)](https://godoc.org/github.com/koofr/go-swiftclient)

## Install

```sh
go get github.com/koofr/go-swiftclient
```

## Testing

To run tests you will need to run a test Ceph RGW docker container:

```sh
docker run --rm -it -p 8080:8080 bancek/ceph-rgw-swift-test:0.1.0-nautilus

sh -c 'while ! curl --fail -H "X-Auth-User: test:test" -H "X-Auth-Key: test" http://localhost:8080/auth/v1.0 2>/dev/null; do echo "waiting for swift" && sleep 1; done; echo'

go test ./...
```
