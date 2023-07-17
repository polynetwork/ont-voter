GC=go build
COMMIT ?= master

side-voter: $(SRC_FILES)
	$(GC) -o side-voter main.go

clean:
	rm -f side-voter
	docker container rm -f go-side-voter-temp
	docker rmi -f go-side-voter-build

build: clean
	@echo "Building voter binary in container"
	docker build --no-cache --build-arg commit=$(COMMIT) -t go-side-voter-build .
	docker container create --name go-side-voter-temp go-side-voter-build
	docker container cp go-side-voter-temp:/workspace/side-voter .
	sha256sum side-voter

always:
.DELETE_ON_ERROR:
.PHONY: clean