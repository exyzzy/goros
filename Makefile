GO      ?= go
TINYGO  ?= tinygo
TINYGO_TARGET ?= pico
BIN     := bin

.PHONY: all build test generate tinygo pico clean vet

all: build

build: $(BIN)
	$(GO) build -o $(BIN)/ ./cmd/goros ./examples/talker ./examples/listener \
		./examples/add_two_ints_server ./examples/add_two_ints_client \
		./examples/parameters ./examples/imu_serial_host

$(BIN):
	mkdir -p $(BIN)

vet:
	$(GO) vet ./...

test:
	$(GO) test -race ./...

generate:
	$(GO) generate ./msgs

tinygo: $(BIN)
	$(TINYGO) build -o $(BIN)/talker-tinygo ./examples/talker
	$(TINYGO) build -o $(BIN)/listener-tinygo ./examples/listener

pico: $(BIN)
	$(TINYGO) build -target $(TINYGO_TARGET) -size short -o $(BIN)/pico_sensor-$(TINYGO_TARGET).uf2 ./examples/tinygo/pico_sensor

flash:
	$(TINYGO) flash -target $(TINYGO_TARGET) ./examples/tinygo/pico_sensor

clean:
	rm -rf $(BIN)
