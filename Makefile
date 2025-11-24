# Directories
BIN_DIR := bin

# Binaries
BINS := coordinator loadgen proxy

.PHONY: all clean $(BINS)

all: $(BINS)

coordinator:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/coordinator ./cmd/coordinator

loadgen:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/loadgen ./cmd/loadgen

proxy:
	@mkdir -p $(BIN_DIR)
	go build -o $(BIN_DIR)/proxy ./cmd/proxy

clean:
	rm -rf $(BIN_DIR)
