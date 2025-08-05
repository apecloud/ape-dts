#
# Copyright 2022-2023 The KubeBlocks Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

################################################################################
# Variables                                                                    #
################################################################################
.SHELLFLAGS = -ec

VERSION ?= 0.1.0
CONFIG_PATH ?= ./images/example/mysql_snapshot_sample.yaml
MODULE_NAME ?= dt-main
GIT_BRANCH ?= main

export RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static

# To use buildx: https://github.com/docker/buildx#docker-ce
export DOCKER_CLI_EXPERIMENTAL=enabled
DOCKER:=DOCKER_BUILDKIT=1 docker
BUILDX_ENABLED ?= ""
ifeq ($(BUILDX_ENABLED), "")
	ifeq ($(shell docker buildx inspect 2>/dev/null | awk '/Status/ { print $$2 }'), running)
		BUILDX_ENABLED = true
	else
		BUILDX_ENABLED = false
	endif
endif
BUILDX_BUILDER ?= x-builder

define BUILDX_ERROR
buildx not enabled, refusing to run this recipe
endef

APT_MIRROR ?= mirrors.aliyun.com
IMG ?= apecloud/ape-dts
IMG_TAG ?= latest
PLATFORMS ?= linux/arm64,linux/amd64

.DEFAULT_GOAL := help

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk commands is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php
# https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

# .PHONY: all
# all: ## Make all cmd binaries.

##@ Development

CARGO_BUILD_ARGS ?=

.PHONY: init
init: ## Build
	git submodule update --init

.PHONY: build
build: ## Build
	cargo build --release $(CARGO_BUILD_ARGS)

.PHONY: build-debug
build-debug: ## Build
	cargo build $(CARGO_BUILD_ARGS)

.PHONY: build-release
build-release: ## Build release
	cargo build --release $(CARGO_BUILD_ARGS)

.PHONY: clean
clean: ## Clean
	cargo clean

.PHONY: lint
lint: ## Run code linting
	cargo clippy --workspace

.PHONY: test 
test: CARGO_INCREMENTAL=0
test: RUSTFLAGS='-Cinstrument-coverage'
test: LLVM_PROFILE_FILE='cargo-test-%p-%m.profraw'
test: ## Run tests.
	cargo test --workspace --lib

.PHONY: test-cover
test-cover: grcov test  ## Run tests with coverage report
	grcov . --binary-path ./target/debug/deps/ -s . -t html --branch --ignore-not-existing --ignore '../*' --ignore "/*" -o target/coverage/html

##@ Container images

DOCKER_BUILD_ARGS ?=

.PHONY: install-docker-buildx
install-docker-buildx: ## Create `docker buildx` builder.
	@if ! docker buildx inspect $(BUILDX_BUILDER) > /dev/null; then \
		echo "Buildx builder $(BUILDX_BUILDER) does not exist, creating..."; \
		docker buildx create --name=$(BUILDX_BUILDER) --use --driver=docker-container --platform linux/amd64,linux/arm64; \
	else \
	  echo "Buildx builder $(BUILDX_BUILDER) already exists"; \
	fi

.PHONY: docker-build
docker-build: DOCKER_BUILD_ARGS += --cache-to type=gha,mode=max --cache-from type=gha --build-arg MODULE_NAME=$(MODULE_NAME) --build-arg APT_MIRROR=$(APT_MIRROR) #--no-cache
docker-build: install-docker-buildx ## Build docker image.
ifneq ($(BUILDX_ENABLED), true)
	$(DOCKER) build --tag $(IMG):$(IMG_TAG) $(DOCKER_BUILD_ARGS) .
else
	$(DOCKER) buildx build --platform ${PLATFORMS} --tag $(IMG):$(IMG_TAG) $(DOCKER_BUILD_ARGS) .
endif

.PHONY: docker-build-local
docker-build-local: ## Build docker image locally without buildx
	docker build --tag $(IMG):$(IMG_TAG) --build-arg MODULE_NAME=$(MODULE_NAME) --build-arg APT_MIRROR=$(APT_MIRROR) .

.PHONY: docker-push
docker-push: docker-build ## Push docker image.
ifneq ($(BUILDX_ENABLED), true)
	$(DOCKER) push --tag $(IMG):$(IMG_TAG) 
else
	$(DOCKER) buildx build --platform ${PLATFORMS} --tag $(IMG):$(IMG_TAG) $(DOCKER_BUILD_ARGS) --push .
endif

##@ Tools

.PHONY: grcov
grcov: ## Download grcov locally if necessary.
ifeq (, $(shell ls $(which grcov) 2>/dev/null))
	cargo install grcov
endif