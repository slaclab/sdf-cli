## sdf-cli / coact-daemon
##
## Container image build + Vault secret fetching for the batch daemons.

# --- container image -------------------------------------------------------
CONTAINER_RT ?= podman
REPO         ?= ghcr.io/slaclab
IMAGE        ?= sdf-cli
TAG          ?= $(shell date +"%Y%m%d-%H%M")
# Expand once so build and push agree even if the minute rolls over.
TAG          := $(TAG)

# --- vault -----------------------------------------------------------------
VAULT_SECRET_PATH     ?= secret/scs/coact
GROUPER_SECRET_PATH   ?= secret/tid/scs/osmaint
GROUPER_SECRET_FIELD  ?= password

# ---------------------------------------------------------------------------
# Container image
# ---------------------------------------------------------------------------
build: update-sdf-ansible ## Build the coact-daemon image
	$(CONTAINER_RT) build . -f Dockerfile -t $(REPO)/$(IMAGE):$(TAG)

push: build ## Build and push the image; prints the tag to pin in coact-deploy
	$(CONTAINER_RT) push $(REPO)/$(IMAGE):$(TAG)
	@echo "Pushed $(REPO)/$(IMAGE):$(TAG)"

update-sdf-ansible: ## Check out the sdf-ansible playbooks (ansible-runner/project)
	git submodule update --init --recursive

# ---------------------------------------------------------------------------
# Secrets
# ---------------------------------------------------------------------------
secrets: ## Fetch daemon secrets from Vault into etc/.secrets/
	mkdir -p etc/.secrets/
	set -e; for i in password; do vault kv get --field=$$i $(VAULT_SECRET_PATH)/service-account > etc/.secrets/$$i ; done
	vault kv get --field=$(GROUPER_SECRET_FIELD) $(GROUPER_SECRET_PATH) > etc/.secrets/grouper_password
	chmod -R go-rwx etc/.secrets

clean-secrets: ## Remove etc/.secrets/
	rm -rf etc/.secrets
