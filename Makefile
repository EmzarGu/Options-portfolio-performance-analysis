VENV_PYTHON := .venv/bin/python
PYTEST_ARGS ?=

.PHONY: test
test:
	@if [ ! -x "$(VENV_PYTHON)" ]; then \
		echo "Project virtualenv not found at $(VENV_PYTHON)."; \
		echo "Create or restore .venv before running tests."; \
		exit 1; \
	fi
	$(VENV_PYTHON) -m pytest -q $(PYTEST_ARGS)
