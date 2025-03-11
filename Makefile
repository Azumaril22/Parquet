# Commands

.PHONY: format
format: ## format and lint backend code
	.venv/bin/isort ./
	.venv/bin/black ./

## .venv/bin/flake8 ./
