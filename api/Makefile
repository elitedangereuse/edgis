# Variables
COVERAGE_DIR=htmlcov
COVERAGE_HTML=index.html

# Targets
.PHONY: all test coverage clean deploy-prod deploy-prod-dry-run

# Default target
all: test

# Run tests and generate coverage report
test:
	pytest tests/ --ignore-glob="edts*" --ignore=edts

test-with-coverage:
	pytest --cov=api tests/ --ignore-glob="edts*" --ignore=edts --cov-report=html --cov-config=.coveragerc

# Open the coverage report in the default web browser
coverage: test-with-coverage
	sensible-browser $(COVERAGE_DIR)/$(COVERAGE_HTML) &

# Clean up coverage report
clean:
	rm -rf $(COVERAGE_DIR) $(DEPLOY_FILE_LIST) .coverage

# Deployment
deploy: test
	cicd/deploy.sh
