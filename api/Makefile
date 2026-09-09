# Variables
COVERAGE_DIR=htmlcov
COVERAGE_HTML=index.html

# Targets
.PHONY: all test test-js test-js-coverage test-with-coverage coverage clean deploy-prod deploy-prod-dry-run deploy-staging

# Default target
all: test

# Run tests and generate coverage report
test:
	pytest tests/ --ignore-glob="edts*" --ignore=edts

# Run the system map JS test suite (from the repo root package.json)
test-js:
	cd .. && npm run test:js

# JS tests with lcov coverage (SonarCloud consumes coverage/js/lcov.info)
test-js-coverage:
	cd .. && npm run test:js:coverage

# Run both test suites
test-all: test test-js

test-with-coverage:
	PYTHONPATH=.. pytest --cov=api --cov=feeders tests/ --ignore-glob="edts*" --ignore=edts --cov-report=html --cov-report=xml --cov-config=.coveragerc

# Open the coverage report in the default web browser
coverage: test-with-coverage
	sensible-browser $(COVERAGE_DIR)/$(COVERAGE_HTML) &

# Clean up coverage report
clean:
	rm -rf $(COVERAGE_DIR) $(DEPLOY_FILE_LIST) .coverage

# Deployment
deploy: test-all
	cicd/deploy.sh

deploy-staging: test-all
	cicd/deploy.sh staging
