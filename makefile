# start project configuration
name := amboy
buildDir := build
packages := $(name) dependency job registry pool queue rest logger management cli queue-amzsqs queue-pgq queue-mdbq
orgPath := github.com/mongodb
projectPath := $(orgPath)/$(name)
# end project configuration


 # start environment setup
gobin := ${GO_BIN_PATH}
ifeq (,$(gobin))
gobin := go
endif
gopath := $(shell $(gobin) env GOPATH)
gocache := $(abspath $(buildDir)/.cache)
ifeq ($(OS),Windows_NT)
gocache := $(shell cygpath -m $(gocache))
gopath := $(shell cygpath -m $(gopath))
endif
goEnv := GOPATH=$(gopath) GOCACHE=$(gocache) $(if ${GO_BIN_PATH},PATH="$(shell dirname ${GO_BIN_PATH}):${PATH}")
# end environment setup


# start lint setup targets
lintDeps := $(buildDir)/.lintSetup $(buildDir)/run-linter $(buildDir)/golangci-lint
$(buildDir)/.lintSetup:$(buildDir)/golangci-lint
	@touch $@
$(buildDir)/golangci-lint:
	@mkdir -p $(buildDir)
	$(goEnv) GO111MODULES=on $(gobin) get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.36.0
	@cp $(gopath)/bin/golangci-lint $(buildDir)
$(buildDir)/run-linter:buildscripts/run-linter.go $(buildDir)/.lintSetup $(buildDir)
	@$(goEnv) $(gobin) build -o $@ $<
# end lint setup targets


# start test setup targets
testDeps := $(buildDir)/.testSetup $(buildDir)/gotestsum $(buildDir)/goveralls
$(buildDir)/.testSetup:$(buildDir)/gotestsum $(buildDir)/goveralls
	@touch $@
$(buildDir)/gotestsum:
	@mkdir -p $(buildDir)
	$(goEnv) GO111MODULES=on $(gobin) get gotest.tools/gotestsum@latest
	@cp $(gopath)/bin/gotestsum $(buildDir)
$(buildDir)/goveralls:
	@mkdir -p $(buildDir)
	$(goEnv) GO111MODULES=on $(gobin) get github.com/mattn/goveralls@latest
	@cp $(gopath)/bin/goveralls $(buildDir)
# end test setup targets


# benchmark setup targets
benchmarks:$(buildDir)/run-benchmarks $(buildDir) .FORCE
	$(goEnv) ./$(buildDir)/run-benchmarks $(run-benchmark)
$(buildDir)/run-benchmarks:cmd/run-benchmarks/run-benchmarks.go $(buildDir)
	$(goEnv) $(gobin) build -o $@ $<
# end benchmark setup targets


######################################################################
##
## Everything below this point is generic, and does not contain
## project specific configuration. (with one noted case in the "build"
## target for library-only projects)
##
######################################################################

_compilePackages := $(subst $(name),,$(subst -,/,$(foreach target,$(packages),./$(target))))
coverageOutput := $(foreach target,$(packages),$(buildDir)/output.$(target).coverage)
coverageHtmlOutput := $(foreach target,$(packages),$(buildDir)/output.$(target).coverage.html)

# start dependency installation tools
#   implementation details for being able to lazily install dependencies
testOutput := $(foreach target,$(packages),$(buildDir)/output.$(target).json)
coverageOutput := $(foreach target,$(packages),$(buildDir)/output.$(target).coverage)
coverageHtmlOutput := $(foreach target,$(packages),$(buildDir)/output.$(target).coverage.html)
# end dependency installation tools


# userfacing targets for basic build and development operations
lint:$(foreach target,$(packages),$(buildDir)/output.$(target).lint)
test:$(foreach target,$(packages),$(buildDir)/output.$(target).json)
coverage:$(buildDir) $(coverageOutput)
coverage-html:$(buildDir) $(coverageHtmlOutput)
compile $(buildDir):
	@mkdir -p $(buildDir)
	$(goEnv) $(gobin) build $(_compilePackages)
compile-base:
	$(goEnv) $(gobin) build  ./
# convenience targets for runing tests and coverage tasks on a
# specific package.
test-%:$(buildDir)/output.%.json
	
coverage-%:$(buildDir)/output.%.coverage
	
html-coverage-%:$(buildDir)/output.%.coverage.html
	
lint-%:$(buildDir)/output.%.lint
	
# end convienence targets
phony := lint build build-race race test coverage coverage-html
.PRECIOUS:$(testOutput) $(coverageOutput) $(coverageHtmlOutput)
.PRECIOUS:$(foreach target,$(packages),$(buildDir)/test.$(target))
.PRECIOUS:$(foreach target,$(packages),$(buildDir)/output.$(target).lint)
.PRECIOUS:$(buildDir)/output.lint
# end front-end


# implementation details for building the binary and creating a
# convienent link in the working directory
$(gopath)/src/$(orgPath):
	@mkdir -p $@
$(gopath)/src/$(projectPath):$(gopath)/src/$(orgPath)
	@[ -L $@ ] || ln -s $(shell pwd) $@
# end main build



# start test and coverage artifacts
#    tests have compile and runtime deps. This varable has everything
#    that the tests actually need to run. (The "build" target is
#    intentional and makes these targets rerun as expected.)
testArgs := -test.v --test.timeout=10m -json
ifneq (,$(RUN_TEST))
testArgs += -test.run='$(RUN_TEST)'
endif
ifneq (,$(RUN_CASE))
testArgs += -testify.m='$(RUN_CASE)'
endif
ifneq (,$(RUN_COUNT))
testArgs += -test.count='$(RUN_COUNT)'
endif
ifneq (,$(RACE_DETECTOR))
testArgs += -race
endif
ifneq (,$(DISABLE_COVERAGE))
testArgs += -cover
endif
ifneq (,$(SKIP_LONG))
testArgs += -short
endif
ifneq (,$(NUM_THREADS))
testArgs += -test.parallel='$(NUM_THREADS)'
endif
#    implementation for package coverage and test running,mongodb to produce
#    and save test output.
# nonobvious thing: 
$(buildDir)/:
	@mkdir -p $@
$(buildDir)/output.%.json:$(buildDir)/gotestsum .FORCE
	@mkdir -p $(buildDir)/
	$(buildDir)/gotestsum --debug --hide-summary=skipped --jsonfile=$@ -- $(testArgs) $(if $(findstring mdbq,$@),-test.parallel=1,) ./$(if $(subst $(name),,$*),$(subst -,/,$*),)
	$(buildDir)/gotestsum tool slowest --jsonfile=$@ --threshold=5s
$(buildDir)/output.%.coverage:$(buildDir)/gotestsum $(buildDir)/goveralls .FORCE
	@mkdir -p $(buildDir)/
	$(buildDir)/gotestsum --debug --hide-summary=skipped --jsonfile=$(buildDir)/output.$*.json -- $(testArgs) $(if $(findstring mdbq,$@),-p 1,) ./$(if $(subst $(name),,$*),$(subst -,/,$*),) -race -covermode atomic -coverprofile=$@
	@-[ -f $@ ] && $(gobin) tool cover -func=$@ | sed 's%$(projectPath)/%%' | column -t
	@-[ -z "$(COVERALLS_TOKEN)" ] || $(buildDir)/goveralls -coverprofile=$@ -parallel -flagname=$* -service=github
$(buildDir)/output.%.coverage.html:$(buildDir)/output.%.coverage
	$(goEnv) $(gobin) tool cover -html=$< -o $@
#  targets to generate gotest output from the linter.
$(buildDir)/output.%.lint:$(buildDir)/run-linter .FORCE
	@mkdir -p $(buildDir)/
	@$(goEnv) ./$< --output=$@ --lintBin=$(buildDir)/golangci-lint --packages='$*'
# end test and coverage artifacts


# clean and other utility targets
clean:
	rm -rf $(name) $(lintDeps) $(buildDir)/output.*
phony += clean
# end dependency targets

# configure phony targets
.FORCE:
.PHONY:$(phony)
mdebw:
	@echo sdfd $(if md,$(findstring md,$@),-parallel=1)

