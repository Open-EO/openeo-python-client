#!/usr/bin/env groovy

/*
    This Jenkinsfile is used to provide snapshot builds using the VITO CI system.
    GitHub Actions is used to provide publicly accessible test results.
    This Jenkinsfile uses the Jenkins shared library. (ssh://git@git.vito.local:7999/biggeo/jenkinslib.git)
    Information about the pythonPipeline method can be found in pythonPipeline.groovy
*/

@Library('lib')_

pythonPipeline {
  package_name = 'openeo'
  wipeout_workspace = true
  python_version = ["3.6"]
  extras_require = 'dev'
  upload_dev_wheels = false
  wheel_repo = 'python-openeo'
  wheel_repo_dev = 'python-openeo'
  pep440 = true
  extra_env_variables = [
    /* Workaround to have pytest `basetemp` inside Jenkins workspace, including the retention feature. Set see https://github.com/pytest-dev/pytest/issues/10829 */
    "PYTEST_DEBUG_TEMPROOT=pytest-tmp",
  ]
}
