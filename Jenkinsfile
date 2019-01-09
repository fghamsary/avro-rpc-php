properties([[$class: 'BuildDiscarderProperty', strategy: [$class: 'LogRotator', numToKeepStr: '15']]])


pipeline {
    agent any
    environment {
        COMPOSER_HOME = tool name: 'Auto_Composer', type: 'com.cloudbees.jenkins.plugins.customtools.CustomTool'
        PATH = "/opt/rh/rh-php72/root/usr/bin:$COMPOSER_HOME:$PATH" // PHP Path on Jenkins
    }

    post {
        failure {
            echo 'Build failed'
            slackSend color: "danger", message: ":negative_squared_cross_mark: *${env.JOB_NAME}* `${env.gitlabBranch}`: " +
                    "#${env.BUILD_NUMBER} failed.\n${env.BUILD_URL}"
            updateGitlabCommitStatus name: 'Global status', state: 'failed'
        }
        success {
            echo 'Build successful'
            slackSend color: "good", message: ":heavy_check_mark: *${env.JOB_NAME}* `${env.gitlabBranch}`: " +
                    "#${env.BUILD_NUMBER} successful.\n${env.BUILD_URL}"
            updateGitlabCommitStatus name: 'Global status', state: 'success'
        }
        unstable {
            echo 'Build unstable'
            updateGitlabCommitStatus name: 'Global status', state: 'failed'
            addGitLabMRComment comment: 'Build is unstable !'

            // fetch test count from Jenkins API
            script {
                def failed = sh(script : "wget -O - '${env.BUILD_URL}/api/xml?xpath=//*[@_class=\"hudson.tasks.junit.TestResultAction\"]/failCount' | sed 's/[^0-9]*//g'", returnStdout: true)
                def total = sh(script : "wget -O - '${env.BUILD_URL}/api/xml?xpath=//*[@_class=\"hudson.tasks.junit.TestResultAction\"]/totalCount' | sed 's/[^0-9]*//g'", returnStdout: true)

                slackSend color: "warning", message: ":warning: *${env.JOB_NAME}* `${env.gitlabBranch}`: " +
                        "#${env.BUILD_NUMBER} unstable (${failed} failed / ${total} total).\n${env.BUILD_URL}"
                addGitLabMRComment comment: "Build is unstable ! Failed tests: ${failed} / ${total}"
            }

        }
        aborted {
            echo 'Build aborted'
            slackSend color: "danger", message: ":no_entry_sign: *${env.JOB_NAME}* `${env.gitlabBranch}`: " +
                    "#${env.BUILD_NUMBER} aborted.\n${env.BUILD_URL}"
            updateGitlabCommitStatus name: 'Global status', state: 'canceled'
        }
    }


    options {
        gitlabBuilds(builds: ['Global status','Prepare','Dependencies','Grunt','Verify submodules','Test','Test results'])
    }


    stages {
        stage("Prepare") {
            steps {
                gitlabCommitStatus(name: "Prepare") {
                    checkout scm
                }
            }
        }
        stage("ENV VARS") {
            steps {
                sh 'printenv'
            }
        }
        stage("Dependencies") {
            steps {
                gitlabCommitStatus(name: "Dependencies") {
// there are dependancies with composer (see composer.json)
                    sh "composer install"
                }
            }
        }
        stage("Test") {
            steps {
                gitlabCommitStatus(name: "Test") {
// PHPUnit was installed from composer, so it is located in 'vendor/'
// Tests are located in 'test' folder, and their name is suffixed by 'Test'
// We also tell PHPUnit to output JUnit-style reports
// And we make the build unstable if some tests don't pass, and failed if there are some other errors (wrong syntax, etc.)
// Here, we parse the line before the last one, which either contains : 'SUCCESS', 'FAILURES' or 'ERRORS'.
// If we see 'ERRORS', we fail the build using a return status that is not 0 (i.e. 1, when grep find the ERRORS string in it's only given line)

// N.B. : we use a bootstrap file for these tests, and we actually need a configuration file to ignore PHP notices
// we also exclude the 'integration' test-group, because we won't be able to run them
                    sh "vendor/bin/phpunit -c test/phpunit.xml --log-junit php_test.xml test/AllTests.php --coverage-clover php_coverage.xml | tail -n 4 | head -n 1 | grep -v ERRORS"
                }
            }
        }
        stage("Test results") {
            steps {
                gitlabCommitStatus(name: "Test results") {
// phpUnit results will be treated as JUnit results, as they have the same format
// and so we use this step to set the build unstable when some tests fails
                    junit allowEmptyResults: true, testResults: 'php_test.xml'
                }
            }
        }
        stage("Sonar preview analysis") {
// this stage perform the Sonar preview analysis to get comments, when:
// - we are in a MR commit (i.e. gitlabMergeRequestTitle is set)
            when {
                anyOf {
                    not {
                        environment name:'gitlabMergeRequestTitle', value:''
                    }
                    environment name:'gitlabActionType', value:'TAG_PUSH'
                }
            }
            steps {
// we first perform a preview analysis, so we can get new issues and show them in GitLab
// we use variables provided by both Jenkins and the GitLab plgin in order to find the MR corresponding to the build, so that the GitLab plugin for Sonar can make comments
// (see specific documentation for more information)
// NB. : with PHP, every branch need to have a sonar-project.properties file, which contains informations about the project, such as current version, name, and so on...
                script {
                    def scannerHome = tool 'Auto_SonarQube_Scanner'
                    gitlabCommitStatus(name: "Sonar preview analysis") {
                        withSonarQubeEnv('SonarQube server') {
                            sh "${scannerHome}/bin/sonar-scanner  -Dsonar.analysis.mode=preview  -Dsonar.gitlab.commit_sha=$GIT_COMMIT  -Dsonar.gitlab.ref_name=$gitlabSourceBranch   -Dsonar.gitlab.project_id=$GIT_URL"
                        }
                    }
                }
            }
        }
        stage("Sonar full analysis") {
// we perform a full analysis when we are commiting to master
            when {
                environment name:'gitlabBranch', value:'master'
            }
            steps {
// NB. : with PHP, every branch need to have a sonar-project.properties file, which contains informations about the project, such as current version, name, and so on...
                script {
                    def scannerHome = tool 'Auto_SonarQube_Scanner'
                    gitlabCommitStatus(name: "Sonar full analysis") {
                        withSonarQubeEnv('SonarQube server') {
                            sh "${scannerHome}/bin/sonar-scanner  -Dsonar.gitlab.commit_sha=$GIT_COMMIT  -Dsonar.gitlab.ref_name=$gitlabSourceBranch   -Dsonar.gitlab.project_id=$GIT_URL   -Dsonar.php.tests.reportPath=php_test.xml  -Dsonar.php.coverage.reportPaths=php_coverage.xml  -Dsonar.tests=test  -Dsonar.sources=src"
                        }
                    }
                }
            }
        }
    }
}
