#!groovy

import groovy.transform.Field

@Field String email_to = 'sw@platinasystems.com'
@Field String email_from = 'jenkins-bot@platinasystems.com'
@Field String email_reply_to = 'no-reply@platinasystems.com'

pipeline {
    agent any
    environment {
	GOPATH = "$WORKSPACE/go-pkg"
	HOME = "$WORKSPACE"
    }
    stages {
	stage('Checkout') {
	    steps {
		echo "Running build #${env.BUILD_ID} on ${env.JENKINS_URL} GOPATH ${GOPATH}"
		dir('goes') {
		    git([
			url: 'https://github.com/platinasystems/goes.git',
			branch: 'master'
		    ])
		}
	    }
	}
	stage('Build') {
	    steps {
		dir('goes') {
		    echo "Building goes..."
		    sh 'set +x; export PATH=/usr/local/go/bin:${PATH}; for package in `find . -type d -print` ; do ls $package/*.go > /dev/null 2>&1 && { echo "Working on" $package ; { go build  -v -buildmode=archive $package || exit; } } || echo "Skipping" $package;done'
		}
	    }
	}
    }

    post {
	success {
	    mail body: "GOES build ok: ${env.BUILD_URL}",
		from: email_from,
		replyTo: email_reply_to,
		subject: 'GOES build ok',
		to: email_to
	}
	failure {
	    cleanWs()
	    mail body: "GOES build error: ${env.BUILD_URL}",
		from: email_from,
		replyTo: email_reply_to,
		subject: 'GOES BUILD FAILED',
		to: email_to
	}
    }
}
