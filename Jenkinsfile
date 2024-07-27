@Library('COSM-Jenkins-libs') _

pipeline {

    agent none

    options {
        // This is required if you want to clean before build
        skipDefaultCheckout(true)
    }

    stages {

        stage('Preparation') {
            agent { node { label 'master' } }
            steps {
                step([$class: 'WsCleanup'])

                checkout scm

                sh '''#!/bin/bash
                    git log -n 1 | grep "commit " | sed 's/commit //g' > currenntVersion
                '''

                stash name:'workspace', includes:'**'
            }
        }

        stage('Build application') {
            agent { docker {
                    image 'maven:eclipse-temurin-21-alpine'
                    // Run the container on the node specified at the
                    // top-level of the Pipeline, in the same workspace,
                    // rather than on a new node entirely:
                    reuseNode true
                    args '-u root'
                } }
            steps {
                unstash 'workspace'
                sh '''#!/bin/bash
                    echo "----------------------"
                    pwd
                    echo "----------------------"
                    ls -la
                    echo "----------------------"
                    mvn -B clean install
                '''
            }
        }

        stage('Deploy artifacts') {
            agent {
                docker {
                    image 'docker-builder'
                    // Run the container on the node specified at the
                    // top-level of the Pipeline, in the same workspace,
                    // rather than on a new node entirely:
                    reuseNode true
                    args '-u root --net="main_bridge" -v /var/run/docker.sock:/var/run/docker.sock'
                }
            }
            steps {
                // 1. Call steps of building artifacts and deploying it for tests:
                sh './.infrastructure/build_updater.sh'
                sh './.infrastructure/build_balancer.sh'
                sh './.infrastructure/build_pgfacade.sh'
                sh './.infrastructure/deploy_minio.sh'
                sh './.infrastructure/deploy_components.sh'

                // 2. Call all tests

                // 3. Deploying new version of app
            }
        }
    }

    post {
        always {
            node ('master') {
                script {
                    env.GIT_URL = env.GIT_URL_1
                    notifyRocketChat(
                        channelName: 'system_notifications_pgfacade',
                        minioCredentialsId: 'jenkins-minio-credentials',
                        minioHostUrl: 'https://minio.cloud.cosm-lab.science'
                    )
                }
            }
        }
    }
}