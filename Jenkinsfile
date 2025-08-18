def date = new Date().format("yyyyMMdd.HHmm", TimeZone.getTimeZone('UTC'))
def short_sha = "000000"
def imgTag = ""
def projectGroup = "kairosdb"
def ingestProjectName = "ingest"

pipeline {
  agent {
    kubernetes {
      defaultContainer 'ubuntu'
      activeDeadlineSeconds 3600
    }
  }
  options {
    ansiColor('xterm')
  }
  stages {
    stage('Init') {
      steps {
        checkout scm
        discoverReferenceBuild()
        script {
          def m = (env.GIT_URL =~ /(\/|:)(([^\/]+)\/)?(([^\/]+?)(\.git)?)$/)
          if (m) {
            def org = m.group(3)
            def repo = m.group(5)
          }
          short_sha = "${GIT_COMMIT}".substring(0, 6)
          imgTag = "${env.BRANCH_NAME}_${date}_${short_sha}"
        }
      }
    }
    
    stage('Setup Tilt') {
      steps {
        sh "k3d cluster create k3d --registry-create k3d-repository"
        sh "tilt up --namespace default -- cassandra < /dev/null > /dev/null &"
      }
    }

    stage('Unit Tests') {
      steps {
        sh "cargo test --workspace --lib -- --test-threads=1"
        sh "cargo clippy --all-targets --all-features --message-format json > target/clippy-report.json || true"
        sh "cargo fmt --check"
      }
    }

    stage('Integration Tests') {
      steps {
        sh "tilt wait --for=condition=Ready 'uiresource/cassandra' --timeout 600s"
        sh "cargo test --workspace --test '*' -- --ignored --test-threads=1"
      }
    }

    stage('Tear down Tilt') {
      steps {
        sh "tilt down"
        sh "k3d cluster delete k3d"
      }
    }
    
    stage('Publish Ingest Docker Image') {
      steps {
        script {
          withCredentials([usernamePassword(credentialsId: 'harbor-user', usernameVariable: 'HARBOR_USERNAME', passwordVariable: 'HARBOR_PASSWORD')]) {
            sh "echo \"\$HARBOR_PASSWORD\" | docker login --password-stdin -u '$HARBOR_USERNAME' harbor.arpnetworking.com"
            sh "docker buildx create --name ${ingestProjectName} --use dind-context"
            sh """
            docker buildx build -f kairosdb-ingest/Dockerfile -t harbor.arpnetworking.com/${projectGroup}/${ingestProjectName}:${imgTag} \\
            --build-arg BUILDKIT_INLINE_CACHE=1 \\
            --cache-from type=registry,ref=harbor.arpnetworking.com/${projectGroup}/${ingestProjectName}-cache:${env.BRANCH_NAME} \\
            --cache-from type=registry,ref=harbor.arpnetworking.com/${projectGroup}/${ingestProjectName}-cache:main \\
            --cache-to type=registry,ref=harbor.arpnetworking.com/${projectGroup}/${ingestProjectName}-cache:${env.BRANCH_NAME},image-manifest=true,mode=max \\
            --push .
            """
          }
        }
      }
    }
    
    stage('Deploy via ArgoCD') {
      when { expression { env.BRANCH_IS_PRIMARY == 'true' } }
      environment {
        ARGOCD_SERVER = "argocd.arpnetworking.com"
      }
      steps {
        withCredentials([string(credentialsId: 'argocd-token', variable: 'ARGOCD_AUTH_TOKEN')]) {
          sh "argocd app set kairosdb --parameter ingest.image.tag=${imgTag}"
          sh "argocd app sync kairosdb"
          sh "argocd app wait kairosdb --timeout 600"
        }
      }
    }
  }
  
  post {
    always {
      // Archive artifacts
      archiveArtifacts artifacts: 'target/clippy-report.json, target/doc/**', fingerprint: true, allowEmptyArchive: true
      
      recordIssues(
        enabledForFailure: false, aggregatingResults: true,
        tools: [
          cargo(pattern: 'target/clippy-report.json')
        ]
      )
    }
  }
}
