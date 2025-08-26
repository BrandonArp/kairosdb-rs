def date = new Date().format("yyyyMMdd.HHmm", TimeZone.getTimeZone('UTC'))
def short_sha = "000000"
def imgTag = ""
def projectGroup = "kairosdb"
def ingestProjectName = "ingest"
def queryProjectName = "query"
def sharedCacheName = "kairosdb-cache"

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
        sh "tilt up --namespace default < /dev/null > tilt.log 2>&1 &"
        sh "echo 'Tilt started, waiting for services to come up...'"
        sh "sleep 10"  // Give Tilt a moment to start
      }
    }

    stage('Unit Tests') {
      steps {
        sh "mkdir -p target/debug/"
        sh "cargo install cargo-make || echo 'cargo-make already installed'"
        sh "cargo check --message-format json > target/debug/check.json"
        sh "cargo clippy --all-targets --all-features --message-format json > target/debug/clippy.json || true"
        catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
          sh "cargo make ci"  // Uses cargo-make for consistent CI pipeline
        }
      }
    }

    stage('Integration Tests') {
      steps {
        script {
          echo "Checking Tilt status..."
          sh "tilt get all || true"
          
          echo "Waiting for all services to be ready..."
          sh "tilt wait --for=condition=Ready 'uiresource/cassandra' --timeout 300s"
          sh "tilt wait --for=condition=Ready 'uiresource/kairosdb' --timeout 300s" 
          sh "tilt wait --for=condition=Ready 'uiresource/kairosdb-ingest' --timeout 300s"
          sh "tilt wait --for=condition=Ready 'uiresource/kairosdb-query' --timeout 300s"
          
          echo "Verifying service health endpoints..."
          sh "curl --retry 10 --retry-delay 5 --retry-connrefused --fail http://localhost:8080/api/v1/health/check || (echo 'Java KairosDB health check failed' && exit 1)"
          sh "curl --retry 10 --retry-delay 5 --retry-connrefused --fail http://localhost:8081/health || (echo 'Rust Ingest health check failed' && exit 1)"
          sh "curl --retry 10 --retry-delay 5 --retry-connrefused --fail http://localhost:8082/health || (echo 'Rust Query health check failed' && exit 1)"
          
          echo "All services are healthy, running E2E tests..."
          catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
            sh "cargo make test-integration"  // Uses cargo-make for consistent integration tests
          }
          catchError(buildResult: 'FAILURE', stageResult: 'FAILURE') {
            sh "cargo make coverage-report"   // Uses cargo-make for coverage generation
          }
        }
      }
    }

    stage('Tear down Tilt') {
      steps {
        sh "tilt down"
        sh "k3d cluster delete k3d"
      }
    }
    
    stage('Publish Docker Images') {
      steps {
        script {
          withCredentials([usernamePassword(credentialsId: 'harbor-user', usernameVariable: 'HARBOR_USERNAME', passwordVariable: 'HARBOR_PASSWORD')]) {
            sh "echo \"\$HARBOR_PASSWORD\" | docker login --password-stdin -u '$HARBOR_USERNAME' harbor.arpnetworking.com"
            
            // Setup buildx context
            sh '''
            docker context create multiarch-context --docker "host=$DOCKER_HOST,ca=/certs/client/ca.pem,cert=/certs/client/cert.pem,key=/certs/client/key.pem" || echo "Context exists"
            docker buildx create --name multiarch --driver docker-container --platform linux/amd64,linux/arm64 --use multiarch-context || docker buildx use multiarch
            '''
            
            // Determine platform based on whether we're building a tag (release)
            def platforms = env.TAG_NAME ? "linux/amd64,linux/arm64" : "linux/amd64"
            echo "Building for platforms: ${platforms} (Tag: ${env.TAG_NAME ?: 'none'})"
            
            // Build ingest service using multi-target Dockerfile with shared cache
            sh """
            docker buildx build -f Dockerfile --target ingest -t harbor.arpnetworking.com/${projectGroup}/${ingestProjectName}:${imgTag} \\
            --platform ${platforms} \\
            --build-arg BUILDKIT_INLINE_CACHE=1 \\
            --cache-from type=registry,ref=harbor.arpnetworking.com/${projectGroup}/${sharedCacheName}:${env.BRANCH_NAME} \\
            --cache-from type=registry,ref=harbor.arpnetworking.com/${projectGroup}/${sharedCacheName}:main \\
            --cache-to type=registry,ref=harbor.arpnetworking.com/${projectGroup}/${sharedCacheName}:${env.BRANCH_NAME},image-manifest=true,mode=max \\
            --push .
            """
            
            // Build query service using multi-target Dockerfile with shared cache
            sh """
            docker buildx build -f Dockerfile --target query -t harbor.arpnetworking.com/${projectGroup}/${queryProjectName}:${imgTag} \\
            --platform ${platforms} \\
            --build-arg BUILDKIT_INLINE_CACHE=1 \\
            --cache-from type=registry,ref=harbor.arpnetworking.com/${projectGroup}/${sharedCacheName}:${env.BRANCH_NAME} \\
            --cache-from type=registry,ref=harbor.arpnetworking.com/${projectGroup}/${sharedCacheName}:main \\
            --push .
            """
          }
        }
      }
    }
    
  }
  
  post {
    always {
      // Archive artifacts
      archiveArtifacts artifacts: 'target/debug/*.json, target/doc/**, tilt.log', fingerprint: true, allowEmptyArchive: true
      
      // Coverage reporting
      recordCoverage(ignoreParsingErrors: true, tools: [[parser: 'COBERTURA', pattern: '**/target/llvm-cov-target/*cobertura*.xml']])
      
      // Code analysis
      recordIssues(
        enabledForFailure: true, aggregatingResults: true,
        tools: [
          junitParser(pattern: '**/target/nextest/ci/junit*.xml'),
          cargo(pattern: '**/target/debug/*.json')
        ]
      )
      
      // Cleanup Docker buildx builder
      script {
        try {
          sh "docker buildx rm multiarch || true"
        } catch (Exception e) {
          echo "Failed to cleanup buildx builder: ${e.getMessage()}"
        }
      }
    }
  }
}
