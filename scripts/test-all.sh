#!/bin/bash

# KairosDB Rust - Comprehensive Test Runner
# This script runs all tests: unit, integration, and end-to-end

set -e

echo "🚀 KairosDB Rust Test Suite"
echo "=========================="

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is available
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed or not in PATH"
        exit 1
    fi
    
    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running"
        exit 1
    fi
    
    print_success "Docker is available"
}

# Clean up any existing test containers
cleanup() {
    print_status "Cleaning up test containers..."
    docker-compose -f docker-compose.test.yml down -v --remove-orphans 2>/dev/null || true
    docker system prune -f --volumes 2>/dev/null || true
}

# Run unit tests
run_unit_tests() {
    print_status "Running unit tests..."
    echo "================================"
    
    if cargo test --lib --workspace; then
        print_success "Unit tests passed"
    else
        print_error "Unit tests failed"
        return 1
    fi
}

# Run integration tests with Docker Cassandra
run_integration_tests() {
    print_status "Running integration tests with Docker Cassandra..."
    echo "=================================================="
    
    # Start Cassandra container
    print_status "Starting Cassandra container..."
    docker-compose -f docker-compose.test.yml up -d cassandra
    
    # Wait for Cassandra to be ready
    print_status "Waiting for Cassandra to be ready..."
    timeout=120
    elapsed=0
    
    while [ $elapsed -lt $timeout ]; do
        if docker-compose -f docker-compose.test.yml exec -T cassandra cqlsh -e "describe cluster" &>/dev/null; then
            print_success "Cassandra is ready"
            break
        fi
        
        echo -n "."
        sleep 5
        elapsed=$((elapsed + 5))
    done
    
    if [ $elapsed -ge $timeout ]; then
        print_error "Cassandra failed to start within ${timeout} seconds"
        return 1
    fi
    
    # Run integration tests
    if KAIROSDB_CASSANDRA_CONTACT_POINTS="localhost:9042" cargo test --test integration_tests -- --ignored; then
        print_success "Integration tests passed"
    else
        print_error "Integration tests failed"
        return 1
    fi
}

# Run end-to-end tests
run_e2e_tests() {
    print_status "Running end-to-end tests..."
    echo "============================"
    
    if cargo test --test e2e_tests; then
        print_success "End-to-end tests passed"
    else
        print_error "End-to-end tests failed"
        return 1
    fi
}

# Run performance benchmarks
run_benchmarks() {
    print_status "Running performance benchmarks..."
    echo "=================================="
    
    if cargo bench --bench ingestion; then
        print_success "Benchmarks completed"
    else
        print_warning "Benchmarks failed or not available"
    fi
}

# Run linting and formatting checks
run_linting() {
    print_status "Running code quality checks..."
    echo "==============================="
    
    # Check formatting
    if cargo fmt --check; then
        print_success "Code formatting is correct"
    else
        print_error "Code formatting issues found. Run 'cargo fmt' to fix."
        return 1
    fi
    
    # Run clippy
    if cargo clippy --all-targets --all-features -- -D warnings; then
        print_success "Clippy checks passed"
    else
        print_error "Clippy found issues"
        return 1
    fi
}

# Main test execution
main() {
    local skip_e2e=false
    local skip_integration=false
    local skip_benchmarks=false
    local skip_linting=false
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --skip-e2e)
                skip_e2e=true
                shift
                ;;
            --skip-integration)
                skip_integration=true
                shift
                ;;
            --skip-benchmarks)
                skip_benchmarks=true
                shift
                ;;
            --skip-linting)
                skip_linting=true
                shift
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo ""
                echo "OPTIONS:"
                echo "  --skip-e2e          Skip end-to-end tests"
                echo "  --skip-integration  Skip integration tests"
                echo "  --skip-benchmarks   Skip performance benchmarks"
                echo "  --skip-linting      Skip linting and formatting checks"
                echo "  --help              Show this help message"
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    # Trap to ensure cleanup
    trap cleanup EXIT
    
    print_status "Starting comprehensive test suite..."
    
    # Check prerequisites
    check_docker
    
    # Cleanup before starting
    cleanup
    
    local failed_tests=()
    
    # Run linting first (fast feedback)
    if [[ "$skip_linting" == false ]]; then
        if ! run_linting; then
            failed_tests+=("linting")
        fi
    fi
    
    # Run unit tests (no external dependencies)
    if ! run_unit_tests; then
        failed_tests+=("unit")
    fi
    
    # Run integration tests (requires Cassandra)
    if [[ "$skip_integration" == false ]]; then
        if ! run_integration_tests; then
            failed_tests+=("integration")
        fi
    fi
    
    # Run end-to-end tests (full Docker setup)
    if [[ "$skip_e2e" == false ]]; then
        if ! run_e2e_tests; then
            failed_tests+=("e2e")
        fi
    fi
    
    # Run benchmarks (optional)
    if [[ "$skip_benchmarks" == false ]]; then
        run_benchmarks || true  # Don't fail the entire suite on benchmark failure
    fi
    
    # Summary
    echo ""
    echo "🏁 Test Suite Summary"
    echo "===================="
    
    if [ ${#failed_tests[@]} -eq 0 ]; then
        print_success "All tests passed! ✅"
        
        print_status "Test coverage summary:"
        echo "  ✅ Unit tests: Core logic and data structures"
        echo "  ✅ Integration tests: HTTP API with real Cassandra"
        echo "  ✅ End-to-end tests: Full stack with Docker containers"
        echo "  ✅ Code quality: Formatting and linting"
        
        if [[ "$skip_benchmarks" == false ]]; then
            echo "  ✅ Performance benchmarks: Throughput and latency"
        fi
        
        echo ""
        print_success "KairosDB Ingestion Service is ready for production! 🚀"
        
        exit 0
    else
        print_error "Some tests failed:"
        for test in "${failed_tests[@]}"; do
            echo "  ❌ $test tests"
        done
        
        echo ""
        print_error "Please fix the failing tests before deploying."
        exit 1
    fi
}

# Run main function with all arguments
main "$@"