use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use kairosdb_ingest::json_parser::JsonParser;
use serde_json::json;

fn create_simple_metrics(count: usize) -> String {
    let metrics: Vec<_> = (0..count)
        .map(|i| {
            json!({
                "name": format!("test.metric.{}", i),
                "datapoints": [
                    [1634567890000i64 + i as i64 * 1000, 42.5 + i as f64],
                    [1634567891000i64 + i as i64 * 1000, 43.5 + i as f64]
                ],
                "tags": {
                    "host": format!("server{}", i % 20),
                    "region": if i % 4 == 0 { "us-east-1" } else if i % 4 == 1 { "us-west-2" } else if i % 4 == 2 { "eu-central-1" } else { "ap-southeast-1" },
                    "service": if i % 3 == 0 { "web" } else if i % 3 == 1 { "api" } else { "worker" },
                    "datacenter": format!("dc{}", i % 8)
                }
            })
        })
        .collect();
    
    serde_json::to_string(&metrics).unwrap()
}

fn create_histogram_metrics(count: usize) -> String {
    let metrics: Vec<_> = (0..count)
        .map(|i| {
            json!({
                "name": format!("response_time_histogram.{}", i),
                "datapoints": [[
                    1634567890000i64 + i as i64 * 1000,
                    {
                        "boundaries": [0.1, 0.5, 1.0, 5.0, 10.0, 50.0],
                        "counts": [10 + i, 20 + i, 15 + i, 8 + i, 3 + i, 1 + i],
                        "total_count": 57 + i * 6,
                        "sum": 125.5 + i as f64 * 12.0,
                        "min": 0.05 + i as f64 * 0.001,
                        "max": 45.8 + i as f64 * 0.2
                    }
                ]],
                "tags": {
                    "service": format!("api{}", i % 10),
                    "endpoint": format!("/v1/endpoint{}", i % 50),
                    "method": if i % 4 == 0 { "GET" } else if i % 4 == 1 { "POST" } else if i % 4 == 2 { "PUT" } else { "DELETE" },
                    "status_code": format!("{}", 200 + i % 10)
                }
            })
        })
        .collect();
    
    serde_json::to_string(&metrics).unwrap()
}

fn create_prometheus_histogram_metrics(count: usize) -> String {
    let metrics: Vec<_> = (0..count)
        .map(|i| {
            json!({
                "name": format!("http_request_duration.{}", i),
                "datapoints": [[
                    1634567890000i64 + i as i64 * 1000,
                    {
                        "buckets": [
                            {"le": 0.1, "count": 5 + i * 2},
                            {"le": 0.5, "count": 15 + i * 4},
                            {"le": 1.0, "count": 30 + i * 6},
                            {"le": 5.0, "count": 45 + i * 8},
                            {"le": 10.0, "count": 50 + i * 9}
                        ],
                        "count": 50 + i * 9,
                        "sum": 85.5 + i as f64 * 15.0
                    }
                ]],
                "tags": {
                    "method": if i % 3 == 0 { "GET" } else if i % 3 == 1 { "POST" } else { "PUT" },
                    "handler": format!("handler_{}", i % 25),
                    "status": format!("{}", if i % 10 < 8 { 200 } else if i % 10 == 8 { 404 } else { 500 })
                }
            })
        })
        .collect();
    
    serde_json::to_string(&metrics).unwrap()
}

fn create_kairosdb_bins_histogram_metrics(count: usize) -> String {
    let metrics: Vec<_> = (0..count)
        .map(|i| {
            let mut bins = json!({});
            // Create bins with different boundaries for each metric
            for (j, boundary) in [0.1, 1.0, 5.0, 10.0, 50.0].iter().enumerate() {
                bins[boundary.to_string()] = json!(10 + i + j * 5);
            }
            
            json!({
                "name": format!("latency_histogram.{}", i),
                "datapoints": [[
                    1634567890000i64 + i as i64 * 1000,
                    {
                        "bins": bins,
                        "min": 0.05 + i as f64 * 0.01,
                        "max": 45.8 + i as f64 * 0.5,
                        "sum": 425.0 + i as f64 * 15.0,
                        "precision": 7
                    }
                ]],
                "tags": {
                    "endpoint": format!("/api/v{}", (i % 3) + 1),
                    "region": if i % 4 == 0 { "us-east" } else if i % 4 == 1 { "us-west" } else if i % 4 == 2 { "eu-central" } else { "ap-southeast" },
                    "datacenter": format!("dc{}", i % 8)
                }
            })
        })
        .collect();
    
    serde_json::to_string(&metrics).unwrap()
}

fn bench_simple_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_parsing");
    group.sample_size(1000); // 100x more samples for better statistics
    let parser = JsonParser::new(10_000, true);
    
    // Test different batch sizes to find performance characteristics
    for batch_size in [1, 10, 100, 1000, 5000].iter() {
        let json_data = create_simple_metrics(*batch_size);
        
        group.bench_with_input(
            BenchmarkId::new("simple_metrics", batch_size),
            &json_data,
            |b, data| {
                b.iter(|| {
                    parser.parse_json(black_box(data)).unwrap()
                })
            },
        );
    }
    group.finish();
}

fn bench_histogram_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("histogram_parsing");
    group.sample_size(1000); // 100x more samples for better statistics
    let parser = JsonParser::new(10_000, true);
    
    // Test histogram parsing with different batch sizes
    for batch_size in [1, 10, 100, 1000].iter() {
        let direct_data = create_histogram_metrics(*batch_size);
        let prometheus_data = create_prometheus_histogram_metrics(*batch_size);
        let kairosdb_data = create_kairosdb_bins_histogram_metrics(*batch_size);
        
        group.bench_with_input(
            BenchmarkId::new("direct_histogram", batch_size),
            &direct_data,
            |b, data| {
                b.iter(|| {
                    parser.parse_json(black_box(data)).unwrap()
                })
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("prometheus_histogram", batch_size),
            &prometheus_data,
            |b, data| {
                b.iter(|| {
                    parser.parse_json(black_box(data)).unwrap()
                })
            },
        );
        
        group.bench_with_input(
            BenchmarkId::new("kairosdb_bins_histogram", batch_size),
            &kairosdb_data,
            |b, data| {
                b.iter(|| {
                    parser.parse_json(black_box(data)).unwrap()
                })
            },
        );
    }
    group.finish();
}

fn bench_validation_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("validation_modes");
    group.sample_size(1000); // 100x more samples for better statistics
    let json_data = create_simple_metrics(1000); // 1000 metrics for meaningful measurement
    
    let strict_parser = JsonParser::new(10_000, true);
    group.bench_function("strict_validation", |b| {
        b.iter(|| {
            strict_parser.parse_json(black_box(&json_data)).unwrap()
        })
    });
    
    let lenient_parser = JsonParser::new(10_000, false);
    group.bench_function("lenient_validation", |b| {
        b.iter(|| {
            lenient_parser.parse_json(black_box(&json_data)).unwrap()
        })
    });
    
    group.finish();
}

criterion_group!(benches, bench_simple_parsing, bench_histogram_parsing, bench_validation_modes);
criterion_main!(benches);