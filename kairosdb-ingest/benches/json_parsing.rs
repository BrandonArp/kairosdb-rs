use criterion::{black_box, criterion_group, criterion_main, Criterion};
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
                    "host": format!("server{}", i % 10),
                    "region": if i % 2 == 0 { "us-east-1" } else { "us-west-2" },
                    "service": "web"
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
                        "boundaries": [0.1, 0.5, 1.0, 5.0, 10.0],
                        "counts": [10 + i, 20 + i, 15 + i, 8 + i, 2 + i],
                        "total_count": 55 + i * 5,
                        "sum": 125.5 + i as f64 * 10.0,
                        "min": 0.05 + i as f64 * 0.01,
                        "max": 9.8 + i as f64 * 0.1
                    }
                ]],
                "tags": {
                    "service": format!("api{}", i % 5),
                    "endpoint": format!("/endpoint{}", i % 20),
                    "method": if i % 3 == 0 { "GET" } else if i % 3 == 1 { "POST" } else { "PUT" }
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
                            {"le": 0.1, "count": 5 + i},
                            {"le": 0.5, "count": 15 + i * 2},
                            {"le": 1.0, "count": 25 + i * 3},
                            {"le": 5.0, "count": 30 + i * 4}
                        ],
                        "count": 30 + i * 4,
                        "sum": 45.5 + i as f64 * 5.0
                    }
                ]],
                "tags": {
                    "method": if i % 2 == 0 { "GET" } else { "POST" },
                    "status": format!("{}", 200 + i % 5),
                    "handler": format!("handler_{}", i % 10)
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

fn create_mixed_value_types_metrics(count: usize) -> String {
    let metrics: Vec<_> = (0..count)
        .map(|i| {
            let value = match i % 6 {
                0 => json!(42i64 + i as i64), // Long
                1 => json!(3.14159 + i as f64), // Double
                2 => json!(format!("text_value_{}", i)), // Text
                3 => json!(i % 2 == 0), // Boolean (converted to Long)
                4 => json!(format!("{}", 100 + i)), // String number
                5 => json!({"real": i as f64, "imaginary": (i + 1) as f64}), // Complex
                _ => json!(42),
            };
            
            json!({
                "name": format!("mixed.metric.{}", i),
                "datapoints": [
                    [1634567890000i64 + i as i64 * 1000, value]
                ],
                "tags": {
                    "type": match i % 6 {
                        0 => "long",
                        1 => "double", 
                        2 => "text",
                        3 => "boolean",
                        4 => "string_number",
                        5 => "complex",
                        _ => "unknown"
                    },
                    "batch": format!("batch_{}", i / 100)
                }
            })
        })
        .collect();
    
    serde_json::to_string(&metrics).unwrap()
}

fn bench_simple_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("simple_parsing");
    
    let parser = JsonParser::new(10_000, true);
    
    for size in [1, 10, 100, 1000].iter() {
        let json_data = create_simple_metrics(*size);
        
        group.bench_with_input(
            BenchmarkId::new("simple_metrics", size),
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
    
    let parser = JsonParser::new(10_000, true);
    
    for size in [1, 10, 100].iter() {
        // Direct format histograms
        let direct_data = create_histogram_metrics(*size);
        group.bench_with_input(
            BenchmarkId::new("direct_histograms", size),
            &direct_data,
            |b, data| {
                b.iter(|| {
                    parser.parse_json(black_box(data)).unwrap()
                })
            },
        );
        
        // Prometheus format histograms  
        let prometheus_data = create_prometheus_histogram_metrics(*size);
        group.bench_with_input(
            BenchmarkId::new("prometheus_histograms", size),
            &prometheus_data,
            |b, data| {
                b.iter(|| {
                    parser.parse_json(black_box(data)).unwrap()
                })
            },
        );
        
        // KairosDB bins format histograms
        let bins_data = create_kairosdb_bins_histogram_metrics(*size);
        group.bench_with_input(
            BenchmarkId::new("kairosdb_bins_histograms", size),
            &bins_data,
            |b, data| {
                b.iter(|| {
                    parser.parse_json(black_box(data)).unwrap()
                })
            },
        );
    }
    group.finish();
}

fn bench_mixed_value_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_value_types");
    
    let parser = JsonParser::new(10_000, true);
    
    for size in [1, 10, 100, 1000].iter() {
        let json_data = create_mixed_value_types_metrics(*size);
        
        group.bench_with_input(
            BenchmarkId::new("mixed_values", size),
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

fn bench_validation_modes(c: &mut Criterion) {
    let mut group = c.benchmark_group("validation_modes");
    
    let json_data = create_simple_metrics(100);
    
    // Strict validation (default)
    let strict_parser = JsonParser::new(10_000, true);
    group.bench_function("strict_validation", |b| {
        b.iter(|| {
            strict_parser.parse_json(black_box(&json_data)).unwrap()
        })
    });
    
    // Lenient validation
    let lenient_parser = JsonParser::new(10_000, false);
    group.bench_function("lenient_validation", |b| {
        b.iter(|| {
            lenient_parser.parse_json(black_box(&json_data)).unwrap()
        })
    });
    
    group.finish();
}

fn bench_batch_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_sizes");
    
    let parser = JsonParser::new(10_000, true);
    
    for batch_size in [1, 5, 10, 50, 100, 500, 1000].iter() {
        let json_data = create_simple_metrics(*batch_size);
        
        group.bench_with_input(
            BenchmarkId::new("batch_size", batch_size),
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

fn bench_tag_processing(c: &mut Criterion) {
    let mut group = c.benchmark_group("tag_processing");
    
    let parser = JsonParser::new(10_000, true);
    
    // Create metrics with varying numbers of tags
    for tag_count in [0, 2, 5, 10, 20].iter() {
        let metrics: Vec<_> = (0..100)
            .map(|i| {
                let mut tags = json!({});
                for j in 0..*tag_count {
                    tags[format!("tag{}", j)] = json!(format!("value{}_{}", j, i));
                }
                
                json!({
                    "name": format!("tagged.metric.{}", i),
                    "datapoints": [
                        [1634567890000i64 + i as i64 * 1000, 42.0 + i as f64]
                    ],
                    "tags": tags
                })
            })
            .collect();
        
        let json_data = serde_json::to_string(&metrics).unwrap();
        
        group.bench_with_input(
            BenchmarkId::new("tag_count", tag_count),
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

criterion_group!(
    benches,
    bench_simple_parsing,
    bench_histogram_parsing,
    bench_mixed_value_types,
    bench_validation_modes,
    bench_batch_sizes,
    bench_tag_processing
);
criterion_main!(benches);