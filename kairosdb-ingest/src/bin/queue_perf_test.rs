//! Queue Performance Test
//!
//! Dedicated test to measure queue performance without full service overhead.
//! This helps isolate and optimize queue operations.

use kairosdb_core::datapoint::{DataPoint, DataPointBatch, DataPointValue};
use kairosdb_core::{MetricName, TagSet, Timestamp};
use kairosdb_ingest::persistent_queue::PersistentQueue;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::fs;
use tokio::time;
use tracing::{info, warn};

#[cfg(feature = "profiling")]
use pprof;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .init();

    info!("Starting Queue Performance Test");

    #[cfg(feature = "profiling")]
    let _guard = pprof::ProfilerGuard::new(1000)?;

    // Test parameters
    let num_batches = 1000;
    let points_per_batch = 100;
    let total_points = num_batches * points_per_batch;

    info!("Test parameters:");
    info!("  Batches: {}", num_batches);
    info!("  Points per batch: {}", points_per_batch);
    info!("  Total points: {}", total_points);

    // Create temporary queue
    let temp_dir = std::env::temp_dir().join("kairosdb_queue_test");
    if temp_dir.exists() {
        fs::remove_dir_all(&temp_dir)?;
    }
    fs::create_dir_all(&temp_dir)?;
    let queue: Arc<PersistentQueue> = Arc::new(PersistentQueue::new(&temp_dir).await?);

    info!("Queue initialized at: {:?}", temp_dir);

    // Generate test batches
    info!("Generating test batches...");
    let batches = generate_test_batches(num_batches, points_per_batch)?;
    info!("Generated {} batches", batches.len());

    // Test 1: Batch Enqueue Performance
    info!("\n=== Test 1: Batch Enqueue Performance ===");
    let enqueue_start = Instant::now();
    let mut enqueue_times = Vec::new();
    
    for (i, batch) in batches.iter().enumerate() {
        let batch_start = Instant::now();
        if let Err(e) = queue.enqueue_batch(batch.clone()) {
            warn!("Failed to enqueue batch {}: {}", i, e);
        }
        enqueue_times.push(batch_start.elapsed());
        
        // Removed frequent logging for performance
    }
    
    let enqueue_duration = enqueue_start.elapsed();
    let queue_size_after_enqueue = queue.size();
    
    info!("Enqueue Results:");
    info!("  Total time: {:?}", enqueue_duration);
    info!("  Batches/sec: {:.2}", num_batches as f64 / enqueue_duration.as_secs_f64());
    info!("  Points/sec: {:.2}", total_points as f64 / enqueue_duration.as_secs_f64());
    info!("  Queue size: {}", queue_size_after_enqueue);
    if num_batches > 0 {
        info!("  Avg enqueue time: {:?}", enqueue_duration / num_batches as u32);
        enqueue_times.sort();
        info!("  Enqueue p50: {:?}", enqueue_times[num_batches / 2]);
        info!("  Enqueue p99: {:?}", enqueue_times[num_batches * 99 / 100]);
        info!("  Enqueue min: {:?}", enqueue_times[0]);
        info!("  Enqueue max: {:?}", enqueue_times[num_batches - 1]);
    }

    // Test 2: Batch Dequeue Performance  
    info!("\n=== Test 2: Batch Dequeue Performance ===");
    let dequeue_start = Instant::now();
    let mut dequeued_batches = 0;
    let mut dequeued_points = 0;
    let mut dequeue_times = Vec::new();
    
    loop {
        let batch_start = Instant::now();
        match queue.claim_work_items_batch(1000, 1000) {
            Ok(work_items) if !work_items.is_empty() => {
                dequeue_times.push(batch_start.elapsed());
                
                let mut total_points = 0;
                for work_item in &work_items {
                    total_points += work_item.entry.batch.points.len();
                    // Remove processed items
                    if let Err(e) = queue.remove_processed_item(&work_item.queue_key, work_item.entry.batch.points.len()) {
                        eprintln!("Failed to remove processed item: {}", e);
                    }
                }
                
                dequeued_batches += work_items.len();
                dequeued_points += total_points;
                
                // Removed frequent logging for performance
            }
            Ok(_) => {
                info!("Queue empty, stopping dequeue test");
                break;
            }
            Err(e) => {
                warn!("Dequeue error: {}", e);
                break;
            }
        }
    }
    
    let dequeue_duration = dequeue_start.elapsed();
    let queue_size_after_dequeue = queue.size();
    
    info!("Dequeue Results:");
    info!("  Total time: {:?}", dequeue_duration);
    info!("  Batches dequeued: {}", dequeued_batches);
    info!("  Points dequeued: {}", dequeued_points);
    info!("  Batches/sec: {:.2}", dequeued_batches as f64 / dequeue_duration.as_secs_f64());
    info!("  Points/sec: {:.2}", dequeued_points as f64 / dequeue_duration.as_secs_f64());
    info!("  Queue size: {}", queue_size_after_dequeue);
    if dequeued_batches > 0 {
        info!("  Avg dequeue time: {:?}", dequeue_duration / dequeued_batches as u32);
        dequeue_times.sort();
        info!("  Dequeue p50: {:?}", dequeue_times[dequeued_batches / 2]);
        info!("  Dequeue p99: {:?}", dequeue_times[dequeued_batches * 99 / 100]);
        info!("  Dequeue min: {:?}", dequeue_times[0]);
        info!("  Dequeue max: {:?}", dequeue_times[dequeued_batches - 1]);
    }

    // Test 3: Mixed Workload (Producer/Consumer)
    info!("\n=== Test 3: Mixed Workload Test ===");
    let mixed_batches = generate_test_batches(500, points_per_batch)?;
    let queue_clone = queue.clone();
    
    let mixed_start = Instant::now();
    
    // Producer task
    let producer_handle = tokio::spawn(async move {
        for (i, batch) in mixed_batches.iter().enumerate() {
            if let Err(e) = queue_clone.enqueue_batch(batch.clone()) {
                warn!("Producer failed to enqueue batch {}: {}", i, e);
            }
            
            // Small delay to simulate realistic load
            if i % 10 == 0 {
                time::sleep(Duration::from_millis(1)).await;
            }
        }
        info!("Producer finished");
    });
    
    // Consumer task
    let queue_clone2 = queue.clone();
    let consumer_handle = tokio::spawn(async move {
        let mut consumed_batches = 0;
        let mut consumed_points = 0;
        
        loop {
            match queue_clone2.claim_work_items_batch(1000, 1000) {
                Ok(work_items) if !work_items.is_empty() => {
                    let mut total_points = 0;
                    for work_item in &work_items {
                        total_points += work_item.entry.batch.points.len();
                        // Remove processed items
                        if let Err(e) = queue_clone2.remove_processed_item(&work_item.queue_key, work_item.entry.batch.points.len()) {
                            eprintln!("Failed to remove processed item: {}", e);
                        }
                    }
                    consumed_batches += work_items.len();
                    consumed_points += total_points;
                }
                Ok(_) => {
                    // No items or empty result - check if producer is still running
                    time::sleep(Duration::from_millis(10)).await;
                    
                    // Try again a few times before giving up
                    if consumed_batches >= 500 {
                        break;
                    }
                }
                Err(e) => {
                    warn!("Consumer error: {}", e);
                    break;
                }
            }
        }
        
        info!("Consumer finished: {} batches, {} points", consumed_batches, consumed_points);
        (consumed_batches, consumed_points)
    });
    
    // Wait for both tasks
    let _ = producer_handle.await?;
    let (consumed_batches, consumed_points) = consumer_handle.await?;
    let mixed_duration = mixed_start.elapsed();
    
    info!("Mixed Workload Results:");
    info!("  Total time: {:?}", mixed_duration);
    info!("  Consumed batches: {}", consumed_batches);
    info!("  Consumed points: {}", consumed_points);
    info!("  Final queue size: {}", queue.size());

    // Test 4: Queue Metrics
    info!("\n=== Test 4: Queue Metrics ===");
    let metrics = queue.metrics();
    info!("Queue Metrics:");
    info!("  Enqueue ops: {}", metrics.enqueue_total.get());
    info!("  Dequeue ops: {}", metrics.dequeue_total.get());
    info!("  Disk usage: {:.2} MB", metrics.disk_usage_bytes.get() / 1_048_576.0);

    // Calculate disk usage
    if let Ok(disk_usage) = queue.get_disk_usage_bytes() {
        info!("  Actual disk usage: {:.2} MB", disk_usage as f64 / 1_048_576.0);
        info!("  Bytes per point: {:.2}", disk_usage as f64 / total_points as f64);
    }

    info!("\n=== Performance Test Complete ===");
    
    #[cfg(feature = "profiling")]
    if let Ok(report) = _guard.report().build() {
        let file = std::fs::File::create("flamegraph.svg")?;
        report.flamegraph(file)?;
        info!("Flamegraph saved to flamegraph.svg");
    }
    
    Ok(())
}

fn generate_test_batches(num_batches: usize, points_per_batch: usize) -> Result<Vec<DataPointBatch>, Box<dyn std::error::Error>> {
    let mut batches = Vec::with_capacity(num_batches);
    let base_timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    for batch_idx in 0..num_batches {
        let mut batch = DataPointBatch::new();
        
        for point_idx in 0..points_per_batch {
            let mut tags = TagSet::new();
            tags.insert("host".to_string().into(), format!("server{}", batch_idx % 10).into())?;
            tags.insert("region".to_string().into(), format!("region{}", batch_idx % 3).into())?;
            tags.insert("service".to_string().into(), "test-service".to_string().into())?;
            
            let data_point = DataPoint {
                metric: MetricName::from(format!("test.metric.{}", point_idx % 5)),
                timestamp: Timestamp::from_millis(base_timestamp + (batch_idx * points_per_batch + point_idx) as i64 * 1000)?,
                value: DataPointValue::Long((batch_idx * points_per_batch + point_idx) as i64),
                tags,
                ttl: 0, // No TTL
            };
            
            batch.points.push(data_point);
        }
        
        batches.push(batch);
    }
    
    Ok(batches)
}