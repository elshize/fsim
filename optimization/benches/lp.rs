#![allow(unused)]

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ndarray::Array2;
use optimization::{LpOptimizer, Optimizer};
use std::path::PathBuf;

fn load_input() -> Array2<f32> {
    let input_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("benches")
        .join("weights.json");
    let input_file = std::fs::File::open(input_path).expect("unable to open input file");
    let weights: Vec<Vec<f32>> =
        serde_json::from_reader(input_file).expect("failed to parse input");
    Array2::from_shape_vec(
        (weights.len(), weights[0].len()),
        weights.iter().flatten().copied().collect::<Vec<_>>(),
    )
    .expect("invavlid nodes config")
}

pub fn lp_optimizer_benchmark(c: &mut Criterion) {
    let optimizer = LpOptimizer;
    let weights = load_input();
    c.bench_function("LP CW09B", |b| {
        b.iter(|| optimizer.optimize(black_box(weights.view())))
    });
}

criterion_group!(benches, lp_optimizer_benchmark);
criterion_main!(benches);
