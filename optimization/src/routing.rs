//! Optimize query routing.

use std::collections::HashMap;

// use crate::{Error, Result};
use lp_modeler::dsl::{lp_sum, BoundableLp, LpContinuous, LpObjective, LpOperations, LpProblem};
use lp_modeler::solvers::{CbcSolver, SolverTrait};
use ndarray::{Array2, ArrayView2};

/// Represents techniques optimizing routing probabilities.
pub trait Optimizer {
    /// Optimize routing policy.
    fn optimize(&self, weights: ArrayView2<'_, f32>) -> Array2<f32>;
}

/// No optimization: returns the same weights as given on input.
pub struct IdentityOptimizer {}

impl Optimizer for IdentityOptimizer {
    fn optimize(&self, weights: ArrayView2<'_, f32>) -> Array2<f32> {
        weights.to_owned()
    }
}

/// Balances load with an LP program.
pub struct LpOptimizer;

fn format_dispatch_variable(shard: usize, node: usize) -> String {
    format!("d({},{})", shard, node)
}

fn parse_dispatch_variable(name: &str) -> Option<(usize, usize)> {
    match (name.find('('), name.find(','), name.find(')')) {
        (Some(open_bracket), Some(comma), Some(close_bracket)) => {
            if name.get(..open_bracket).unwrap() == "d" && name.get(close_bracket..).unwrap() == ")"
            {
                let shard = name
                    .get(open_bracket + 1..comma)
                    .and_then(|n| n.parse::<usize>().ok())?;
                let node = name
                    .get(comma + 1..close_bracket)
                    .and_then(|n| n.parse::<usize>().ok())?;
                Some((shard, node))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn result_to_array(dim: (usize, usize), result: &HashMap<String, f32>) -> Array2<f32> {
    let mut array = Array2::<f32>::from_elem(dim, 0.0);
    for (variable, value) in result {
        if let Some((shard, node)) = parse_dispatch_variable(variable) {
            *array
                .get_mut((node, shard))
                .expect("variable points out of bounds") = *value;
        }
    }
    array
}

impl Optimizer for LpOptimizer {
    fn optimize(&self, weights: ArrayView2<'_, f32>) -> Array2<f32> {
        let mut problem = LpProblem::new("Balance routing", LpObjective::Minimize);
        let z = LpContinuous::new("Z");
        problem += &z;
        let dispatch_variables: Vec<Vec<_>> = weights
            .gencolumns()
            .into_iter()
            .enumerate()
            .map(|(shard_id, weights)| {
                let shard_vars: Vec<_> = weights
                    .iter()
                    .enumerate()
                    .map(|(node_id, weight)| {
                        LpContinuous::new(&format_dispatch_variable(shard_id, node_id))
                            .lower_bound(0.0)
                            .upper_bound(if *weight == 0.0 { 0.0 } else { 1.0 })
                    })
                    .collect();
                problem += lp_sum(&shard_vars).equal(1.0);
                shard_vars
            })
            .collect();
        for node in 0..weights.nrows() {
            let node_vars: Vec<_> = dispatch_variables
                .iter()
                .map(|shard_vars| &shard_vars[node])
                .collect();
            problem += lp_sum(&node_vars).le(&z);
        }
        let solver = CbcSolver::new();
        let solution = solver.run(&problem).expect("failed to run the CBC solver");
        match solution.status {
            lp_modeler::solvers::Status::Optimal => {
                result_to_array(weights.dim(), &solution.results)
            }
            status => panic!("CBC solver failed: {:?}", status),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;

    fn test_dispatch(weights: ArrayView2<'_, f32>) {
        let optimizer = LpOptimizer;
        let dispatch = optimizer.optimize(weights);
        assert!(dispatch.iter().all(|&v| v >= 0.0 && v <= 1.0));
        dispatch
            .genrows()
            .into_iter()
            .map(|row| row.iter().sum::<f32>())
            .all(|s| approx::ulps_eq!(s, 1.0, max_ulps = 4, epsilon = f32::EPSILON));
    }

    fn assignment(length: usize, replicas: usize) -> impl Strategy<Value = Vec<bool>> {
        let v = std::iter::repeat(true)
            .take(replicas)
            .chain(std::iter::repeat(false))
            .take(length)
            .collect::<Vec<_>>();
        Just(v).prop_shuffle()
    }

    fn distr(length: usize, replicas: usize) -> impl Strategy<Value = Vec<f32>> {
        (
            assignment(length, replicas),
            prop::collection::vec(0.1_f32..1.0, replicas..=replicas),
        )
            .prop_map(|(assignment, mut probs)| {
                let norm: f32 = probs.iter().sum();
                for prob in probs.iter_mut() {
                    *prob /= norm;
                }
                let mut v = assignment
                    .into_iter()
                    .map(|a| if a { 1.0 } else { 0.0 })
                    .collect::<Vec<_>>();
                for (r, prob) in v.iter_mut().filter(|x| **x > 0.0).zip(probs) {
                    *r = prob;
                }
                v
            })
    }

    fn weights() -> impl Strategy<Value = Array2<f32>> {
        (2..10_usize, 2..10_usize, 2..=3_usize)
            .prop_flat_map(|(nodes, shards, replicas)| {
                (
                    Just(nodes),
                    Just(shards),
                    prop::collection::vec(distr(nodes, replicas), shards..=shards),
                )
            })
            .prop_map(|(nodes, shards, vecs)| {
                Array2::from_shape_vec((shards, nodes), vecs.into_iter().flatten().collect())
                    .expect("unable to create array")
                    .reversed_axes()
            })
    }

    proptest! {
        #[test]
        fn test_probabilistic_dispatch(weights in weights()) {
            test_dispatch(weights.view());
        }
    }
}
