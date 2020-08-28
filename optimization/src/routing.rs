//! Optimize query routing.

use crate::{Error, Result};
use lp_modeler::dsl::{
    lp_sum,
    BoundableLp,
    LpContinuous,
    LpObjective,
    LpOperations,
    LpProblem, //Problem,
};
use lp_modeler::solvers::{CbcSolver, SolverTrait};
use nalgebra::{DMatrix, DVector};

/// Represents techniques optimizing routing probabilities.
pub trait Optimizer {
    /// Optimize routing policy.
    fn optimize(&self, routing: ProbabilityRouting) -> DMatrix<f32>;
}

/// No optimization: returns the same weights as given on input.
pub struct IdentityOptimizer {}

impl Optimizer for IdentityOptimizer {
    fn optimize(&self, routing: ProbabilityRouting) -> DMatrix<f32> {
        routing.nodes
    }
}

/// Balances load with an LP program.
pub struct LpOptimizer {}

impl Optimizer for LpOptimizer {
    fn optimize(&self, routing: ProbabilityRouting) -> DMatrix<f32> {
        let mut problem = LpProblem::new("Balance routing", LpObjective::Minimize);
        let z = LpContinuous::new("Z");
        problem += &z;
        let dispatch_variables: Vec<Vec<_>> = routing
            .nodes
            .row_iter()
            .enumerate()
            .map(|(shard, weights)| {
                let shard_vars: Vec<_> = weights
                    .iter()
                    .enumerate()
                    .map(|(node, _)| {
                        LpContinuous::new(&format!("d({}, {})", shard, node))
                            .lower_bound(0.0)
                            .upper_bound(1.0)
                    })
                    .collect();
                problem += lp_sum(&shard_vars).equal(1.0);
                shard_vars
            })
            .collect();
        for node in 0..routing.num_nodes() {
            let node_vars: Vec<_> = dispatch_variables
                .iter()
                .map(|shard_vars| &shard_vars[node])
                .collect();
            problem += lp_sum(&node_vars).le(&z);
        }
        let solver = CbcSolver::new();
        let (_, _result) = solver.run(&problem).unwrap();
        todo!()
    }
}

/// Sets up routing system for optimization.
pub struct ProbabilityRouting {
    nodes: DMatrix<f32>,
}

impl ProbabilityRouting {
    /// Constructs a routing instance with requested numbers of nodes and shards.
    #[must_use]
    pub fn new(nodes: usize, shards: usize) -> Self {
        Self {
            nodes: DMatrix::from_element(nodes, shards, 0.0),
        }
    }

    /// Constructs a routing instance from an assignment matrix.
    pub fn from_nodes<N, S, F>(nodes: N) -> Self
    where
        N: IntoIterator<Item = S>,
        S: IntoIterator<Item = F>,
        F: Into<f32>,
    {
        Self {
            nodes: DMatrix::from_columns(
                &nodes
                    .into_iter()
                    .map(|node| {
                        DVector::from_column_slice(
                            &node.into_iter().map(F::into).collect::<Vec<_>>()[..],
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
        }
    }

    /// Scale node weights. The larger the value, the slower the machine.
    ///
    /// # Errors
    ///
    /// An error will be returned if the number of factors passed to the functions does not match
    /// the number of nodes.
    pub fn scale_nodes(&mut self, factors: &[f32]) -> Result<&mut Self> {
        if factors.len() == self.num_nodes() {
            let vec = DVector::from_column_slice(factors);
            for (mut column, m) in self.nodes.column_iter_mut().zip(vec.iter()) {
                for elem in column.iter_mut() {
                    *elem *= m;
                }
            }
            Ok(self)
        } else {
            Err(Error::InconsistentMachineCount)
        }
    }

    /// Scale shard weights. The larger the value, the slower retrieval.
    ///
    /// # Errors
    ///
    /// An error will be returned if the number of factors passed to the functions does not match
    /// the number of shards.
    pub fn scale_shards(&mut self, factors: &[f32]) -> Result<&mut Self> {
        if factors.len() == self.num_shards() {
            let vec = DVector::from_row_slice(factors);
            for mut column in self.nodes.column_iter_mut() {
                for (a, b) in column.iter_mut().zip(vec.iter()) {
                    *a *= b;
                }
            }
            Ok(self)
        } else {
            Err(Error::InconsistentShardCount)
        }
    }

    /// Optimizes routing.
    pub fn optimize<O: Optimizer>(self, optimizer: &O) -> DMatrix<f32> {
        optimizer.optimize(self)
    }

    /// Number of shards.
    #[must_use]
    pub fn num_shards(&self) -> usize {
        self.nodes.nrows()
    }

    /// Number of nodes.
    #[must_use]
    pub fn num_nodes(&self) -> usize {
        self.nodes.ncols()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_construct_from_nodes() {
        let r = ProbabilityRouting::from_nodes(vec![vec![0.0]]);
        assert_eq!(r.num_shards(), 1);
        assert_eq!(r.num_nodes(), 1);

        let r = ProbabilityRouting::from_nodes(vec![vec![0.0, 0.0]]);
        assert_eq!(r.num_shards(), 2);
        assert_eq!(r.num_nodes(), 1);

        let r = ProbabilityRouting::from_nodes(vec![
            vec![0.0, 0.0],
            vec![0.0, 0.0],
            vec![0.0, 0.0],
            vec![0.0, 0.0],
        ]);
        assert_eq!(r.num_shards(), 2);
        assert_eq!(r.num_nodes(), 4);
    }

    #[test]
    #[should_panic]
    fn test_construct_from_invalid_nodes() {
        ProbabilityRouting::from_nodes(vec![
            vec![0.0, 0.0],
            vec![0.0, 0.0],
            vec![0.0],
            vec![0.0, 0.0],
        ]);
    }

    #[test]
    fn test_scale_nodes() {
        let mut r = ProbabilityRouting::from_nodes(vec![
            vec![1.0, 1.0, 0.0],
            vec![0.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0],
            vec![1.0, 0.0, 1.0],
        ]);
        assert!(r.scale_nodes(&[0.0]).is_err());
        r.scale_nodes(&[1.0, 2.0, 3.0, 4.0]).unwrap();
        assert_eq!(
            r.nodes,
            DMatrix::from_columns(&[
                DVector::from_column_slice(&[1.0, 1.0, 0.0]),
                DVector::from_column_slice(&[0.0, 2.0, 2.0]),
                DVector::from_column_slice(&[3.0, 3.0, 0.0]),
                DVector::from_column_slice(&[4.0, 0.0, 4.0]),
            ])
        );
    }

    #[test]
    fn test_scale_shards() {
        let mut r = ProbabilityRouting::from_nodes(vec![
            vec![1.0, 1.0, 0.0],
            vec![0.0, 1.0, 1.0],
            vec![1.0, 1.0, 0.0],
            vec![1.0, 0.0, 1.0],
        ]);
        assert!(r.scale_shards(&[0.0]).is_err());
        r.scale_shards(&[1.0, 2.0, 3.0]).unwrap();
        assert_eq!(
            r.nodes,
            DMatrix::from_columns(&[
                DVector::from_column_slice(&[1.0, 2.0, 0.0]),
                DVector::from_column_slice(&[0.0, 2.0, 3.0]),
                DVector::from_column_slice(&[1.0, 2.0, 0.0]),
                DVector::from_column_slice(&[1.0, 0.0, 3.0]),
            ])
        );
    }
}
