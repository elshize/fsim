//! This module contains algebra-based calculations for optimizing replica assignmnt.

use crate::{Error, Result};
use nalgebra::{DMatrix, DVector};
use ordered_float::OrderedFloat;
use rand::seq::SliceRandom;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::HashSet;
use std::iter::repeat;

/// It consists of a set of function that use a common random number generator.
/// Implemented to avoid constantly passing the rng around.
pub struct Optimizer<'r, R: Rng> {
    rng: &'r mut R,
}

impl<'r, R: Rng> Optimizer<'r, R> {
    /// Constructs a new optimizer that uses `rng` as its random number generator.
    pub fn new(rng: &'r mut R) -> Self {
        Self { rng }
    }

    /// Finds the initial solution
    fn find_initial(
        &mut self,
        loads: &DMatrix<f32>,
        volumes: &DVector<f32>,
        replicas: &DVector<u32>,
        capacities: &DVector<f32>,
    ) -> Result<DMatrix<u32>> {
        let (num_machines, num_shards) = loads.shape();
        if num_machines != capacities.shape().0 {
            return Err(Error::InconsistentMachineCount);
        }
        if num_shards != volumes.shape().0 || num_shards != replicas.shape().0 {
            return Err(Error::InconsistentMachineCount);
        }
        let mut replicas: Vec<_> = replicas
            .iter()
            .copied()
            .enumerate()
            .flat_map(|(s, r)| repeat(s).take(r as usize))
            .collect();
        replicas.shuffle(&mut self.rng);
        let mut assignment = DMatrix::<u32>::zeros(num_machines, num_shards);
        'outer: for s in replicas {
            // Assign shard `s` replica to a machine.
            let volume = volumes[s];
            let mut machine_ids: Vec<_> = (0..num_machines).collect();
            // To keep it fair, we always check in different order.
            machine_ids.shuffle(&mut self.rng);
            let float_assignment = to_floats(&assignment);
            for m in machine_ids {
                let machine_load = loads.row(m).dot(&float_assignment.row(m));
                if assignment[(m, s)] == 0 && machine_load + volume <= capacities[m] {
                    assignment[(m, s)] = 1;
                    continue 'outer;
                }
            }
            return Err(Error::PossiblyInfeasible);
        }
        Ok(assignment)
    }

    fn neighbors(
        assignment: &DMatrix<u32>,
        volumes: &DVector<f32>,
        capacities: &DVector<f32>,
    ) -> Vec<DMatrix<u32>> {
        let mut neighbors: Vec<DMatrix<u32>> = Vec::new();
        let (num_machines, num_shards) = assignment.shape();
        let assgn_float = to_floats(&assignment);
        for shard in 0..num_shards {
            for from in (0..num_machines).filter(|&m| assignment[(m, shard)] == 1) {
                for to in (0..num_machines).filter(|&m| assignment[(m, shard)] == 0) {
                    let volume = volumes
                        .component_mul(&assgn_float.row(to).transpose())
                        .sum();
                    let additional_volume = assignment[(to, shard)] as f32 * volumes[shard];
                    if volume + additional_volume <= capacities[to] {
                        let mut neighbor = assignment.clone();
                        neighbor[(to, shard)] = 1;
                        neighbor[(from, shard)] = 0;
                        neighbors.push(neighbor);
                    }
                }
            }
        }
        neighbors
    }

    /// Find a replica assignment that tries to balance load across all machines, with respect to
    /// the following constraints:
    /// - any shard `s` has `replicas[s]` replicas assigned to different machines,
    /// - the sum of the volumes of shard replicas assigned to machine `m` doesn't exceed the
    /// machine's capacity `machine_capacities[m]`.
    ///
    /// # Errors
    ///
    /// An error will be returned if the vectors have inconsistent sizes. All `shard_*` vectors
    /// must have the same length, as well as `machine_*` vectors.
    ///
    /// An error will also be returned if an initial assignment cannot be found due to low machine
    /// capacities. Note that this error isn't equivalent to the problem being infeasible, because
    /// we use a greedy (although additionally randomized) method to find this initial assignment.
    pub fn optimize(
        &mut self,
        shard_probabilities: &DVector<f32>,
        shard_loads: &DVector<f32>,
        shard_volumes: &DVector<f32>,
        shard_replicas: &DVector<u32>,
        machine_inhibitions: &DVector<f32>,
        machine_capacities: &DVector<f32>,
    ) -> Result<DMatrix<u32>> {
        let load_matrix = machine_inhibitions
            * shard_loads.transpose()
            * DMatrix::<f32>::from_diagonal(&shard_probabilities);
        let initial_assignment = self.find_initial(
            &load_matrix,
            &shard_volumes,
            &shard_replicas,
            &machine_capacities,
        )?;
        let initial_score = max_load(&load_matrix, &initial_assignment);

        let mut queue = Vec::<(DMatrix<u32>, OrderedFloat<f32>)>::with_capacity(10);
        let mut solution = (initial_assignment.clone(), initial_score);
        let mut visited = HashSet::<DMatrix<u32>>::new();

        visited.insert(initial_assignment.clone());
        queue.push((initial_assignment, initial_score));
        while let Some((candidate, score)) = queue.pop() {
            println!("{}", score.0);
            if score < solution.1 {
                solution = (candidate.clone(), score);
            }
            let neighbors = Self::neighbors(&candidate, &shard_volumes, &machine_capacities);
            let mut neighbors: Vec<_> = neighbors
                .into_iter()
                .map(|n| {
                    let score = max_load(&load_matrix, &n);
                    (n, score)
                })
                .collect();
            neighbors.sort_by_key(|(_, score)| *score);
            neighbors.truncate(3);
            let neighbors = neighbors
                .into_iter()
                .filter(|(cand, _)| !visited.contains(cand))
                .collect::<Vec<_>>();
            for neighbor in neighbors {
                visited.insert(neighbor.0.clone());
                queue.push(neighbor);
            }
            queue.sort_by_key(|(_, score)| Reverse(*score));
        }
        Ok(solution.0)
    }

    /// TODO
    pub fn optimize_stochastic(
        &mut self,
        shard_probabilities: &DVector<f32>,
        shard_loads: &DVector<f32>,
        shard_volumes: &DVector<f32>,
        shard_replicas: &DVector<u32>,
        machine_inhibitions: &DVector<f32>,
        machine_capacities: &DVector<f32>,
    ) -> Result<DMatrix<u32>> {
        let load_matrix = machine_inhibitions
            * shard_loads.transpose()
            * DMatrix::<f32>::from_diagonal(&shard_probabilities);
        std::iter::repeat_with(|| {
            self.find_initial(
                &load_matrix,
                &shard_volumes,
                &shard_replicas,
                &machine_capacities,
            )
        })
        .filter_map(|a| {
            if let Ok(a) = a {
                let score = max_load(&load_matrix, &a);
                Some((a, score))
            } else {
                None
            }
        })
        .take(1000)
        .min_by_key(|(_, score)| *score)
        .map(|(a, _)| a)
        .ok_or(Error::PossiblyInfeasible)
    }
}

#[inline]
fn max_load(loads: &DMatrix<f32>, assignment: &DMatrix<u32>) -> OrderedFloat<f32> {
    OrderedFloat(
        loads
            .component_mul(&to_floats(&assignment))
            .column_sum()
            .max(),
    )
}

/// Convert int matrix to float matrix.
#[inline]
pub fn to_floats(matrix: &DMatrix<u32>) -> DMatrix<f32> {
    let (rows, columns) = matrix.shape();
    let data = matrix.data.as_vec().iter().map(|&v| v as f32).collect();
    DMatrix::from_vec(rows, columns, data)
}

#[cfg(test)]
mod test {
    use super::*;
    use proptest::prelude::*;
    use rand::SeedableRng;
    use rand_chacha::ChaChaRng;
    use rand_distr::{Distribution, Normal};

    #[test]
    fn test_load() {
        let load = max_load(
            &DMatrix::<f32>::from_rows(&[
                DVector::from_column_slice(&[0.1, 0.2, 0.3]).transpose(),
                DVector::from_column_slice(&[0.4, 0.5, 0.6]).transpose(),
            ]),
            &DMatrix::<u32>::from_rows(&[
                DVector::from_column_slice(&[1, 0, 1]).transpose(),
                DVector::from_column_slice(&[0, 1, 1]).transpose(),
            ]),
        );
        assert_eq!(load, OrderedFloat(1.1));
    }

    #[test]
    #[ignore]
    fn test_optimize() {
        let mut rng = ChaChaRng::seed_from_u64(1);
        let mut opt = Optimizer::new(&mut rng);
        opt.optimize(
            &DVector::<f32>::from_column_slice(&[0.1, 0.9]),
            &DVector::<f32>::from_column_slice(&[1.0, 1.0]),
            &DVector::<f32>::from_column_slice(&[1.0, 1.0]),
            &DVector::<u32>::from_column_slice(&[1, 1]),
            &DVector::<f32>::from_column_slice(&[1.0, 1.0, 1.0]),
            &DVector::<f32>::from_column_slice(&[1.0, 1.0, 1.0]),
        )
        .unwrap();
    }

    fn test_optimize_feasible(seed: u64, num_machines: usize, num_shards: usize, replicas: u32) {
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let mut opt = Optimizer::new(&mut rng);
        let loads = DVector::<f32>::repeat(num_shards, 1.0);
        let probabilities = DVector::<f32>::repeat(num_shards, 1.0);
        let volumes = DVector::<f32>::repeat(num_shards, 1.0);
        let replicas = DVector::<u32>::repeat(num_shards, replicas);
        let capacities = DVector::<f32>::repeat(num_machines, f32::MAX);
        let inhibitions = DVector::<f32>::repeat(num_machines, 1.0);
        let assignment = opt.optimize_stochastic(
            &probabilities,
            &loads,
            &volumes,
            &replicas,
            &inhibitions,
            &capacities,
        );
        //println!("{:?}", assignment);
        assert!(assignment.is_ok());
        let assignment = assignment.unwrap();
        println!(
            "{}",
            max_load(
                &(inhibitions * loads.transpose() * DMatrix::<f32>::from_diagonal(&probabilities)),
                &assignment
            )
        );
        for (c, &r) in replicas.iter().enumerate() {
            assert_eq!(r, assignment.column(c).sum());
        }
    }

    fn test_initial_feasible(seed: u64, num_machines: usize, num_shards: usize, replicas: u32) {
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let mut opt = Optimizer::new(&mut rng);
        let loads = DMatrix::<f32>::repeat(num_machines, num_shards, 1.0);
        let volumes = DVector::<f32>::repeat(num_shards, 1.0);
        let replicas = DVector::<u32>::repeat(num_shards, replicas);
        let capacities = DVector::<f32>::repeat(num_machines, f32::MAX);
        let assignment = opt.find_initial(&loads, &volumes, &replicas, &capacities);
        assert!(assignment.is_ok());
        let assignment = assignment.unwrap();
        for (c, &r) in replicas.iter().enumerate() {
            assert_eq!(r, assignment.column(c).sum());
        }
    }

    fn test_initial_any(seed: u64, num_machines: usize, num_shards: usize, replicas: u32) {
        let mut rng = ChaChaRng::seed_from_u64(seed);
        let mut lrng = ChaChaRng::from_rng(&mut rng).unwrap();
        let mut vrng = ChaChaRng::from_rng(&mut rng).unwrap();
        let mut crng = ChaChaRng::from_rng(&mut rng).unwrap();

        let non_negative = |v| if v <= 0.0 { 0.0 } else { v };

        let normal = Normal::new(1.0, 0.1).unwrap();
        let loads = DMatrix::<f32>::from_fn(num_machines, num_shards, move |_, _| {
            non_negative(normal.sample(&mut lrng))
        });
        let volumes = DVector::<f32>::from_fn(num_shards, move |_, _| {
            non_negative(normal.sample(&mut vrng))
        });
        let avg_load_per_machine = loads.sum() * replicas as f32 / num_machines as f32;
        let normal = Normal::new(avg_load_per_machine, 0.1).unwrap();
        let replicas = DVector::<u32>::repeat(num_shards, replicas);
        let capacities = DVector::<f32>::from_fn(num_machines, move |_, _| {
            1.0 + (1.0 - normal.sample(&mut crng)).abs()
        });

        let mut opt = Optimizer::new(&mut rng);
        if let Ok(assignment) = opt.find_initial(&loads, &volumes, &replicas, &capacities) {
            for (c, &r) in replicas.iter().enumerate() {
                assert_eq!(r, assignment.column(c).sum());
            }
            for (load, capacity) in to_floats(&assignment)
                .component_mul(&loads)
                .row_sum()
                .iter()
                .zip(&capacities)
            {
                assert!(load <= capacity);
            }
        }
    }

    #[test]
    fn test_optimize_feasible_prop() {
        test_optimize_feasible(1, 20, 100, 1);
    }

    proptest! {
        #[test]
        fn test_initial_feasible_prop(
            seed: u64,
            num_machines in 3..100_usize,
            num_shards in 3..100_usize,
            replicas in 1..3_u32,
        ) {
            test_initial_feasible(seed, num_machines, num_shards, replicas);
        }

        #[test]
        fn test_initial_prop(
            seed: u64,
            num_machines in 3..100_usize,
            num_shards in 3..100_usize,
            replicas in 1..3_u32,
        ) {
            test_initial_any(seed, num_machines, num_shards, replicas);
        }

        //#[test]
        //fn test_optimize_feasible_prop(
        //    seed: u64,
        //    //num_machines in 3..100_usize,
        //    //num_shards in 3..100_usize,
        //    //replicas in 1..3_u32,
        //    num_machines in 20..=20_usize,
        //    num_shards in 100..=100_usize,
        //    replicas in 3..=3_u32,
        //) {
        //    test_optimize_feasible(seed, num_machines, num_shards, replicas);
        //}
    }
}
