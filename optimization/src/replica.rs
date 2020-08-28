use crate::{
    Error, MachineCapacities, MachineInhibitions, Result, ShardLoads, ShardProbabilities,
    ShardReplicas, ShardVolumes,
};
use ndarray::{Array2, ArrayView2, Axis};
use ordered_float::OrderedFloat;
use rand::distributions::WeightedIndex;
use rand::seq::SliceRandom;
use rand::Rng;
use rand_distr::{Distribution, Uniform};
use std::convert::TryFrom;
use std::iter::repeat;
use std::num::NonZeroUsize;

/// Assignment of shard replicas to machines.
///
/// In assignment `a`, `a[(m, s)] == true` iff a replica of shard `s` is assigned to machine `m`.
/// This structure wraps around a two-dimensional array of `bool`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaAssignment(Array2<bool>);

/// Dimension of an optimization problem.
#[derive(Debug, Copy, Clone)]
pub struct Dimension {
    /// Number of shards in the system.
    pub num_shards: usize,
    /// Number of machines containing replicas.
    pub num_machines: usize,
}

/// Builds the system configuration to solve replica assignment for.
pub struct AssignmentBuilder {
    loads: Option<ShardLoads>,
    volumes: Option<ShardVolumes>,
    replicas: Option<ShardReplicas>,
    probabilities: Option<ShardProbabilities>,
    capacities: Option<MachineCapacities>,
    inhibitions: Option<MachineInhibitions>,
    generations: usize,
    population: usize,
    dimension: Dimension,
}

/// Always sorted by max load so that the first assignment has the lowest max load.
#[derive(Debug)]
struct Population<'p, R: Rng> {
    population: Vec<(ReplicaAssignment, f32)>,
    loads: Array2<f32>,
    volumes: &'p ShardVolumes,
    replicas: &'p ShardReplicas,
    capacities: &'p MachineCapacities,
    rng: &'p mut R,
}

impl TryFrom<Array2<bool>> for ReplicaAssignment {
    type Error = Error;
    fn try_from(array: Array2<bool>) -> Result<Self> {
        match (array.len_of(Axis(0)), array.len_of(Axis(1))) {
            (0, _) => Err(Error::ZeroMachines),
            (_, 0) => Err(Error::ZeroShards),
            (_, _) => Ok(Self(array)),
        }
    }
}

/// Calculates the lowest possible mean machine load.
///
/// For each shard (column) `i`, we take the `replicas[i]` lowest loads, sum these up
/// for all shards, and divide by the number of machines (rows).
///
/// This value is the lowest possible load of the highest-laod machine in a balanced assignment.
/// This is because we select the lowest loads within each shard. If this happens to be a balanced
/// assignment, then the average is exactly equal to the optimal max-machine-load. Otherwise, the
/// average in the balanced assignment is higher, and therefore the max must be higher as well.
pub fn lower_bound_mean_machine_load(loads: ArrayView2<'_, f32>, replicas: &ShardReplicas) -> f32 {
    loads
        .gencolumns()
        .into_iter()
        .zip(replicas.iter())
        .map(|(col, &replicas)| -> f32 {
            let mut shard_loads: Vec<_> = col.iter().copied().collect();
            shard_loads.sort_by_key(|l| OrderedFloat(*l));
            shard_loads.into_iter().take(replicas as usize).sum()
        })
        .sum::<f32>()
        / loads.nrows() as f32
}

impl ReplicaAssignment {
    /// Constructs an empty assignment without any assigned replicas.
    pub fn empty(num_machines: NonZeroUsize, num_shards: NonZeroUsize) -> Self {
        Self(Array2::<bool>::default((
            num_machines.into(),
            num_shards.into(),
        )))
    }

    /// Constructs a random assignment.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the vectors are empty or if not all of the shard vectors are of
    /// equal size: `volumes` and `replicas`.
    pub fn random<R: Rng>(
        volumes: &ShardVolumes,
        replicas: &ShardReplicas,
        capacities: &MachineCapacities,
        rng: &mut R,
    ) -> Result<Self> {
        let num_shards = volumes.len();
        let num_machines = capacities.len();
        if num_shards != replicas.len() {
            return Err(Error::InconsistentShardCount);
        }
        let mut assgn = Self::empty(
            NonZeroUsize::new(num_machines).ok_or(Error::ZeroMachines)?,
            NonZeroUsize::new(num_shards).ok_or(Error::ZeroShards)?,
        );
        let mut replicas: Vec<_> = replicas
            .iter()
            .copied()
            .enumerate()
            .flat_map(|(s, r)| repeat(s).take(r as usize))
            .collect();
        replicas.shuffle(rng);
        'outer: for s in replicas {
            // Assign the replica of shard `s` to a machine.
            let volume = volumes.vec()[s];
            let mut machine_ids: Vec<_> = (0..num_machines).collect();
            // To keep it fair, we always check in different order.
            machine_ids.shuffle(rng);
            for m in machine_ids {
                let machine_volume: f32 = volumes.filter(assgn.0.row(m)).sum();
                if !assgn.0[(m, s)] && machine_volume + volume <= capacities.vec()[m] {
                    assgn.0[(m, s)] = true;
                    continue 'outer;
                }
            }
            return Err(Error::PossiblyInfeasible);
        }
        Ok(assgn)
    }

    /// Constructs a random assignment where the probabilities depend on the weights.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the vectors are empty or if not all of the shard vectors are of
    /// equal size: `volumes` and `replicas`.
    pub fn random_weighted<R: Rng>(
        loads: ArrayView2<'_, f32>,
        volumes: &ShardVolumes,
        replicas: &ShardReplicas,
        capacities: &MachineCapacities,
        rng: &mut R,
    ) -> Result<Self> {
        let num_shards = volumes.len();
        let num_machines = capacities.len();
        if num_shards != replicas.len() {
            return Err(Error::InconsistentShardCount);
        }
        let mut assgn = Self::empty(
            NonZeroUsize::new(num_machines).ok_or(Error::ZeroMachines)?,
            NonZeroUsize::new(num_shards).ok_or(Error::ZeroShards)?,
        );
        let mut load_cap = lower_bound_mean_machine_load(loads.clone(), replicas);
        let mut replicas: Vec<_> = replicas
            .iter()
            .copied()
            .enumerate()
            .flat_map(|(s, r)| repeat(s).take(r as usize))
            .collect();
        replicas.shuffle(rng);
        for shard in replicas {
            let unassigned_machine_weights: Vec<_> = assgn
                .0
                .column(shard)
                .iter()
                .enumerate()
                .map(|(m, &a)| {
                    if a {
                        0.0
                    } else {
                        load_cap - assgn.machine_load(m, &loads)
                    }
                })
                .collect();

            // Assign the replica of shard `s` to a machine.
            let weights = WeightedIndex::new(unassigned_machine_weights)
                .map_err(|_| Error::PossiblyInfeasible)?;
            let machine = weights.sample(rng);

            debug_assert!(!assgn.0[(machine, shard)]);
            assgn.0[(machine, shard)] = true;

            let overflow = assgn.machine_load(machine, &loads) - load_cap;
            if overflow > 0.0 {
                load_cap += overflow;
            }
        }
        Ok(assgn)
    }

    /// Returns the number of machines (rows).
    pub fn num_machines(&self) -> usize {
        self.0.len_of(Axis(0))
    }

    /// Returns the number of shards (columns).
    pub fn num_shards(&self) -> usize {
        self.0.len_of(Axis(1))
    }

    /// Randomly moves one replica to a different machine.
    ///
    /// For simplicity, in case when all machines have the randomly selected shard already
    /// assigned, this function will return the exact copy of `self`.
    /// Handling such case in any different way would unnecessarily complicate things
    /// and be inefficient, and this situation most likely never happens anyway
    /// (it would mean that a shard has as many replicas as there are machines, which is not a
    /// realistic scenario).
    /// The same will happen when there are no replica assigned at all.
    pub fn random_move<R: Rng>(&self, rng: &mut R) -> Self {
        let mut copy = self.clone();
        let shard = Uniform::new(0, self.num_shards()).sample(rng);
        let (zeroes, ones) = self.partition_column_by_value(shard);
        match (zeroes.choose(rng), ones.choose(rng)) {
            (Some(&to), Some(&from)) => {
                copy.0[[from, shard]] = false;
                copy.0[[to, shard]] = true;
            }
            _ => {}
        }
        copy
    }

    /// Swaps a randomly chosen shard replica with another replica in a different machine.
    /// If a swap is not possible, it returns the exact copy of the original.
    pub fn random_swap<R: Rng>(&self, rng: &mut R) -> Self {
        let mut copy = self.clone();
        let assigned: Vec<_> = (0..self.num_machines())
            .flat_map(|m| (0..self.num_shards()).map(move |s| (m, s)))
            .filter(|&coord| self.0[coord])
            .collect();
        if let Some(from) = assigned.choose(rng).copied() {
            let to_candidates: Vec<_> =
                assigned.into_iter().filter(|(_, s)| *s == from.1).collect();
            if let Some(to) = to_candidates.choose(rng).copied() {
                copy.0[from] = false;
                copy.0[to] = false;
                copy.0[(from.0, to.1)] = true;
                copy.0[(to.0, from.1)] = true;
            }
        }
        copy
    }

    fn machine_load(&self, machine: usize, loads: &ArrayView2<'_, f32>) -> f32 {
        loads
            .row(machine)
            .iter()
            .zip(self.0.row(machine))
            .filter_map(|(load, assgn)| if *assgn { Some(*load) } else { None })
            .sum()
    }

    fn machine_loads<'a>(
        &'a self,
        loads: &'a ArrayView2<'_, f32>,
    ) -> impl Iterator<Item = f32> + 'a {
        loads
            .genrows()
            .into_iter()
            .zip(self.0.genrows().into_iter())
            .map(move |(loads, assignments)| {
                loads
                    .into_iter()
                    .zip(assignments)
                    .filter_map(|(load, assgn)| if *assgn { Some(*load) } else { None })
                    .sum()
            })
    }

    /// Calculates the maximum load across all machines with respect to the the weights in the load
    /// matrix `loads`.
    pub fn max_machine_load(&self, loads: ArrayView2<'_, f32>) -> f32 {
        self.machine_loads(&loads)
            .map(OrderedFloat)
            .max()
            .unwrap_or(OrderedFloat(0.0))
            .0
    }

    /// Returns two vectors containing indices of the cells in the given column that have value
    /// `false` and `true`, respectively.
    fn partition_column_by_value(&self, shard: usize) -> (Vec<usize>, Vec<usize>) {
        (0..self.num_machines()).partition(|&m| self.0[(m, shard)] == false)
    }
}

impl<'p, R: Rng> Population<'p, R> {
    fn new(
        size: usize,
        loads: &'p ShardLoads,
        volumes: &'p ShardVolumes,
        replicas: &'p ShardReplicas,
        probabilities: &'p ShardProbabilities,
        capacities: &'p MachineCapacities,
        machine_inhibitions: &'p MachineInhibitions,
        rng: &'p mut R,
    ) -> Self {
        let loads = machine_inhibitions
            .0
            .view()
            .into_shape((machine_inhibitions.len(), 1))
            .expect("Always can reshape")
            .dot(
                &loads
                    .0
                    .view()
                    .into_shape((1, loads.len()))
                    .expect("Always can reshape"),
            )
            .dot(&Array2::from_diag(&probabilities.0));
        let population = std::iter::repeat_with(|| {
            ReplicaAssignment::random_weighted(loads.view(), &volumes, &replicas, &capacities, rng)
        })
        .filter_map(|a| {
            a.ok().map(|a| {
                let load = a.max_machine_load(loads.view());
                (a, load)
            })
        })
        .take(size)
        .collect();
        let mut population = Self {
            population,
            loads,
            volumes,
            replicas,
            capacities,
            rng,
        };
        population.sort();
        population
    }

    fn sort(&mut self) {
        self.population.sort_by_key(|(_, load)| OrderedFloat(*load));
    }

    fn top(&self) -> Option<&(ReplicaAssignment, f32)> {
        self.population.first()
    }

    /// Takes a third of population with the lowest max load and duplicates each of them twice:
    /// once by random move, once by random swap.
    fn breed(&mut self) {
        self.population.truncate(self.population.len() / 2);
        self.population.shuffle(self.rng);
        let rng = &mut self.rng;
        let loads = &self.loads;
        let moved: Vec<_> = self
            .population
            .iter()
            .map(|(p, _)| {
                let mutated = p.random_move(rng);
                let load = mutated.max_machine_load(loads.view());
                (mutated, load)
            })
            .collect();
        // let swapped: Vec<_> = self
        //     .population
        //     .iter()
        //     .map(|(p, _)| {
        //         let swapped = p.clone().random_swap(rng);
        //         let load = swapped.max_machine_load(loads.view());
        //         (swapped, load)
        //     })
        //     .collect();
        self.population.extend_from_slice(&moved);
        // self.population.extend_from_slice(&swapped);
        self.sort();
    }
}

macro_rules! builder_property {
    ($prop:ident, $t:ty, $comment:literal) => {
        /// $literal
        pub fn $prop(&mut self, $prop: $t) -> &mut Self {
            self.$prop = Some($prop);
            self
        }
    };
}

impl AssignmentBuilder {
    /// Initializes a builder for a problem of the given dimension (numbers of shards and
    /// machines).
    ///
    /// # Defaults
    ///
    /// By default, all loads, volumes, inhibitions, and probabilities are equal 1.0, while all
    /// capacities are `f32::MAX`.
    ///
    /// The default number of generations is 300 and the population size is 3000.
    ///
    /// Each shard will have only 1 replica by default.
    pub fn new(dimension: Dimension) -> Self {
        Self {
            loads: None,
            volumes: None,
            replicas: None,
            probabilities: None,
            capacities: None,
            inhibitions: None,
            generations: 1000,
            population: 2000,
            dimension,
        }
    }

    builder_property!(loads, ShardLoads, "Sets shard loads.");
    builder_property!(volumes, ShardVolumes, "Sets shard volumes.");
    builder_property!(replicas, ShardReplicas, "Sets shard replicas.");
    builder_property!(
        probabilities,
        ShardProbabilities,
        "Sets shard probabilities."
    );
    builder_property!(capacities, MachineCapacities, "Sets machine capacities.");
    builder_property!(inhibitions, MachineInhibitions, "Sets machine inhibitions.");

    /// Consumes the builder and returns an optimized replica assignment.
    pub fn assign<R: Rng>(mut self, rng: &mut R) -> Result<(ReplicaAssignment, f32)> {
        let Dimension {
            num_shards,
            num_machines,
        } = self.dimension;
        let loads = default_array(self.loads.take(), 1.0, num_shards);
        let volumes = default_array(self.volumes.take(), 1.0, num_shards);
        let replicas = default_array(self.replicas.take(), 1, num_shards);
        let probabilities = default_array(self.probabilities.take(), 1.0, num_shards);
        let capacities = default_array(self.capacities.take(), f32::MAX, num_machines);
        let inhibitions = default_array(self.inhibitions.take(), 1.0, num_machines);
        assign_replicas(
            loads,
            volumes,
            replicas,
            probabilities,
            capacities,
            inhibitions,
            rng,
            self.generations,
            self.population,
        )
    }
}

fn default_array<A, T>(array: Option<A>, value: T, len: usize) -> A
where
    T: Copy,
    A: std::iter::FromIterator<T>,
{
    array.unwrap_or_else(move || repeat(value).take(len).collect())
}

fn assign_replicas<R: Rng>(
    loads: ShardLoads,
    volumes: ShardVolumes,
    replicas: ShardReplicas,
    probabilities: ShardProbabilities,
    capacities: MachineCapacities,
    machine_inhibitions: MachineInhibitions,
    rng: &mut R,
    generations: usize,
    population: usize,
) -> Result<(ReplicaAssignment, f32)> {
    let mut population = Population::new(
        population,
        &loads,
        &volumes,
        &replicas,
        &probabilities,
        &capacities,
        &machine_inhibitions,
        rng,
    );
    let mut optimal = population.top().ok_or(Error::PossiblyInfeasible)?.clone();
    for generation in 0..generations {
        population.breed();
        let top = population.top().unwrap();
        if top.1 < optimal.1 {
            optimal = (top.0.clone(), top.1);
        }
        println!("{} {}", generation, optimal.1);
    }
    Ok((optimal.0.clone(), optimal.1))
}

#[cfg(test)]
mod test {
    use super::*;
    use ndarray::{arr1, arr2, Array};
    use proptest::prelude::*;
    use rand::SeedableRng;
    use rand_chacha::ChaChaRng;

    #[test]
    fn test_filter_vector() {
        let volumes = ShardVolumes::from(arr1(&[1.0, 2.0, 3.0, 4.0, 5.0]));
        let filtered: Vec<_> = volumes
            .filter(&[true, false, true, false, true])
            .copied()
            .collect();
        assert_eq!(&filtered, &[1.0, 3.0, 5.0]);
        let filtered: Vec<_> = volumes
            .filter(arr1(&[true, false, true, false, true]).iter())
            .copied()
            .collect();
        assert_eq!(&filtered, &[1.0, 3.0, 5.0]);
    }

    #[test]
    fn test_partition_column() {
        let assignment = ReplicaAssignment::try_from(arr2(&[
            [false, true, false],
            [false, true, true],
            [true, false, false],
            [true, false, true],
        ]))
        .unwrap();
        assert_eq!(assignment.num_shards(), 3);
        assert_eq!(assignment.num_machines(), 4);
        let (zeroes, ones) = assignment.partition_column_by_value(0);
        assert_eq!(&zeroes, &[0, 1]);
        assert_eq!(&ones, &[2, 3]);
        let (zeroes, ones) = assignment.partition_column_by_value(1);
        assert_eq!(&zeroes, &[2, 3]);
        assert_eq!(&ones, &[0, 1]);
        let (zeroes, ones) = assignment.partition_column_by_value(2);
        assert_eq!(&zeroes, &[0, 2]);
        assert_eq!(&ones, &[1, 3]);
    }

    #[test]
    fn test_max_machine_load() {
        let assignment = ReplicaAssignment::try_from(arr2(&[
            [false, true, false],
            [false, true, true],
            [true, false, false],
            [true, false, true],
        ]))
        .unwrap();
        let loads = arr2(&[
            [1.0, 2.0, 3.0],
            [4.0, 5.0, 6.0],
            [7.0, 8.0, 9.0],
            [10.0, 11.0, 12.0],
        ]);
        assert_eq!(assignment.max_machine_load(loads.view()), 22.0);
    }

    #[test]
    fn test_lower_bound_mean_machine_load() -> Result<()> {
        use ndarray::{arr1, arr2};
        let replicas = ShardReplicas::from(arr1(&[2, 2, 2, 2, 2])); // Each shard has 2 replicas.
        let loads = arr2(&[
            [2.0, 1.0, 2.0, 1.0, 1.0],
            [1.0, 2.0, 2.0, 1.0, 2.0],
            [2.0, 1.0, 2.0, 1.0, 2.0],
        ]);
        // Minimum machine loads are:
        //   3.0, 2.0, 4.0, 2.0, 3.0
        assert_eq!(
            lower_bound_mean_machine_load(loads.view(), &replicas),
            14.0 / 3.0
        );
        Ok(())
    }

    fn random_assignment() -> impl Strategy<Value = ReplicaAssignment> {
        ((2..10_usize), (20..50_usize)).prop_flat_map(|(num_machines, num_shards)| {
            prop::collection::vec(0..=1_u8, num_machines * num_shards).prop_map(move |vec| {
                let mut matrix = Array::from_shape_vec(
                    (num_machines, num_shards),
                    vec.into_iter().map(|v| v == 1).collect(),
                )
                .unwrap();
                for shard in 0..num_shards {
                    let mut column = matrix.column_mut(shard);
                    let count = column.iter().filter(|&v| *v).count();
                    if count == 0 || count == column.len() {
                        column[0] = !column[0];
                    }
                }
                ReplicaAssignment::try_from(matrix).unwrap()
            })
        })
    }

    fn random_loads() -> impl Strategy<Value = Array2<f32>> {
        ((4..10_usize), (20..50_usize)).prop_flat_map(|(num_machines, num_shards)| {
            prop::collection::vec(1.0..2.0_f32, num_machines * num_shards).prop_map(move |vec| {
                Array::from_shape_vec((num_machines, num_shards), vec).unwrap()
            })
        })
    }

    proptest! {
        #[test]
        fn test_random_weighted(
            seed: u64,
            loads in random_loads(),
        ) {
            let mut rng = ChaChaRng::seed_from_u64(seed);
            let assignment = ReplicaAssignment::random_weighted(
                loads.view(),
                &repeat(1.0).take(loads.ncols()).collect(),
                &repeat(3).take(loads.ncols()).collect(),
                &repeat(f32::MAX).take(loads.nrows()).collect(),
                &mut rng
            ).unwrap();

            println!("{:#?}", loads);
            println!("{:#?}", assignment);

            for col in assignment.0.gencolumns() {
                assert_eq!(col.iter().filter(|&&a| a).count(), 3);
            }

            let lower_bound = lower_bound_mean_machine_load(
                loads.view(),
                &repeat(3).take(loads.ncols()).collect()
            );

            println!("{} <=? {}", lower_bound, assignment.max_machine_load(loads.view()));
            assert!(lower_bound <= assignment.max_machine_load(loads.view()));
        }

        #[test]
        fn test_random_move(
            seed: u64,
            assgn in random_assignment()
        ) {
            let mut rng = ChaChaRng::seed_from_u64(seed);
            let moved = assgn.random_move(&mut rng);
            assert_ne!(moved, assgn);
            for shard in 0..assgn.num_shards() {
                assert_eq!(
                    assgn.0.column(shard).iter().filter(|&&v| v).count(),
                    moved.0.column(shard).iter().filter(|&&v| v).count()
                );
            }
        }

        #[test]
        fn test_random_swap(
            seed: u64,
            assgn in random_assignment()
        ) {
            let mut rng = ChaChaRng::seed_from_u64(seed);
            let moved = assgn.random_swap(&mut rng);
            for shard in 0..assgn.num_shards() {
                assert_eq!(
                    assgn.0.column(shard).iter().filter(|&&v| v).count(),
                    moved.0.column(shard).iter().filter(|&&v| v).count()
                );
            }
        }
    }
}
