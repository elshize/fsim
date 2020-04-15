//! Replica assignment generation.

use indexed_vec::{Idx, IndexVec};
use itertools::Itertools;
use rand::{Rng, SeedableRng};
use rand_distr::weighted::WeightedIndex;
use rand_distr::Distribution;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

/// Shard identifier.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShardId(usize);

/// Machine identifier.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MachineId(usize);

impl Idx for ShardId {
    fn new(v: usize) -> Self {
        Self(v)
    }
    fn index(self) -> usize {
        self.0
    }
}
impl Idx for MachineId {
    fn new(v: usize) -> Self {
        Self(v)
    }
    fn index(self) -> usize {
        self.0
    }
}

/// A distribution that changes with each sampling such that balanced system is preserved.
///
/// Each resource of type `T` gets a budget `n`. If the resource is sampled, its budget decreases
/// to `n - 1`. The interface is very similar to `rand::Distribution` but due to its mutable
/// nature, this distribution takes `&mut self` when sampling, and returns `Option<T>` because
/// the resources can run out.
///
/// # Example
///
/// ```
/// # use fsim::optimization::replica::BalancedDistribution;
/// # use rand_chacha::ChaChaRng;
/// # use rand::SeedableRng;
/// let resources = vec![0_u32, 0, 1, 1, 2, 2];
/// let distr: BalancedDistribution<_> = resources.iter().copied().collect();
/// let mut sampled: Vec<_> = distr.sample_iter(ChaChaRng::from_entropy()).collect();
/// sampled.sort();
/// assert_eq!(sampled, resources);
/// ```
pub struct BalancedDistribution<T> {
    resources: Vec<T>,
}

impl<A> std::iter::FromIterator<A> for BalancedDistribution<A> {
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = A>,
    {
        Self {
            resources: iter.into_iter().collect(),
        }
    }
}

impl<T: Sized> BalancedDistribution<T> {
    /// Sample single value.
    pub fn sample<R: Rng + ?Sized>(&mut self, rng: &mut R) -> Option<T> {
        if self.resources.is_empty() {
            None
        } else {
            let idx = rng.gen::<usize>() % self.resources.len();
            Some(self.resources.swap_remove(idx))
        }
    }
    /// Convert to iterator of elements in order of selection.
    pub fn sample_iter<R>(mut self, mut rng: R) -> impl Iterator<Item = T>
    where
        R: Rng,
        Self: Sized,
    {
        std::iter::from_fn(move || self.sample(&mut rng))
    }
}

/// [`BalancedDistribution`] that ensures that one shard is assigned to distinct machines.
///
/// [`BalancedDistribution`]: trait.BalancedDistribution.html
pub struct BalancedWithDistinctMachines<T: std::hash::Hash> {
    counts: HashMap<T, usize>,
}

impl<A: std::hash::Hash + Eq + Copy> std::iter::FromIterator<A>
    for BalancedWithDistinctMachines<A>
{
    fn from_iter<T>(iter: T) -> Self
    where
        T: IntoIterator<Item = A>,
    {
        let mut counts: HashMap<A, usize> = HashMap::default();
        for r in iter {
            *counts.entry(r).or_default() += 1;
        }
        Self { counts }
    }
}

impl<T: std::hash::Hash + Eq + Copy> BalancedWithDistinctMachines<T> {
    /// Sample single value.
    ///
    /// # Errors
    ///
    /// See [struct-level documentation](struct.BalancedWithDistinctMachines.html).
    pub fn sample<R: Rng + ?Sized>(
        &mut self,
        rng: &mut R,
        blacklist: &HashSet<T>,
    ) -> Result<T, ()> {
        let resources: Vec<_> = self
            .counts
            .iter()
            .flat_map(|(t, &count)| {
                std::iter::repeat(t).take(if blacklist.contains(t) { 0 } else { count })
            })
            .collect();
        if resources.is_empty() {
            Err(())
        } else {
            let elem = *resources[rng.gen::<usize>() % resources.len()];
            if let Entry::Occupied(mut occupied) = self.counts.entry(elem) {
                let value = {
                    let value = occupied.get_mut();
                    *value -= 1;
                    *value
                };
                if value == 0 {
                    occupied.remove();
                }
            }
            Ok(elem)
        }
    }
}

/// This is similar to [`BalancedWithDistinctMachines`] but takes into account differences
/// in weights between the shards and machines.
///
/// First, all machine budgets and shard costs are normalized such that 1 cost equals 1 buget.
/// Each time a machine is selected for a shard of weight `w`, the first step is to select randomly
/// (but remaining budget distribution) a machine that:
/// 1. does not have the current shard assigned,
/// 2. has remaining budget above `w`.
///
/// In case none such machine exists, one is selected out of all that still have a positive budget.
///
/// [`BalancedWithDistinctMachines`]: trait.BalancedWithDistinctMachines.html
pub struct FloatBalancedWithDistinctMachines<T: std::hash::Hash> {
    counts: HashMap<T, f32>,
}

impl<T: std::hash::Hash + Eq + Copy> FloatBalancedWithDistinctMachines<T> {
    /// Constructs new distribution.
    #[must_use]
    pub fn new(counts: HashMap<T, f32>) -> Self {
        Self { counts }
    }

    /// Sample single value.
    ///
    /// # Errors
    ///
    /// See [struct-level documentation](struct.FloatBalancedWithDistinctMachines.html).
    pub fn sample<R: Rng + ?Sized>(
        &mut self,
        rng: &mut R,
        cost: f32,
        blacklist: &HashSet<T>,
    ) -> Result<T, ()> {
        let resources: Vec<_> = {
            let mut resources: Vec<_> = self
                .counts
                .iter()
                .filter(|(t, &w)| !blacklist.contains(t) && w >= cost)
                .collect();
            if resources.is_empty() {
                resources = self
                    .counts
                    .iter()
                    .filter(|(t, _)| !blacklist.contains(t))
                    .collect();
            }
            resources
        };

        if resources.is_empty() {
            Err(())
        } else {
            let dist = WeightedIndex::new(resources.iter().map(|(_, &w)| w)).unwrap();
            let elem = *resources[dist.sample(rng)].0;
            if let Entry::Occupied(mut occupied) = self.counts.entry(elem) {
                let value = {
                    let value = occupied.get_mut();
                    *value -= cost;
                    *value
                };
                if value <= 0.0 {
                    occupied.remove();
                }
            }
            Ok(elem)
        }
    }
}

/// Shard description.
struct Shard {
    replicas: usize,
    weight: f32,
}

/// Machine description.
struct Machine {
    weight: f32,
}

trait AssignReplicas {
    #[must_use]
    fn assign_replicas(
        &self,
        shards: &[Shard],
        machines: &[Machine],
    ) -> IndexVec<ShardId, Vec<MachineId>>;
}

/// The simplest random assignment that simply uniformly selects machines for replicas.
///
/// This strategy will likely produce slightly unbalanced assignments (unequal numbers of replicas
/// per machine) and can assign two replicas of the same shard to one machine, which is
/// undesirable.
pub struct JustRandomReplicas<R, D, N>
where
    R: Rng,
    D: Distribution<usize>,
    N: Fn(usize, usize) -> D,
{
    rng: RefCell<R>,
    new_dist: N,
}

impl<R, D, N> JustRandomReplicas<R, D, N>
where
    R: Rng,
    D: Distribution<usize>,
    N: Fn(usize, usize) -> D,
{
    /// Constructs a new generator form a random number generator and a function constructing
    /// a distribution on a range.
    pub fn new(rng: R, new_dist: N) -> Self {
        Self {
            rng: RefCell::new(rng),
            new_dist,
        }
    }
}

impl<R, D, N> AssignReplicas for JustRandomReplicas<R, D, N>
where
    R: Rng,
    D: Distribution<usize>,
    N: Fn(usize, usize) -> D,
{
    fn assign_replicas(
        &self,
        shards: &[Shard],
        machines: &[Machine],
    ) -> IndexVec<ShardId, Vec<MachineId>> {
        let num_machines = machines.len();
        shards
            .iter()
            .map(|s| {
                (0..s.replicas)
                    .map(|_| {
                        let distr = (self.new_dist)(0, num_machines);
                        MachineId(self.rng.borrow_mut().sample(distr))
                    })
                    .collect()
            })
            .collect()
    }
}

fn generate_discrete_resources<T>(shards: &[Shard], num_machines: usize) -> T
where
    T: std::iter::FromIterator<MachineId>,
{
    let total_replicas: usize = shards.iter().map(|s| s.replicas).sum();
    let replicas_per_shards = (total_replicas + num_machines - 1) / num_machines;
    let tail = num_machines * replicas_per_shards - total_replicas;
    let head = num_machines - tail;
    (0..replicas_per_shards * head)
        .chunks(replicas_per_shards)
        .into_iter()
        .flat_map(|machine_chunk| machine_chunk.map(|n| MachineId(n / replicas_per_shards)))
        .chain(
            (0..(replicas_per_shards - 1) * tail)
                .chunks(replicas_per_shards - 1)
                .into_iter()
                .flat_map(|machine_chunk| {
                    machine_chunk.map(|n| MachineId(head + (n / (replicas_per_shards - 1))))
                }),
        )
        .collect()
}

/// Replica assignment policy that ensures the load to be balanced but have no other constraints.
/// In particular, it is possible that two or more replicas of the same shards will be assigned
/// to the same machine.
pub struct BalancedRandomReplicas<R>
where
    R: SeedableRng,
{
    rng: RefCell<R>,
}

impl<R> BalancedRandomReplicas<R>
where
    R: SeedableRng,
{
    /// Constructs a new generator form a random number generator.
    pub fn new(rng: R) -> Self {
        Self {
            rng: RefCell::new(rng),
        }
    }
}

impl<R> AssignReplicas for BalancedRandomReplicas<R>
where
    R: SeedableRng + rand::RngCore,
{
    fn assign_replicas(
        &self,
        shards: &[Shard],
        machines: &[Machine],
    ) -> IndexVec<ShardId, Vec<MachineId>> {
        let distr: BalancedDistribution<_> = generate_discrete_resources(shards, machines.len());
        let sampled =
            RefCell::new(distr.sample_iter(R::from_rng(&mut (*self.rng.borrow_mut())).unwrap()));
        shards
            .iter()
            .map(|s| sampled.borrow_mut().by_ref().take(s.replicas).collect())
            .collect()
    }
}

/// This policy ensures that:
/// 1. The maximum number of replicas per machine `M` is at most `m + 1` where `m` is the minimum.
/// 2. No machine has replicas of the same shard.
///
/// The algorithm is greedy, and therefore can fail. `assign_replicas` will panic if the algorithm
/// filed 20 times.
pub struct DistinctMachinesBalancedRandomReplicas<R>
where
    R: SeedableRng,
{
    rng: RefCell<R>,
}

impl<R> DistinctMachinesBalancedRandomReplicas<R>
where
    R: SeedableRng + rand::RngCore,
{
    /// Constructs a new generator form a random number generator.
    pub fn new(rng: R) -> Self {
        Self {
            rng: RefCell::new(rng),
        }
    }

    fn try_assign_replicas(
        &self,
        shards: &[Shard],
        machines: &[Machine],
    ) -> Result<IndexVec<ShardId, Vec<MachineId>>, ()> {
        let mut distr: BalancedWithDistinctMachines<_> =
            generate_discrete_resources(shards, machines.len());
        shards
            .iter()
            .map(|s| {
                let mut machines = HashSet::<MachineId>::new();
                for _ in 0..s.replicas {
                    match distr.sample(&mut (*self.rng.borrow_mut()), &machines) {
                        Ok(m) => {
                            machines.insert(m);
                        }
                        Err(()) => return Err(()),
                    }
                }
                Ok(machines.into_iter().collect::<Vec<_>>())
            })
            .collect()
    }
}

impl<R> AssignReplicas for DistinctMachinesBalancedRandomReplicas<R>
where
    R: SeedableRng + rand::RngCore,
{
    fn assign_replicas(
        &self,
        shards: &[Shard],
        machines: &[Machine],
    ) -> IndexVec<ShardId, Vec<MachineId>> {
        for _ in 0..10 {
            if let Ok(assgn) = self.try_assign_replicas(shards, machines) {
                return assgn;
            }
        }
        panic!("Unable to assign replicas to distinct machines after 10 tries");
    }
}

fn machine_costs(
    shards: &[Shard],
    machines: &[Machine],
    assignment: &IndexVec<ShardId, Vec<MachineId>>,
) -> Vec<f64> {
    let mut machine_costs = vec![0_f64; machines.len()];
    for (shard_id, machine_ids) in assignment.into_iter().enumerate() {
        for mid in machine_ids {
            machine_costs[mid.index()] +=
                f64::from(shards[shard_id].weight) / f64::from(machines[mid.index()].weight);
        }
    }
    machine_costs
}

/// Replica assignment policy that utilizes [`FloatBalancedWithDistinctMachines`].
/// The assignment will fail if the standard deviation of the assigned costs divided by
/// the average cost is above 0.2. If the assignment fails after 20 tries, it will panic.
///
/// [`FloatBalancedWithDistinctMachines`]: struct.FloatBalancedWithDistinctMachines.html
pub struct WeightedBalancedRandomReplicas<R>
where
    R: SeedableRng,
{
    rng: RefCell<R>,
}

impl<R> WeightedBalancedRandomReplicas<R>
where
    R: SeedableRng + rand::RngCore,
{
    /// Constructs a new generator form a random number generator.
    pub fn new(rng: R) -> Self {
        Self {
            rng: RefCell::new(rng),
        }
    }

    fn try_assign_replicas(
        &self,
        shards: &[Shard],
        machines: &[Machine],
    ) -> Result<IndexVec<ShardId, Vec<MachineId>>, ()> {
        use statrs::statistics::Statistics;
        let total_budget: f32 = shards.iter().map(|s| s.weight * s.replicas as f32).sum();
        let normalized_machine_weights: HashMap<_, _> = {
            let total_weight: f32 = machines.iter().map(|m| m.weight).sum();
            machines
                .iter()
                .enumerate()
                .map(|(id, m)| (MachineId::new(id), total_budget * (m.weight / total_weight)))
                .collect()
        };
        let mut distr = FloatBalancedWithDistinctMachines::new(normalized_machine_weights);
        let assignment: Result<IndexVec<ShardId, Vec<MachineId>>, ()> = shards
            .iter()
            .map(|s| {
                let mut machines = HashSet::<MachineId>::new();
                for _ in 0..s.replicas {
                    match distr.sample(&mut (*self.rng.borrow_mut()), s.weight, &machines) {
                        Ok(m) => {
                            machines.insert(m);
                        }
                        Err(()) => return Err(()),
                    }
                }
                Ok(machines.into_iter().collect::<Vec<_>>())
            })
            .collect();
        assignment.and_then(|a| {
            let costs = machine_costs(shards, machines, &a);
            let sum = costs.iter().sum::<f64>() / costs.len() as f64;
            if costs.std_dev() / sum <= 0.02 {
                Ok(a)
            } else {
                Err(())
            }
        })
    }
}

impl<R> AssignReplicas for WeightedBalancedRandomReplicas<R>
where
    R: SeedableRng + rand::RngCore,
{
    fn assign_replicas(
        &self,
        shards: &[Shard],
        machines: &[Machine],
    ) -> IndexVec<ShardId, Vec<MachineId>> {
        for _ in 0..20 {
            if let Ok(assgn) = self.try_assign_replicas(shards, machines) {
                return assgn;
            }
        }
        panic!("Unable to assign replicas to distinct machines after 20 tries");
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::test::WrappingEchoDistribution;
    use ordered_float::OrderedFloat;
    use proptest::prelude::*;
    use rand::{rngs::mock::StepRng, SeedableRng};
    use rand_chacha::ChaChaRng;

    #[test]
    fn test_just_random_replicas() {
        let shards: Vec<_> = (0..5)
            .map(|_| Shard {
                replicas: 3,
                weight: 1.0,
            })
            .collect();
        let machines: Vec<_> = (0..4).map(|_| Machine { weight: 1.0 }).collect();
        let assignment: Vec<_> =
            JustRandomReplicas::new(StepRng::new(0, 1), |_, m| WrappingEchoDistribution::new(m))
                .assign_replicas(&shards, &machines)
                .into_iter()
                .collect();
        assert_eq!(
            assignment,
            vec![
                vec![MachineId::new(0), MachineId::new(1), MachineId::new(2)],
                vec![MachineId::new(3), MachineId::new(0), MachineId::new(1)],
                vec![MachineId::new(2), MachineId::new(3), MachineId::new(0)],
                vec![MachineId::new(1), MachineId::new(2), MachineId::new(3)],
                vec![MachineId::new(0), MachineId::new(1), MachineId::new(2)],
            ]
        );
        let assignment: Vec<_> =
            JustRandomReplicas::new(StepRng::new(0, 2), |_, m| WrappingEchoDistribution::new(m))
                .assign_replicas(&shards, &machines)
                .into_iter()
                .collect();
        assert_eq!(
            assignment,
            vec![
                vec![MachineId::new(0), MachineId::new(2), MachineId::new(0)],
                vec![MachineId::new(2), MachineId::new(0), MachineId::new(2)],
                vec![MachineId::new(0), MachineId::new(2), MachineId::new(0)],
                vec![MachineId::new(2), MachineId::new(0), MachineId::new(2)],
                vec![MachineId::new(0), MachineId::new(2), MachineId::new(0)],
            ]
        );
    }

    #[test]
    fn test_generate_discrete_resources() {
        let shards: Vec<_> = (0..5)
            .map(|_| Shard {
                replicas: 3,
                weight: 1.0,
            })
            .collect();
        let num_machines = 4;
        let distr: BalancedDistribution<_> = generate_discrete_resources(&shards, num_machines);
        assert_eq!(
            distr.resources,
            vec![0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3]
                .into_iter()
                .map(MachineId::new)
                .collect::<Vec<_>>()
        );
    }

    proptest! {
        #[test]
        fn test_balanced_distribution(
            seed: u64,
            mut resources in proptest::collection::vec(0..100, 0..20)
        ) {
            let rng = ChaChaRng::seed_from_u64(seed);
            let distr: BalancedDistribution<_> = resources.iter().copied().collect();
            let mut sampled: Vec<_> = distr.sample_iter(rng).collect();
            sampled.sort();
            resources.sort();
            assert_eq!(sampled, resources);
        }
    }

    proptest! {
        #[test]
        fn test_distinct_machines_balanced_random_replicas(seed: u64) {
            let replicas = 3;
            let num_machines = 20;
            let num_shards = 190;
            let rng = ChaChaRng::seed_from_u64(seed);
            let distr = DistinctMachinesBalancedRandomReplicas::new(rng);
            let shards: Vec<_> = std::iter::repeat(replicas).map(|r| Shard {
                replicas: r,
                weight: 1.0,
            }).take(num_shards).collect();
            let machines: Vec<_> = (0..num_machines).map(|_| Machine { weight: 1.0 }).collect();
            let assignment = distr.assign_replicas(&shards, &machines);
            let mut machine_counts = vec![0; num_machines];
            for mut machines in assignment.into_iter() {
                assert_eq!(machines.len(), replicas);
                machines.sort();
                machines.dedup();
                assert_eq!(machines.len(), replicas);
                for mid in machines {
                    machine_counts[mid.index()] += 1;
                }
            }
            let min = machine_counts.iter().min().unwrap();
            let max = machine_counts.iter().max().unwrap();
            assert!(max - min <= 1);
        }
    }

    proptest! {
        #[test]
        fn test_weighted_balanced_random_replicas(seed: u64) {
            use rand_distr::Normal;
            let replicas = 3;
            let num_machines = 20;
            let num_shards = 190;
            let mut rng = ChaChaRng::seed_from_u64(seed);
            let mut weight_rng = ChaChaRng::from_rng(&mut rng)?;
            let mut machine_rng = ChaChaRng::from_rng(&mut rng)?;
            let weight_distr = Normal::new(1.0, 0.2).unwrap();
            let distr = WeightedBalancedRandomReplicas::new(rng);
            let shards: Vec<_> = std::iter::repeat(replicas).map(move |r| Shard {
                replicas: r,
                weight: match weight_distr.sample(&mut weight_rng) {
                    v if v < 0.1 => 0.0,
                    v => v
                },
            }).take(num_shards).collect();
            let weight_distr = Normal::new(1.0, 0.2).unwrap();
            let machines: Vec<_> = (0..num_machines).map(move |_| Machine {
                weight: match weight_distr.sample(&mut machine_rng) {
                    v if v < 0.1 => 0.0,
                    v => v
                },
            }).collect();
            let assignment = distr.assign_replicas(&shards, &machines);
            let mut machine_counts = vec![0; num_machines];
            let mut machine_costs = vec![0_f32; num_machines];
            for (shard_id, mut machine_ids) in assignment.into_iter().enumerate() {
                assert_eq!(machine_ids.len(), replicas);
                machine_ids.sort();
                machine_ids.dedup();
                assert_eq!(machine_ids.len(), replicas);
                for mid in machine_ids {
                    machine_counts[mid.index()] += 1;
                    machine_costs[mid.index()] += shards[shard_id].weight / machines[mid.index()].weight;
                }
            }
            let min = machine_costs.iter().copied().map(OrderedFloat::from).min().unwrap();
            let max = machine_costs.iter().copied().map(OrderedFloat::from).max().unwrap();
            println!("{} {} {}", max, min, min.into_inner() / max.into_inner());
            assert!(min.into_inner() / max.into_inner()  >= 0.90);
        }
    }
}
