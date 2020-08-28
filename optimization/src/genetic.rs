////! Shard replica assignment optimization through a genetic algorithm.
//use super::{to_floats, Error, Result};
//use itertools::Itertools;
//use nalgebra::{DMatrix, DVector};
//use ordered_float::OrderedFloat;
//use rand::seq::SliceRandom;
//use rand::Rng;
//use rand_distr::{Bernoulli, Distribution, Normal, Uniform};
//use std::iter::repeat;

//type Load = OrderedFloat<f32>;

///// For testing: CW09B+selective+natural
//pub const SHARD_LOADS: [f32; 128] = [
//    2769.4465, 2563.0969, 1658.6009, 1907.1591, 4095.8377, 2595.4211, 1902.8827, 1871.9019,
//    2811.6678, 1330.1244, 2173.1014, 1736.8571, 1910.7388, 3055.4968, 2306.2665, 3043.5685,
//    2606.7585, 1864.2502, 1463.707, 5827.256, 2743.4234, 3540.5672, 1845.7381, 3852.1842,
//    1393.0152, 1712.4744, 3049.0203, 2669.7383, 2727.4197, 1815.9953, 2097.3796, 4033.9158,
//    1670.9638, 2398.5224, 2727.7015, 1681.0979, 1143.6434, 3077.4799, 1934.5515, 2077.0641,
//    3472.3239, 3767.8709, 1996.7776, 2122.1478, 2598.968, 2673.3812, 1370.5703, 2553.3114,
//    3621.8528, 3332.3947, 3298.2937, 2321.8184, 1651.1206, 1599.3494, 2775.6897, 933.9928,
//    2308.0888, 2967.4318, 2489.4295, 2660.5304, 3875.0388, 3459.9756, 2933.5879, 2302.4254,
//    2456.0884, 2866.4099, 2222.7688, 2949.4361, 772.0176, 3036.9805, 734.1402, 2389.1279, 931.8882,
//    2351.7616, 3564.0863, 2805.4057, 2414.5001, 3401.5756, 2094.716, 2065.8288, 3419.5094, 2417.12,
//    2622.0397, 1560.3342, 1112.5524, 1140.2073, 2929.6292, 2854.6724, 2502.3643, 2123.6761,
//    1784.0063, 1504.3259, 3160.18, 2189.8444, 2562.0229, 559.5894, 782.1046, 3110.4198, 2694.6023,
//    2192.6533, 2190.0165, 3324.3018, 2680.1467, 2323.2137, 2491.8795, 3858.0141, 1483.7859,
//    2896.5784, 3752.6631, 1996.6473, 2060.7296, 4128.9895, 1560.5123, 2694.7296, 2868.7403,
//    2571.9589, 1845.6197, 3914.823, 3462.8996, 1884.551, 2930.887, 3339.6855, 1777.2353, 3382.8745,
//    1000.3053, 4795.2733, 2168.9426, 3044.6332,
//];

///// For testing: CW09B+selective+natural
//pub const SHARD_VOLUMES: [f32; 128] = [
//    22.2293262, 12.6619292, 9.9792899, 7.4227925, 35.9115986, 16.9188487, 12.718849, 9.8888731,
//    27.6115509, 11.1012769, 29.3419464, 9.2526493, 10.9776041, 31.5410774, 17.0611388, 21.2793979,
//    19.5846393, 10.1899121, 9.5617223, 77.8259615, 22.5645708, 44.5621033, 9.1685526, 28.5377459,
//    7.7961363, 7.8787045, 15.6376474, 13.5616299, 20.7264479, 9.2617893, 13.2923171, 38.9188564,
//    9.1492336, 20.7700482, 20.0522159, 7.1870716, 3.6720468, 20.2183387, 13.4836079, 21.5389323,
//    23.1554164, 31.5159969, 10.4453579, 15.6147215, 19.9352025, 18.8024051, 5.2953479, 21.3048384,
//    30.890757, 17.7598981, 30.1640823, 12.5522911, 9.9624533, 8.6006635, 24.4348158, 4.0249707,
//    13.9640678, 19.9929309, 16.0116179, 21.3256179, 34.1877979, 24.0736217, 24.9283799, 16.4821927,
//    25.7021179, 23.7681685, 17.8688572, 22.3077097, 3.8391571, 23.1142085, 13.5761679, 14.8221898,
//    3.3282193, 17.6892673, 36.9519875, 20.1374695, 12.2232552, 24.2528947, 14.9436307, 14.8295564,
//    37.9661578, 15.0037191, 18.7673903, 6.1808815, 7.3814802, 8.4884903, 15.6926281, 16.2914561,
//    19.7665783, 15.3217833, 22.558547, 7.5832559, 33.4387182, 14.7449038, 12.2888097, 2.0108969,
//    1.6852314, 17.7036442, 15.576595, 17.8193341, 20.4416948, 22.2861352, 15.4434949, 10.7370789,
//    22.1935833, 31.5384887, 5.156956, 26.392024, 23.8591493, 7.499831, 9.3541704, 37.9407141,
//    11.7599145, 24.7952155, 24.5642507, 19.2584626, 11.7979558, 28.4853907, 34.2552787, 17.240649,
//    26.3187414, 27.1509591, 9.4575177, 20.5271572, 5.6577814, 43.6696296, 12.5498856, 22.7881003,
//];

///// Phenotype represents a single solution, i.e., any replica assignment, possibly incorrect with
///// respect to the problem constraints.
//#[derive(Debug, Clone, Eq, PartialEq)]
//pub struct Phenotype(pub DMatrix<u32>);

//impl From<DMatrix<u32>> for Phenotype {
//    fn from(matrix: DMatrix<u32>) -> Self {
//        Self(matrix)
//    }
//}

//impl Phenotype {
//    /// Randomly selects a cell in matrix that has value 1 and moves it within its column to a
//    /// random cell with value zero. Equivalent to moving shard replica from one machine to
//    /// another.
//    ///
//    /// # Panics
//    ///
//    /// Panics if the phenotype is empty, or the selected column is either all zeroes or all ones.
//    pub fn mutate<R: Rng>(mut self, rng: &mut R) -> Self {
//        let (_, num_shards) = self.0.shape();
//        assert!(num_shards > 0, "Cannot mutate an empty phenotype");
//        let shard = Uniform::new(0, num_shards).sample(rng);
//        let (zeroes, ones) = self.partition_column_by_value(shard);
//        assert!(
//            zeroes.len() > 0,
//            "Cannot mutate a phenotype with a full column: {:?}",
//            self.0.column(shard)
//        );
//        assert!(
//            ones.len() > 0,
//            "Cannot mutate a phenotype with an empty column: {:?}",
//            self.0.column(shard)
//        );
//        let from = ones[Uniform::new(0, ones.len()).sample(rng)];
//        let to = zeroes[Uniform::new(0, zeroes.len()).sample(rng)];
//        self.0[(from, shard)] = 0;
//        self.0[(to, shard)] = 1;
//        self
//    }

//    fn swap<R: Rng>(mut self, rng: &mut R) -> Self {
//        let (num_machines, num_shards) = self.0.shape();
//        let assigned: Vec<_> = (0..num_machines)
//            .flat_map(|m| (0..num_shards).map(move |s| (m, s)))
//            .filter(|&coord| self.0[coord] == 1)
//            .collect();
//        let from = assigned[Uniform::new(0, assigned.len()).sample(rng)];
//        let to_candidates: Vec<_> = assigned.into_iter().filter(|(_, s)| *s == from.1).collect();
//        let to = to_candidates[Uniform::new(0, to_candidates.len()).sample(rng)];
//        self.0[from] = 0;
//        self.0[to] = 0;
//        self.0[(from.0, to.1)] = 1;
//        self.0[(to.0, from.1)] = 1;
//        self
//    }

//    /// Randomly takes a half of columns from `self` and the rest from `other` to create a new
//    /// phenotype.
//    pub fn crossover<R: Rng>(mut self, other: &Self, rng: &mut R) -> Self {
//        assert_eq!(
//            self.0.shape(),
//            other.0.shape(),
//            "Shapes of crossed phenotypes do not match"
//        );
//        let (_, num_shards) = self.0.shape();
//        let mut indices: Vec<_> = (0..num_shards).collect();
//        indices.shuffle(rng);
//        for shard in indices.into_iter().skip(num_shards / 2) {
//            self.0.set_column(shard, &other.0.column(shard));
//        }
//        self
//    }

//    /// The fitness of this phenotype wit respect to the given load matix.
//    pub fn fitness(&self, loads: &DMatrix<f32>) -> Load {
//        OrderedFloat(loads.component_mul(&to_floats(&self.0)).column_sum().max())
//    }

//    /// Returns two vectors containing indices of the cells in the given column that have value 0
//    /// and 1 respectively.
//    fn partition_column_by_value(&self, column: usize) -> (Vec<usize>, Vec<usize>) {
//        (0..self.0.shape().0).partition(|&m| self.0[(m, column)] == 0)
//    }

//    /// Number of shards/columns.
//    pub fn num_shards(&self) -> usize {
//        self.0.shape().1
//    }

//    /// Number of machines/rows.
//    pub fn num_machines(&self) -> usize {
//        self.0.shape().0
//    }
//}

//pub struct Parameters {
//    pub shard_probabilities: DVector<f32>,
//    pub shard_loads: DVector<f32>,
//    pub shard_volumes: DVector<f32>,
//    pub shard_replicas: DVector<u32>,
//    pub machine_inhibitions: DVector<f32>,
//    pub machine_capacities: DVector<f32>,
//}

//impl Parameters {
//    /// Calculate load matrix.
//    pub fn load_matrix(&self) -> DMatrix<f32> {
//        &self.machine_inhibitions
//            * self.shard_loads.transpose()
//            * DMatrix::<f32>::from_diagonal(&self.shard_probabilities)
//    }
//}

//pub struct Problem<R: Rng> {
//    population: Vec<(Phenotype, Load)>,
//    loads: DMatrix<f32>,
//    volumes: DVector<f32>,
//    replicas: DVector<u32>,
//    capacities: DVector<f32>,
//    rng: R,
//    num_machines: usize,
//    num_shards: usize,
//    mutation_distr: Bernoulli,
//}

//impl<R: Rng> Problem<R> {
//    pub fn new(parameters: Parameters, rng: R) -> Self {
//        // TODO
//        // let (num_machines, num_shards) = self.loads.shape();
//        // if num_machines != capacities.shape().0 {
//        //     return Err(Error::InconsistentMachineCount);
//        // }
//        // if num_shards != volumes.shape().0 || num_shards != replicas.shape().0 {
//        //     return Err(Error::InconsistentMachineCount);
//        // }
//        let num_machines = parameters.machine_inhibitions.len();
//        let num_shards = parameters.shard_probabilities.len();
//        let mut problem = Self {
//            population: Vec::new(),
//            loads: parameters.load_matrix(),
//            volumes: parameters.shard_volumes,
//            replicas: parameters.shard_replicas,
//            capacities: parameters.machine_capacities,
//            rng,
//            num_machines,
//            num_shards,
//            mutation_distr: Bernoulli::new(0.5).unwrap(),
//        };
//        problem.population = (0..3000)
//            .filter_map(|_| {
//                problem.find_initial().ok().map(Phenotype::from).map(|p| {
//                    let load = problem.fitness(&p);
//                    (p, load)
//                })
//            })
//            .collect();
//        problem
//    }

//    /// Returns the load matrix.
//    pub fn load_matrix(&self) -> &DMatrix<f32> {
//        &self.loads
//    }

//    fn find_initial(&mut self) -> Result<DMatrix<u32>> {
//        let mut replicas: Vec<_> = self
//            .replicas
//            .iter()
//            .copied()
//            .enumerate()
//            .flat_map(|(s, r)| repeat(s).take(r as usize))
//            .collect();
//        replicas.shuffle(&mut self.rng);
//        let mut assignment = DMatrix::<u32>::zeros(self.num_machines, self.num_shards);
//        'outer: for s in replicas {
//            // Assign shard `s` replica to a machine.
//            let volume = self.volumes[s];
//            let mut machine_ids: Vec<_> = (0..self.num_machines).collect();
//            // To keep it fair, we always check in different order.
//            machine_ids.shuffle(&mut self.rng);
//            let float_assignment = to_floats(&assignment);
//            for m in machine_ids {
//                let machine_load = self.loads.row(m).dot(&float_assignment.row(m));
//                if assignment[(m, s)] == 0 && machine_load + volume <= self.capacities[m] {
//                    assignment[(m, s)] = 1;
//                    continue 'outer;
//                }
//            }
//            return Err(Error::PossiblyInfeasible);
//        }
//        Ok(assignment)
//    }

//    fn breed(&mut self) {
//        let rng = &mut self.rng;
//        let mut population = std::mem::take(&mut self.population);
//        let loads = &self.loads;
//        population.sort_by_key(|(_, load)| *load);
//        population.truncate(population.len() / 3);
//        population.shuffle(rng);
//        let moved: Vec<_> = population
//            .iter()
//            .map(|(p, _)| {
//                let mutated = p.clone().mutate(rng);
//                let load = mutated.fitness(loads);
//                (mutated, load)
//            })
//            .collect();
//        let swapped: Vec<_> = population
//            .iter()
//            .map(|(p, _)| {
//                let swapped = p.clone().swap(rng);
//                let load = swapped.fitness(loads);
//                (swapped, load)
//            })
//            .collect();
//        population.extend_from_slice(&moved);
//        population.extend_from_slice(&swapped);
//        self.population = population;
//    }
//    // fn breed(&mut self) {
//    //     let rng = &mut self.rng;
//    //     let mut population = std::mem::take(&mut self.population);
//    //     let loads = &self.loads;
//    //     let mutation_distr = &self.mutation_distr;
//    //     population.sort_by_key(|(_, load)| *load);
//    //     population.truncate(population.len() / 2);
//    //     population.shuffle(rng);
//    //     self.population = population
//    //         .into_iter()
//    //         .map(|(p, _)| p)
//    //         .batching(|it| {
//    //             if let Some(parent) = it.next() {
//    //                 let other_parent = it.next().unwrap();
//    //                 let a = if mutation_distr.sample(rng) {
//    //                     parent.clone().mutate(rng)
//    //                 } else {
//    //                     parent.clone()
//    //                 };
//    //                 let b = if mutation_distr.sample(rng) {
//    //                     other_parent.clone().mutate(rng)
//    //                 } else {
//    //                     other_parent.clone()
//    //                 };
//    //                 Some(
//    //                     (0..2)
//    //                         .map(|_| {
//    //                             let child = parent.clone().crossover(&other_parent, rng);
//    //                             if mutation_distr.sample(rng) {
//    //                                 child.mutate(rng)
//    //                             } else {
//    //                                 child
//    //                             }
//    //                         })
//    //                         .chain(std::iter::once(a))
//    //                         .chain(std::iter::once(b))
//    //                         .collect::<Vec<_>>(),
//    //                 )
//    //             } else {
//    //                 None
//    //             }
//    //         })
//    //         .flatten()
//    //         .map(|child| {
//    //             let load = child.fitness(&loads);
//    //             (child, load)
//    //         })
//    //         .collect();
//    //     // self.population = population
//    //     //     .into_iter()
//    //     //     .map(|(p, _)| p)
//    //     //     .batching(|it| {
//    //     //         if let Some(parent) = it.next() {
//    //     //             let other_parent = it.next().unwrap();
//    //     //             Some(
//    //     //                 (0..4)
//    //     //                     .map(|_| {
//    //     //                         let child = parent.clone().crossover(&other_parent, rng);
//    //     //                         if mutation_distr.sample(rng) {
//    //     //                             child.mutate(rng)
//    //     //                         } else {
//    //     //                             child
//    //     //                         }
//    //     //                     })
//    //     //                     .collect::<Vec<_>>(),
//    //     //             )
//    //     //         } else {
//    //     //             None
//    //     //         }
//    //     //     })
//    //     //     .flatten()
//    //     //     .map(|child| {
//    //     //         let load = child.fitness(&loads);
//    //     //         (child, load)
//    //     //     })
//    //     //     .collect();
//    // }

//    pub fn solve(&mut self) -> Phenotype {
//        self.population.sort_by_key(|(_, load)| *load);
//        let mut optimal = (
//            self.population.first().unwrap().0.clone(),
//            self.population.first().unwrap().1,
//        );
//        for generation in 0..100 {
//            self.breed();
//            self.population.sort_by_key(|(_, load)| *load);
//            let top = self.population.first().unwrap();
//            let load = top.1;
//            if load < optimal.1 {
//                optimal = (top.0.clone(), load);
//            }
//            println!(
//                "Generation {}: load {}; size: {}",
//                generation + 1,
//                load.into_inner(),
//                self.population.len(),
//            );
//        }
//        println!("Optimal: {}", optimal.1.into_inner());
//        optimal.0.clone()
//    }

//    fn fitness(&self, phenotype: &Phenotype) -> Load {
//        OrderedFloat(
//            self.loads
//                .component_mul(&to_floats(&phenotype.0))
//                .column_sum()
//                .max(),
//        )
//    }
//}

//#[cfg(test)]
//mod test {
//    use super::*;
//    use proptest::prelude::*;
//    use rand::SeedableRng;
//    use rand_chacha::ChaChaRng;

//    #[test]
//    fn test_partition_column() {
//        let phenotype = Phenotype::from(DMatrix::from_column_slice(
//            4,
//            3,
//            &[0, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 1],
//        ));
//        let (zeroes, ones) = phenotype.partition_column_by_value(0);
//        assert_eq!(&zeroes, &[0, 1]);
//        assert_eq!(&ones, &[2, 3]);
//        let (zeroes, ones) = phenotype.partition_column_by_value(1);
//        assert_eq!(&zeroes, &[2, 3]);
//        assert_eq!(&ones, &[0, 1]);
//        let (zeroes, ones) = phenotype.partition_column_by_value(2);
//        assert_eq!(&zeroes, &[0, 2]);
//        assert_eq!(&ones, &[1, 3]);
//    }

//    fn phenotype() -> impl Strategy<Value = Phenotype> {
//        ((2..10_usize), (20..50_usize)).prop_flat_map(|(num_machines, num_shards)| {
//            prop::collection::vec(0..=1_u32, num_machines * num_shards).prop_map(move |vec| {
//                let mut matrix = DMatrix::from_vec(num_machines, num_shards, vec);
//                for shard in 0..num_shards {
//                    let mut column = matrix.column_mut(shard);
//                    if !is_between_exclusive(column.sum(), 0, column.len() as u32) {
//                        column[0] = 1 - column[0];
//                    }
//                }
//                Phenotype::from(matrix)
//            })
//        })
//    }

//    fn is_between_exclusive<T: Ord>(value: T, low: T, high: T) -> bool {
//        low < value && value < high
//    }

//    proptest! {
//        #[test]
//        fn test_mutate(
//            seed: u64,
//            phenotype in phenotype()
//        ) {
//            let mut rng = ChaChaRng::seed_from_u64(seed);
//            let mutated = phenotype.clone().mutate(&mut rng);
//            assert_ne!(mutated, phenotype);
//            for shard in 0..phenotype.num_shards() {
//                assert_eq!(phenotype.0.column(shard).sum(), mutated.0.column(shard).sum());
//            }
//        }

//        #[test]
//        fn test_crossover(
//            seed: u64,
//            size in 20..50_usize
//        ) {
//            let mut rng = ChaChaRng::seed_from_u64(seed);
//            let vec: Vec<_> = (0..size).flat_map(|s| {
//                repeat(0).take(s + 1).chain(repeat(1)).take(size)
//            }).collect();
//            let lhs = Phenotype::from(DMatrix::<u32>::from_vec(size, size, vec));
//            let vec: Vec<_> = (0..size).flat_map(|s| {
//                repeat(1).take(s + 1).chain(repeat(0)).take(size)
//            }).collect();
//            let rhs = Phenotype::from(DMatrix::<u32>::from_vec(size, size, vec));
//            let child = lhs.crossover(&rhs, &mut rng);
//            for col in 1..size {
//                assert_ne!(child.0.column(col - 1), child.0.column(col));
//            }
//            let lhs = (0..size).filter(|&j| child.0[(0, j)] == 0).count();
//            let rhs = (0..size).filter(|&j| child.0[(0, j)] == 1).count();
//            if size % 2 == 0 {
//                assert_eq!(lhs, rhs);
//            } else {
//                assert_eq!(lhs + 1, rhs);
//            }
//        }
//    }

//    fn test_optimize_feasible(seed: u64, num_machines: usize, num_shards: usize, replicas: u32) {
//        let mut rng = ChaChaRng::seed_from_u64(seed);
//        // let normal = Normal::new(1.0, 0.5).unwrap();
//        // let shard_loads: Vec<f32> = (0..num_shards).map(|_| normal.sample(&mut rng)).collect();
//        // let shard_loads = DVector::<f32>::from_vec(shard_loads);
//        let shard_loads = DVector::<f32>::repeat(num_shards, 1.0);
//        let shard_probabilities = DVector::<f32>::repeat(num_shards, 1.0);
//        let shard_volumes = DVector::<f32>::repeat(num_shards, 1.0);
//        let shard_replicas = DVector::<u32>::repeat(num_shards, replicas);
//        let machine_capacities = DVector::<f32>::repeat(num_machines, f32::MAX);
//        let machine_inhibitions = DVector::<f32>::repeat(num_machines, 1.0);

//        let goal = (shard_loads.sum() * replicas as f32) / machine_inhibitions.len() as f32;
//        println!("Goal: {}", goal);

//        let mut problem = Problem::new(
//            Parameters {
//                shard_loads,
//                shard_probabilities,
//                shard_volumes,
//                shard_replicas,
//                machine_capacities,
//                machine_inhibitions,
//            },
//            rng,
//        );
//        problem.solve();
//        println!("Goal: {}", goal);

//        //let assignment = opt.optimize_stochastic(
//        //    &probabilities,
//        //    &loads,
//        //    &volumes,
//        //    &replicas,
//        //    &inhibitions,
//        //    &capacities,
//        //);
//        ////println!("{:?}", assignment);
//        //assert!(assignment.is_ok());
//        //let assignment = assignment.unwrap();
//        //println!(
//        //    "{}",
//        //    max_load(
//        //        &(inhibitions * loads.transpose() * DMatrix::<f32>::from_diagonal(&probabilities)),
//        //        &assignment
//        //    )
//        //);
//        //for (c, &r) in replicas.iter().enumerate() {
//        //    assert_eq!(r, assignment.column(c).sum());
//        //}
//    }

//    #[test]
//    fn test_optimize_feasible_prop() {
//        test_optimize_feasible(1241, 20, 200, 4);
//    }
//}
