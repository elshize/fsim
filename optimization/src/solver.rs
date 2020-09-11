//! Module for custom CBC solver based on the one from `lp_modeler`.

use uuid::Uuid;

use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::PathBuf;
use std::process::Command;
use std::sync;
use std::time::Duration;

use lp_modeler::dsl::LpProblem;
use lp_modeler::format::lp_format::*;
use lp_modeler::solvers::Status;

/// Errors used when running the solver.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum Error {
    #[error("The solver has timed out in {0:#?}")]
    Timeout(Duration),
    #[error("Could not write to {file}: {source}")]
    WriteModel { source: io::Error, file: PathBuf },
    #[error("Error executing solver: {0}")]
    SolverCommand(String),
    #[error("Error reading command: {0}")]
    ReadSolution(String),
}

/// This is a copy of `lp_modeler::CbcSolver` that supports running the solver with timeout.
pub struct TimeoutCbcSolver {
    name: String,
    command_name: String,
    temp_solution_file: String,
}

impl Default for TimeoutCbcSolver {
    fn default() -> Self {
        Self {
            name: "Cbc".to_string(),
            command_name: "cbc".to_string(),
            temp_solution_file: format!("{}.sol", Uuid::new_v4().to_string()),
        }
    }
}

impl TimeoutCbcSolver {
    pub fn command_name(&self, command_name: String) -> Self {
        Self {
            name: self.name.clone(),
            command_name,
            temp_solution_file: self.temp_solution_file.clone(),
        }
    }
    pub fn temp_solution_file(&self, temp_solution_file: String) -> Self {
        Self {
            name: self.name.clone(),
            command_name: self.command_name.clone(),
            temp_solution_file,
        }
    }
    fn read_solution(&self) -> Result<(Status, HashMap<String, f32>), String> {
        fn read_specific_solution(f: &File) -> Result<(Status, HashMap<String, f32>), String> {
            let mut vars_value: HashMap<_, _> = HashMap::new();

            let mut file = BufReader::new(f);
            let mut buffer = String::new();
            let _ = file.read_line(&mut buffer);

            let status = if let Some(status_line) = buffer.split_whitespace().next() {
                match status_line.split_whitespace().next() {
                    Some("Optimal") => Status::Optimal,
                    // Infeasible status is either "Infeasible" or "Integer infeasible"
                    Some("Infeasible") | Some("Integer") => Status::Infeasible,
                    Some("Unbounded") => Status::Unbounded,
                    // "Stopped" can be "on time", "on iterations", "on difficulties" or "on ctrl-c"
                    Some("Stopped") => Status::SubOptimal,
                    _ => Status::NotSolved,
                }
            } else {
                return Err("Incorrect solution format".to_string());
            };
            for line in file.lines() {
                let l = line.unwrap();
                let mut result_line: Vec<_> = l.split_whitespace().collect();
                if result_line[0] == "**" {
                    result_line.remove(0);
                };
                if result_line.len() == 4 {
                    match result_line[2].parse::<f32>() {
                        Ok(n) => {
                            vars_value.insert(result_line[1].to_string(), n);
                        }
                        Err(e) => return Err(e.to_string()),
                    }
                } else {
                    return Err("Incorrect solution format".to_string());
                }
            }
            Ok((status, vars_value))
        }

        match File::open(&self.temp_solution_file) {
            Ok(f) => {
                let res = read_specific_solution(&f)?;
                let _ = fs::remove_file(&self.temp_solution_file);
                Ok(res)
            }
            Err(_) => return Err("Cannot open file".to_string()),
        }
    }

    /// Attempts at solving the problem in `timeout` time.
    pub fn run(
        &self,
        problem: &LpProblem,
        timeout: Duration,
    ) -> Result<(Status, HashMap<String, f32>), Error> {
        let file_model = &format!("{}.lp", problem.unique_name);

        problem
            .write_lp(file_model)
            .map_err(|e| Error::WriteModel {
                source: e,
                file: PathBuf::from(file_model),
            })?;

        let mut child = Command::new(&self.command_name)
            .arg(file_model)
            .arg("solve")
            .arg("solution")
            .arg(&self.temp_solution_file)
            .spawn()
            .map_err(|e| Error::SolverCommand(e.to_string()))?;

        let (tx, rx) = sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            std::thread::sleep(timeout);
            // If it couldn't be sent, `rx` is already closed and therefore solution already found.
            let _ = tx.send(());
        });

        loop {
            if let Some(status) = child
                .try_wait()
                .map_err(|_| Error::SolverCommand("Error attempting to wait".into()))?
            {
                return if status.success() {
                    self.read_solution().map_err(|e| Error::ReadSolution(e))
                } else {
                    Err(Error::SolverCommand(status.to_string()))
                };
            }
            if rx.try_recv().is_ok() {
                let _ = child.kill();
                return Err(Error::Timeout(timeout));
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }
}
