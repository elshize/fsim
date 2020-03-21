#!/bin/bash
cargo tarpaulin --exclude-files main.rs --run-types Tests Doctests -o Html --ignore-tests
