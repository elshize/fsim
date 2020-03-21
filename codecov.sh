#!/bin/bash
cargo tarpaulin --exclude-files ui.rs event.rs main.rs --run-types Tests Doctests -o Html --ignore-tests
