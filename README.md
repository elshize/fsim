# Query Routing Simulation

This repository contains software supporting research into query routing in distributed search systems.

# Installation

The code base is written in [Rust]. It uses some cutting-edge language features, so you will need to use at least **Rust 1.42**.

## Setting Up Rust

If you do not have Rust ecosystem installed, you can set it up simply with:

```bash
# Taken from: https://www.rust-lang.org/learn/get-started
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

If you already have `rustup` installed, you only need to update it to the latest **stable** version:

```bash
rustup update
```

## Build Project

The project follows the standard [Cargo] workflow. For those who are unfamiliar with it, here are just a few commands you will need to build and run the application.

The following command will build the project along with all its dependencies in release mode. It will take longer but the build will be faster due to the optimizations performed at the compilation time. You can skip the `--release` flag if you only want to play with toy examples or for development of the application.

```bash
cargo build --release
```

See [Installation](#install) for an alternative way to build it.

## Running the application

If you have built the project with the command above, the binary is now located in `target/release/simulate` and ready to run:

```bash
target/release/simulate --help
```

Alternatively, you can run the binary with `cargo`:

```bash
cargo run -- --help
```

Notice the `--`, which separates Cargo's arguments from the arguments of the target application.

## Installation {#install}

Instead of building and running the project locally, you can install it in Cargo's local repository. On Linux, this is usually in `~/.cargo`. In order to make the installed binaries available, make sure to add `~/.cargo/bin` to your `PATH` environment variable or its equivalent in your operating system. Consult Cargo's [documentation][Cargo] for more information.

To install this project with Cargo, run the following command.

```bash
cargo install --path .
```

# Usage

![](misc/tui.png)

[Cargo]: https://doc.rust-lang.org/cargo/
[Rust]: https://www.rust-lang.org/
