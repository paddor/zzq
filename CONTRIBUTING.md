# Contributing

Bug reports and pull requests are welcome at
[paddor/zzq](https://github.com/paddor/zzq).

Before opening a pull request:

- Run `cargo fmt --all`,
  `cargo clippy --workspace --all-targets -- -D warnings` and
  `cargo test --workspace --release`.
- Cover the change with a test. A bug fix comes with a test that fails
  without it.
- Add a line to `CHANGELOG.md` under `## [Unreleased]`.

## License

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed under [ISC](LICENSE-ISC) or
[Apache-2.0](LICENSE-APACHE), like the rest of zzq, without any additional
terms or conditions.
