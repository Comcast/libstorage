# libstorage
[![Build Status](https://travis-ci.org/Comcast/libstorage.svg?branch=master)](https://travis-ci.org/Comcast/libstorage)
[![crates.io](https://img.shields.io/crates/v/libstorage.svg)](https://crates.io/crates/libstorage)
[![Documentation](https://docs.rs/libstorage/badge.svg)](https://docs.rs/libstorage)
Library for all our storage systems
----

libstorage is a collection of helper functions written in RUST to make interfacing with storage servers easier.  Under the src/
directory there is a module for each storage system the library supports.  

----

## To start using libstorage

libstorage is easy to use in your project.  Just include the dependency in your Cargo.toml and you're ready to roll.
The isilon library has been put behind a cargo feature flag because it's so large and the feature has to be enabled during the build.

## Example

The following example shows a sample use of the hitachi module: 
```rust
use libstorage::hitachi::HitachiConfig;
use reqwest::Client;

fn main() -> Result<(), libstorage::Error>> {
    let web_client = reqwest::Client::new();
    let hitachi_config = HitachiConfig {
        endpoint: "server".into(),
        user: "username".into(),
        password: "password".into(),
        region: "region".into(),
    };

    // 1. Get the host:instance list with /AgentForRAID
    let agents = get_agent_for_raid(&web_client, &hitachi_config)?;
    println!("items: {} {:?}", agents.items.len(), agents);

    Ok(())
}
```

## Support and Contributions

If you need support, start by checking the [issues] page.
If that doesn't answer your questions, or if you think you found a bug,
please [file an issue].

That said, if you have questions, reach out to us
[communication].

Want to contribute to libstorage? Awesome! Check out the [contributing](https://github.com/Comcast/libstorage/blob/master/Contributing.md) guide.

[communication]: https://github.com/Comcast/libstorage/issues/new
[community repository]: https://github.com/Comcast/libstorage
[file an issue]: https://github.com/Comcast/libstorage/issues/new
[issues]: https://github.com/Comcast/libstorage/issues
