<p align="center">
  <img src="https://github.com/openpeeps/boogie/blob/main/.github/boogie.png" width="100px"><br>
  A stupid simple WAL-based embedded database in Nim<br>
</p>

<p align="center">
  <code>nimble install boogie</code>
</p>

<p align="center">
  <a href="https://openpeeps.github.io/boogie">API reference</a><br>
  <img src="https://github.com/openpeeps/boogie/workflows/test/badge.svg" alt="Github Actions">  <img src="https://github.com/openpeeps/boogie/workflows/docs/badge.svg" alt="Github Actions">
</p>

## 😍 Key Features
- Red-black tree indexes for fast lookups and range queries
- Write-ahead log (WAL) for durability and crash recovery
- Simple API for inserting, updating, deleting, and querying records
- Configurable options for performance tuning, such as batch sizes and flush intervals
- In-memory and on-disk storage
- Primitive data types (`string`, `int`, `float`, `bool`, `json`, `null`)

>[!NOTE]
> Boogie is an experimental project mostly made with the chatbot for fun and learning. It is still in early stages, so expect data loss and breaking changes. Use at your own risk.

## Examples
Check the [tests](https://github.com/openpeeps/boogie/tree/main/src/boogie/tests) for more examples.


### Todos
- [x] Add support for multiple tables
- [x] Add basic tests and benchmarks
- 

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/boogie/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/boogie/fork)
- 😎 [Get €20 in cloud credits from Hetzner](https://hetzner.cloud/?ref=Hm0mYGM9NxZ4)

### 🎩 License
LGPLv3 license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
