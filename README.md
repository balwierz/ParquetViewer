# parquet_viewer

A fast, self-contained command-line tool for inspecting Apache Parquet files.
It prints the first *N* rows as an ASCII table — like `pandas.DataFrame.head()` — followed by a schema summary and file metadata.

## Features

- **Pandas-style table** with aligned columns, right-aligned numbers, null markers
- **Full Arrow type support** — integers, floats, booleans, strings, timestamps, dates, decimals, binary, lists, structs, maps, and **dictionary-encoded columns** (values decoded, not raw indices)
- **Schema summary** — column names, Arrow types, nullability
- **File metadata** — row count, row groups, compressed size, creator string
- **Fast** — reads only as many row groups as needed to satisfy the requested row count
- **Fully static binary** — zero shared library dependencies beyond libc; runs on any Linux with kernel ≥ 3.2

## Usage

```
parquet_viewer [options] <file.parquet>

  -n <rows>   number of rows to display  (default: 10, 0 = all)
  -w <width>  max cell width in chars    (default: 32, longer values truncated with ...)
  -c <cols>   max columns to show        (default: all)
  --no-index  suppress the row-index column
  -h          show help
```

## Example output

```
$ parquet_viewer data.parquet
+---+------+----------+----------+--------+-------------------------+----------------------------------+
|   | id   | name     | score    | active | ts                      | notes                            |
+---+------+----------+----------+--------+-------------------------+----------------------------------+
| 0 |    0 | Person 0 |        0 | true   | 2024-01-01 00:00:00.000 | null                             |
| 1 |    1 | Person 1 |  1.23456 | false  | 2024-01-01 01:00:00.000 | Note with some longer text fo... |
| 2 |    2 | Person 2 |  2.46912 | true   | 2024-01-01 02:00:00.000 | Note with some longer text fo... |
| 3 |    3 | Person 3 |  3.70368 | false  | 2024-01-01 03:00:00.000 | Note with some longer text fo... |
| 4 |    4 | Person 4 |  4.93824 | true   | 2024-01-01 04:00:00.000 | Note with some longer text fo... |
| 5 |    5 | Person 5 |   6.1728 | false  | 2024-01-01 05:00:00.000 | Note with some longer text fo... |
| 6 |    6 | Person 6 |  7.40736 | true   | 2024-01-01 06:00:00.000 | Note with some longer text fo... |
| 7 |    7 | Person 7 |  8.64192 | false  | 2024-01-01 07:00:00.000 | null                             |
| 8 |    8 | Person 8 |  9.87648 | true   | 2024-01-01 08:00:00.000 | Note with some longer text fo... |
| 9 |    9 | Person 9 | 11.11104 | false  | 2024-01-01 09:00:00.000 | Note with some longer text fo... |
+---+------+----------+----------+--------+-------------------------+----------------------------------+

[100 rows x 6 columns]

Column  Type           Nullable
------  -------------  --------
id      int32          yes
name    string         yes
score   double         yes
active  bool           yes
ts      timestamp[ms]  yes
notes   string         yes

File: data.parquet
Row groups: 1  |  Compressed size: 3.5 KiB
Created by: parquet-cpp-arrow version 23.0.1
```

## Building

Requires CMake 3.16+, g++ with C++20 support, and the Arrow/Parquet development libraries.

On Debian/Ubuntu:
```sh
sudo apt-get install libarrow-dev libparquet-dev libmimalloc-dev cmake g++
```

Then:
```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
```

The binary is at `build/parquet_viewer`. It is statically linked — copy it anywhere.

### Dependencies

All dependencies except glibc are linked statically:

| Library | Version | How |
|---|---|---|
| Apache Arrow + Parquet | system | static `.a` |
| mimalloc | v2.1.9 | built from source via FetchContent |
| snappy, lz4, zstd, zlib, bz2, brotli | system | static `.a` |
| thrift, re2, xxhash, utf8proc, absl | system | static `.a` |
| libc, libm | system | dynamic (glibc ABI is stable) |
