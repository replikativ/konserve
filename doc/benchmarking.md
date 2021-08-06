# Benchmarking Konserve

There is a small command line utility integrated in this project to measure the performance of our *in-memory* and our *file* backend. It is also capable of comparing benchmarking results.

To run the benchmarks, navigate to the project folder in your console and run 
```bash
clj -M:benchmark [OPTIONS] [OUTPUTFILEPATH] 
```


## Options

| Short | Long                           | Description                                                    | Default        |
|-------+--------------------------------+----------------------------------------------------------------+----------------|
| -t    | --tag TAG                      | Add tag to measurements; multiple tags possible.               | `#{}`          | 
| -i    | --iterations ITERATIONS        | Number of iterations of each measurement.                      | `5`            | 
| -f    | --only-function FUNCTIONNAME   | Name of function to test.                                      | `#{}`          |
| -F    | --except-function FUNCTIONNAME | Name of function not to test.                                  | `#{}`          |
| -s    | --only-store STORE             | Name of store to test.                                         | `#{}`          |
| -S    | --except-store STORE           | Name of store not to test.                                     | `#{}`          |
| -n    | --store-sizes VECTOR           | Numbers of values in store for which benchmarks should be run. | `[0 1 10 100]` |
| -o    | --output-format FORMAT         | Short form of ooutput format to use.                           | `edn`          | 
| -h    | --help                         | Show help screen for tool usage.                               |                |


### Run Configuration

Options for *stores* (`-s`):
- `:memory` for in-memory database with persistent-set index
- `:file` for file store


Options for *functions* (`-f`):

- `:get`: Testing `get`.
- `:assoc`: Testing `assoc`.
- `:create`: Testing `new-memstore`/`new-fs-store`.
Run can be configured via options `-s`, `-n`, `-i`.


Options for *output formats* (`-o`)
- `:html`
- `:edn`
- `:csv`

The edn output will look as follows:

```clojure
[ ;;...
 {:store :file, 
  :function :get, 
  :size 0,
  :position :first, 
  :observations [6.964227 0.549398 0.437072 0.353058 0.351321], 
  :mean 1.7310152000000003, 
  :median 0.437072, 
  :sd 2.6176071548789284} 
  ;; ...
]
```
