# Examples

Every published example lives under `web/landing/content/examples/topics/`. The only thing that
executes them is `PlaygroundExamplesTest`, which drives each one through the real WASM playground in
a browser and asserts it produced no `.output-error`. Execution only - output correctness is the
framework test suite's business.

## Layout

Two shapes, mutually exclusive. An example with options must not have its own `code.php`.

```
content/examples/topics/
  reading/                     # topic
    _meta.yaml
    csv/                       # example (2-level)
      _meta.yaml
      code.php                 # required
      description.md           # what the reader will learn
      documentation.md         # a sentence, then links to the concept page
  transformations/
    when/                      # example with options (3-level)
      _meta.yaml
      basic/                   # option
        _meta.yaml
        code.php
        ...
```

URLs follow the tree: `/{topic}/{example}/` and `/{topic}/{example}/{option}/`.

## `_meta.yaml`

```yaml
priority: 3          # required; ordering within the parent. 99 is rejected
hidden: false        # optional; hides it from navigation
run:                 # optional
  skip: "<reason>"   # not executed, and not run by the sweep
```

Any other key - at either level - is a hard failure. `ExampleMeta::fromDirectory()` enforces it and
the website reads every file through it, so a misspelled key breaks the page instead of failing open.
`ExampleCorpusMetaTest` checks all of them.

## Rules an example must follow

1. **One idea per example.**
2. **No argument that does not teach.** No argument equal to its default; no value that would harm a
   reader who copied it. A parameter is taught by a second snippet showing its effect, never by a
   long parameter list in the first one.
3. **The output is shown**, not just the code. The playground has no visible filesystem, so an
   example that writes must read it back and echo it. Text formats echo the bytes; binary formats
   read back through the matching Flow reader.
4. **`description.md` states what the reader will learn**, not what the method is called.
5. **`documentation.md` names the concept in a sentence**, then links the concept page it belongs to -
   not only the adapter.
6. **Output stays short enough to read**, bounded with `->limit()` or `->select()` in the example
   itself. Nothing enforces this - it is a review call.

## `require __DIR__ . '/vendor/autoload.php'`

There is no filesystem to resolve that against. The playground builds an in-memory Emscripten FS at
boot, fetches every entry of the `wasm_resources` map in `_playground.html.twig` into `/workspace/`,
and `chdir`s there - so `__DIR__` is `/workspace`. Its `vendor/autoload.php` is a shim requiring
`phar://tools/flow.phar/vendor/autoload.php`.

CI builds that phar from the current source before running the tests (`build-phar` job in
`test-website.yml`), so a pull request is always tested against its own code.

Locally the phar is whatever was last committed. After changing anything it carries:

```bash
just phar
rm -rf web/landing/public/assets && composer build:assets --working-dir=web/landing
```

Both steps are needed. `public/assets/wasm/tools/flow-<digest>.phar` is the copy the browser loads,
`composer build:assets` writes a new digest without removing the old file, and the manifest decides
which one is served - so skipping the `rm` leaves the playground running the stale one.

## Running them

The sweep is its own `examples` suite and is NOT part of `just test-website`. It takes minutes and
is run on demand.

```bash
just test-examples                             # every published example
just test-examples --filter 'reading/csv'      # one of them
just test-website                              # the rest of the website suite
```
