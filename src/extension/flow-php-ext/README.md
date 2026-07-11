# Flow PHP - Native Extension

A PHP extension written in Rust ([ext-php-rs](https://github.com/extphprs/ext-php-rs)) providing
native implementations for performance-critical parts of the Flow DataFrame framework. It encodes and
decodes the frames of the Flow **Floe** binary format (`.floe`; the canonical PHP implementation
lives in `Flow\Floe` in `flow-php/etl`). `Flow\Floe\FloeReader`/`FloeWriter` keep file header/footer
assembly in PHP and delegate frame work to the extension:

```php
$decoder = new Flow\Floe\RowsDecoder();
$decoder->schema($schemaFrameBody);
$row = $decoder->row($rowFrameBody);            // Flow\ETL\Row
$rows = $decoder->rows([$rowFrameBody, ...]);   // batched: Flow\ETL\Rows

$encoder = new Flow\Floe\RowsEncoder();
$encoder->schema($schemaFrameBody);
$rowFrameBody = $encoder->row($row);            // byte-identical to Flow\Floe\RowEncoder
$segments = $encoder->rows($rows);              // batched: framed ROW bytes per section,
                                                // list of Flow\Floe\FrameSegment
```

The extension is always optional: `FloeReader`/`FloeWriter` route to it automatically when `flow_php`
is loaded, and behavior is identical with or without it. Extension errors surface as
`Flow\Floe\Exception\ExtensionException` — there is no silent fallback. All 17 entry types are
covered; xml/xml_element/html/html_element values delegate DOM (de)construction to the pure-PHP
`Flow\Floe\ValueDecoder`/`ValueEncoder` static methods. Only `DateTime`/`DateTimeImmutable` are
supported — datetime subclasses throw.

## Build

```bash
nix-shell --arg with-rust true --run "cd src/extension/flow-php-ext && make build"
```

## Test

```bash
nix-shell --arg with-rust true --run "cd src/extension/flow-php-ext && make test"
```
