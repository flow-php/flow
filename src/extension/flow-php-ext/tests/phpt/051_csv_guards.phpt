--TEST--
native CSV classes reject an invalid dialect, batch size, fold limit and HTML/XML candidates
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\RustColumnFoldNative;
use Flow\ETL\Adapter\CSV\RustCSVReaderNative;

expect_exception(static fn() => new RustCSVReaderNative(',,', '"', '\\', true, true, true));
expect_exception(static fn() => new RustCSVReaderNative(',', '', '\\', true, true, true));
expect_exception(static fn() => new RustCSVReaderNative(',', '"', '\\\\', true, true, true));
expect_exception(static fn() => (new RustCSVReaderNative(',', '"', '\\', true, true, true))->next(0));
expect_exception(static fn() => (new RustCSVReaderNative(',', '"', '\\', true, true, true))->next(-1));
$fold = new RustColumnFoldNative([], ['integer']);
expect_exception(static fn() => (new RustCSVReaderNative(',', '"', '\\', true, true, true))->fold($fold, -2));
expect_exception(static fn() => new RustColumnFoldNative([], ['integer', 'html']));
expect_exception(static fn() => new RustColumnFoldNative([], ['xml']));
?>
--EXPECT--
Flow\Floe\Exception\ExtensionException: flow_php CSV separator must be exactly one byte
Flow\Floe\Exception\ExtensionException: flow_php CSV enclosure must be exactly one byte
Flow\Floe\Exception\ExtensionException: flow_php CSV escape must be empty or exactly one byte
Flow\Floe\Exception\ExtensionException: flow_php CSV batch size must be greater than 0
Flow\Floe\Exception\ExtensionException: flow_php CSV batch size must be greater than 0
Flow\Floe\Exception\ExtensionException: flow_php CSV fold limit must be -1 or at least 0
Flow\Floe\Exception\ExtensionException: flow_php cannot fold html or xml candidates natively
Flow\Floe\Exception\ExtensionException: flow_php cannot fold html or xml candidates natively
