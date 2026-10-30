--TEST--
native columns encode byte-identical frames to PHP columns, after every Rows reshaping, and decode PHP frames
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Values\Column\RustBackend;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Column\Frame\NoopCodec;

use function Flow\ETL\DSL\schema;

$schema = all_types_schema();
$php = php_rows($schema, all_types_values());
$native = native_rows($schema, all_types_values());
$frame = static fn(string $label, Rows $expected, Rows $actual) => print(
    $label . ': ' . (bin2hex((new PhpBackend())->encodeFrame($expected, new NoopCodec())) === bin2hex((new PhpBackend())->encodeFrame($actual, new NoopCodec())) ? 'identical' : 'DIFF') . "\n"
);
$projection = schema($schema->get('uuid'), $schema->get('int'), $schema->get('structure'));

$frame('all types', $php, $native);
$frame('slice', $php->slice(3, 3), $native->slice(3, 3));
$frame('gather', $php->gather([5, 0, 3, 3]), $native->gather([5, 0, 3, 3]));
$frame('concat', Rows::concat($php->schema(), new PhpBackend(), $php, $php->slice(1, 2)), Rows::concat($native->schema(), new RustBackend(), $native, $native->slice(1, 2)));
$frame('project', $php->project($projection, new Flow\ETL\Values\Column\PhpBackend()), $native->project($projection, new Flow\ETL\Values\Column\RustBackend()));
$frame('withSchema', $php->withSchema(all_types_schema('UTC')), $native->withSchema(all_types_schema('UTC')));
$frame('mixed', $php, Rows::concat($php->schema(), new PhpBackend(), $php, $native)->slice(0, $php->count()));

$body = (new PhpBackend())->encodeFrame($php, new NoopCodec());
$decodedPhp = (new PhpBackend())->decodeFrame($body, $schema, new NoopCodec());
$decodedNative = (new RustBackend())->decodeFrame($body, $schema, new NoopCodec());
echo get_class($decodedNative->column('int')), "\n";
echo comparable($decodedPhp->toArray()) === comparable($decodedNative->toArray()) ? 'decoded rows identical' : 'DIFF', "\n";
?>
--EXPECT--
all types: identical
slice: identical
gather: identical
concat: identical
project: identical
withSchema: identical
mixed: identical
Flow\ETL\Values\Column\RustColumn
decoded rows identical
