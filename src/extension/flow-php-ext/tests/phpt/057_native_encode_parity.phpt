--TEST--
native columns encode byte-identical frames to PHP columns, after every Rows reshaping, and decode PHP frames
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Rows;
use Flow\Floe\FrameDecoder;

use function Flow\ETL\DSL\schema;

$schema = all_types_schema();
$php = php_rows($schema, all_types_values());
$native = native_rows($schema, all_types_values());
$frame = static fn(string $label, Rows $expected, Rows $actual) => print(
    $label . ': ' . (bin2hex($expected->encodeFrame()) === bin2hex($actual->encodeFrame()) ? 'identical' : 'DIFF') . "\n"
);
$projection = schema($schema->get('uuid'), $schema->get('int'), $schema->get('structure'));

$frame('all types', $php, $native);
$frame('slice', $php->slice(3, 3), $native->slice(3, 3));
$frame('gather', $php->gather([5, 0, 3, 3]), $native->gather([5, 0, 3, 3]));
$frame('concat', $php->concat($php->slice(1, 2)), $native->concat($native->slice(1, 2)));
$frame('project', $php->project($projection), $native->project($projection));
$frame('withSchema', $php->withSchema(all_types_schema('UTC')), $native->withSchema(all_types_schema('UTC')));
$frame('mixed', $php, $php->concat($native)->slice(0, $php->count()));

$body = $php->encodeFrame();
$decodedPhp = (new FrameDecoder())->decode($body, $schema, new PhpBackend());
$decodedNative = (new FrameDecoder())->decode($body, $schema, new DefaultBackend());
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
Flow\ETL\Column\NativeColumn
decoded rows identical
