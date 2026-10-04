--TEST--
native and PHP columns build into each other: appendFrom/appendTake, Rows::concat, Rows::gather, adopt() both ways
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Column\RustBackend;
use Flow\ETL\Column\RustColumn;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Rows;

$schema = all_types_schema();
$php = php_rows($schema, all_types_values());
$native = native_rows($schema, all_types_values());
$same = static fn(string $label, Rows $expected, Rows $actual) => print(
    $label . ': ' . ($expected->encodeFrame() === $actual->encodeFrame() && comparable($expected->toArray()) === comparable($actual->toArray()) ? 'identical' : 'DIFF') . "\n"
);
$rebuilt = static function (Rows $from, object $backend, bool $take) use ($schema): Rows {
    $columns = [];

    foreach ($schema->definitions() as $name => $definition) {
        $builder = $backend->builder($definition);

        if ($take) {
            $builder->appendTake($from->column((string) $name), [5, 0, 3, 3]);
        } else {
            foreach ([5, 0, 3, 3] as $i) {
                $builder->appendFrom($from->column((string) $name), $i);
            }
        }

        $columns[$name] = $builder->finish();
    }

    return Rows::fromColumns($schema, $columns, 4);
};

$same('native appendTake of a PHP column', $php->gather([5, 0, 3, 3]), $rebuilt($php, new RustBackend(), true));
$same('native appendFrom of a PHP column', $php->gather([5, 0, 3, 3]), $rebuilt($php, new RustBackend(), false));
$same('PHP appendTake of a native column', $php->gather([5, 0, 3, 3]), $rebuilt($native, new PhpBackend(), true));
$same('Rows::concat PHP + native', $php->concat(new PhpBackend(), $php), $php->concat(new RustBackend(), $native));
$same('Rows::concat native + PHP', $php->concat(new PhpBackend(), $php), $native->concat(new PhpBackend(), $php));
$same('Rows::gather over both', $php->gather([1, 4]), $native->gather([1, 4]));

foreach ($schema->definitions() as $name => $definition) {
    $phpColumn = $php->column((string) $name);
    $nativeColumn = $native->column((string) $name);
    $adopted = (new RustBackend())->adopt($definition, $phpColumn);
    $empty = (new RustBackend())->adopt($definition, $phpColumn->slice(0, 0));
    $back = (new PhpBackend())->adopt($definition, $nativeColumn);

    if (!$adopted instanceof RustColumn || $adopted->encode() !== $phpColumn->encode() || $empty->count() !== 0
        || $back instanceof RustColumn || $back->encode() !== $phpColumn->encode()
        || (new RustBackend())->adopt($definition, $nativeColumn) !== $nativeColumn) {
        echo "adopt DIFF {$name}\n";
    }
}

echo "adopt identical\n";
?>
--EXPECT--
native appendTake of a PHP column: identical
native appendFrom of a PHP column: identical
PHP appendTake of a native column: identical
Rows::concat PHP + native: identical
Rows::concat native + PHP: identical
Rows::gather over both: identical
adopt identical
