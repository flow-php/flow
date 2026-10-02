--TEST--
RustJsonOpenSource reads, cast refusals and malformed refusals leak neither PHP memory nor native allocations
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\JSON\RustJsonOpenSource;
use Flow\ETL\Column\RustBackend;
use Flow\ETL\Exception\{RuntimeException, SchemaMismatchException};
use Flow\Filesystem\Stream\StringSourceStream;

use function Flow\ETL\DSL\{int_schema, list_schema, map_schema, schema, str_schema, structure_schema};
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\{structure_element, type_integer, type_list, type_map, type_optional, type_string, type_structure};

$backend = new RustBackend();
$schema = schema(
    int_schema('id'),
    str_schema('name', nullable: true),
    list_schema('tags', type_list(type_string())),
    map_schema('scores', type_map(type_string(), type_integer())),
    structure_schema('owner', type_structure([
        'id' => structure_element('id', type_integer()),
        'note' => structure_element('note', type_optional(type_string()), optional: true),
    ])),
);
$valid = '{"id":1,"name":"aé","tags":["x","y"],"scores":{"a":1,"a":2},"owner":{"id":1}}';
$read = static function (string $raw, bool $lines) use ($backend, $schema): void {
    $open = new RustJsonOpenSource(new StringSourceStream(path('memory://leak.json'), $raw), $lines, 'memory://leak.json', '', 0);

    try {
        foreach ($open->batches($schema, 2, $backend) as $batch) {
            $batch->toArray();
        }
    } finally {
        $open->close();
    }
};
$refused = static function (string $raw, bool $lines, string $class) use ($read): void {
    try {
        $read($raw, $lines);
        throw new LogicException('a refused read was accepted');
    } catch (Throwable $e) {
        if (!$e instanceof $class) {
            throw $e;
        }
    }
};
$cycle = static function () use ($read, $refused, $valid): void {
    $read("[{$valid},{$valid},\n{$valid}]", false);
    $read("{$valid}\n{$valid}\n\n{$valid}", true);
    $read('[{"id":"2","tags":"[\"json\"]","scores":[],"owner":{"id":"3","note":null}}]', false);
    $refused('[' . $valid . ',{"id":"x","tags":[],"scores":{},"owner":{"id":1}}]', false, SchemaMismatchException::class);
    $refused('{"id":1,"tags":[1,[2]],"scores":{},"owner":{"note":"n"}}', true, SchemaMismatchException::class);
    $refused($valid . "\n" . $valid . '}', true, RuntimeException::class);
    $refused('[' . $valid . ',' . $valid, false, RuntimeException::class);
    $refused($valid . "\n5", true, RuntimeException::class);
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);
$baselineRust = $backend->allocatedBytes();

for ($i = 0; $i < 100; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
var_dump($backend->allocatedBytes() === $baselineRust);
?>
--EXPECT--
bool(true)
bool(true)
