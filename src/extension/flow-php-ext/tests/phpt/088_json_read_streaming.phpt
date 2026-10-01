--TEST--
NativeJsonOpenSource reads a JSON array and JSON lines in memory independent of the record count: one chunk, the largest record and one batch
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\JSON\{NativeJsonOpenSource, NativeJsonReader};
use Flow\ETL\Column\DefaultBackend;
use Flow\Filesystem\Local\NativeLocalFilesystem;

use function Flow\ETL\DSL\{float_schema, int_schema, list_schema, schema, str_schema};
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\{type_list, type_string};

$backend = new DefaultBackend();
$schema = schema(int_schema('id'), str_schema('name'), list_schema('tags', type_list(type_string())), float_schema('score'));
$write = static function (string $file, int $records, bool $lines): void {
    $handle = fopen($file, 'wb');
    fwrite($handle, $lines ? '' : '[');

    for ($i = 0; $i < $records; $i += 1000) {
        $chunk = [];

        for ($j = $i; $j < min($records, $i + 1000); $j++) {
            $chunk[] = sprintf('{"id":%d,"name":"user %d","tags":["a","b%d"],"score":%d.5}', $j, $j, $j % 7, $j % 100);
        }

        fwrite($handle, ($i > 0 ? ($lines ? "\n" : ",\n") : '') . implode($lines ? "\n" : ",\n", $chunk));
    }

    fwrite($handle, $lines ? "\n" : "]\n");
    fclose($handle);
};
// rows read, PHP peak over the read, the most native bytes held while a batch was
$read = static function (string $file, bool $lines) use ($backend, $schema): array {
    gc_collect_cycles();
    memory_reset_peak_usage();
    $php = memory_get_usage(false);
    $rust = $backend->allocatedBytes();
    $held = 0;
    $rows = 0;
    $open = new NativeJsonOpenSource((new NativeLocalFilesystem())->readFrom(path($file)), new NativeJsonReader($lines, $file));

    foreach ($open->batches($schema, 1000, $backend) as $batch) {
        $rows += $batch->count();
        $held = max($held, $backend->allocatedBytes() - $rust);
    }

    $open->close();
    unset($batch, $open);

    return [$rows, memory_get_peak_usage(false) - $php, $held];
};

// the Makefile phpt runner has no CLEAN section, so the files are removed here
foreach (['array' => false, 'lines' => true] as $label => $lines) {
    $file = sys_get_temp_dir() . "/flow_php_088_{$label}.json";
    $measured = [];

    try {
        foreach ([20_000, 200_000] as $records) {
            $write($file, $records, $lines);
            $measured[$records] = $read($file, $lines);
        }
    } finally {
        @unlink($file);
    }

    [$rows20, $php20, $rust20] = $measured[20_000];
    [$rows200, $php200, $rust200] = $measured[200_000];

    echo "{$label}: {$rows20} and {$rows200} rows, PHP peak ", $php200 <= $php20 + NativeJsonOpenSource::CHUNK ? 'bounded' : "{$php20} -> {$php200}",
        ', native held ', $rust200 <= $rust20 + NativeJsonOpenSource::CHUNK ? 'bounded' : "{$rust20} -> {$rust200}", "\n";
}
?>
--EXPECT--
array: 20000 and 200000 rows, PHP peak bounded, native held bounded
lines: 20000 and 200000 rows, PHP peak bounded, native held bounded
