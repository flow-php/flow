--TEST--
The native CSV and JSON writers leak neither PHP memory nor native allocations over 1 000 encodes, refusals included
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\CSV\IO\Loader\CSVWriteOptions;
use Flow\ETL\Adapter\CSV\IO\Loader\RustCSVEncoder;
use Flow\ETL\Adapter\CSV\IO\Loader\PhpCSVEncoder;
use Flow\ETL\Adapter\JSON\IO\Loader\RustJsonEncoder;
use Flow\ETL\Adapter\JSON\IO\Loader\PhpJsonEncoder;
use Flow\ETL\Values\Column\RustBackend;

use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_list;

$schema = all_types_schema();
$native = native_rows($schema, all_types_values());
$php = php_rows($schema, all_types_values());
// the string column holds bytes that are not UTF-8: JSON writes the batch without it
$jsonSchema = schema(...array_filter($schema->definitions(), static fn($definition): bool => $definition->name() !== 'string'));
$refusedSchema = schema(str_schema('s'), float_schema('f'), list_schema('l', type_list(type_float())));
$refused = native_rows($refusedSchema, [['s' => "\xff", 'f' => NAN, 'l' => [INF]]]);
$dateTimeFormat = 'Y-m-d\TH:i:s.uP T';
$csv = new RustCSVEncoder(new CSVWriteOptions(',', '"', '\\', "\n", $dateTimeFormat, 'Y-m-d'), new PhpCSVEncoder(new CSVWriteOptions(newLineSeparator: "\n", dateTimeFormat: $dateTimeFormat)));
$json = new RustJsonEncoder(new PhpJsonEncoder(JSON_THROW_ON_ERROR, $dateTimeFormat));
// stand-ins for the held encoder: a cell list of the wrong length, and an encoder that throws
$short = new class {
    public int $flags = JSON_THROW_ON_ERROR;

    public string $dateTimeFormat = 'Y-m-d\TH:i:s.uP T';

    public string $dateFormat = 'Y-m-d';

    public function cells(): array
    {
        return ['too', 'few'];
    }

    public function fragments(): array
    {
        return ['"too"', '"few"'];
    }
};
$throwing = new class {
    public int $flags = JSON_THROW_ON_ERROR;

    public string $dateTimeFormat = 'Y-m-d\TH:i:s.uP T';

    public string $dateFormat = 'Y-m-d';

    public function cells(): array
    {
        throw new Flow\ETL\Foundation\Exception\RuntimeException('cells refused');
    }

    public function fragments(): array
    {
        throw new Flow\ETL\Foundation\Exception\RuntimeException('fragments refused');
    }
};
$backend = new RustBackend();

$cycle = static function () use ($schema, $jsonSchema, $native, $php, $refused, $csv, $json, $short, $throwing, $dateTimeFormat): void {
    $csv->encode($native);
    // PHP columns are adopted for the encode and freed after it
    $csv->encode($php);
    $json->encode($native->project($jsonSchema, new Flow\ETL\Values\Column\RustBackend()), "\n");
    $json->encode($php->project($jsonSchema, new Flow\ETL\Values\Column\PhpBackend()), ',');

    foreach ([
        static fn() => $csv->encode($refused),
        static fn() => $json->encode($refused, "\n"),
        static fn() => $json->encode($native, "\n"),
        static fn() => (new RustCSVEncoder(new CSVWriteOptions(',', '"', '\\', "\n", $dateTimeFormat, 'Y-m-d'), $short))->encode($native),
        static fn() => (new RustJsonEncoder($short))->encode($native->project($jsonSchema, new Flow\ETL\Values\Column\RustBackend()), "\n"),
        static fn() => (new RustCSVEncoder(new CSVWriteOptions(',', '"', '\\', "\n", $dateTimeFormat, 'Y-m-d'), $throwing))->encode($native),
        static fn() => (new RustJsonEncoder($throwing))->encode($native->project($jsonSchema, new Flow\ETL\Values\Column\RustBackend()), "\n"),
    ] as $refusal) {
        try {
            $refusal();
            throw new LogicException('a refused batch was written');
        } catch (JsonException|Flow\ETL\Foundation\Exception\RuntimeException|Flow\ETL\Foundation\Exception\InvalidArgumentException) {
        }
    }
};

for ($i = 0; $i < 10; $i++) {
    $cycle();
}
gc_collect_cycles();
$baseline = memory_get_usage(false);
$baselineRust = $backend->allocatedBytes();

for ($i = 0; $i < 1_000; $i++) {
    $cycle();
}
gc_collect_cycles();

var_dump(memory_get_usage(false) <= $baseline);
var_dump($backend->allocatedBytes() === $baselineRust);
?>
--EXPECT--
bool(true)
bool(true)
