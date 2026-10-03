--TEST--
RustJsonOpenSource reads random valid JSON, whole or a byte at a time, into the frames or refusals of PhpJsonOpenSource, through either backend
--SKIPIF--
<?php if (!extension_loaded("flow_php")) die("skip flow_php extension not loaded"); ?>
--FILE--
<?php
require __DIR__ . '/bootstrap.php';

use Flow\ETL\Adapter\JSON\JSONMachine\{JsonFileReader, JsonFormat};
use Flow\ETL\Adapter\JSON\{PhpJsonOpenSource, RustJsonOpenSource};
use Flow\ETL\Column\{RustBackend, RustColumn, PhpBackend};
use Flow\ETL\Extractor\File\SourceFile;
use Flow\ETL\{Rows, Schema};
use Flow\Filesystem\Stream\StringSourceStream;

use function Flow\ETL\DSL\{bool_schema, date_schema, datetime_schema, float_schema, int_schema, json_schema, list_schema, map_schema, schema, str_schema, structure_schema, uuid_schema};
use function Flow\Filesystem\DSL\{memory_filesystem, path};
use function Flow\Types\DSL\{structure_element, type_integer, type_list, type_map, type_optional, type_string, type_structure};

/**
 * Each definition with JSON texts drawn for it: its type, castable strings and nulls, then (prefixed "!") values its
 * cast is expected to refuse.
 *
 * @return list<array{callable(string): object, list<string>}>
 */
function json_read_pool(): array
{
    $u = '\\' . 'u';
    $ints = ['1', '-0', '42', '9223372036854775807', '-9223372036854775808', '"42"', '1E2', 'null', '!9223372036854775808', '!"x"', '!1.5', '!true', '![1]'];
    $strings = ['"a"', "\"{$u}00e9\"", "\"{$u}d83d{$u}de00\"", '"a\/b"', '""', '"tab\there"', '5', '-0.0', 'true', 'null', '![1]', '!{"a":1}'];
    $person = type_structure([
        'id' => structure_element('id', type_integer()),
        'name' => structure_element('name', type_optional(type_string()), optional: true),
    ]);
    $people = ['{"id":1,"name":"a"}', '{"id":1}', '{"id":"1","name":null}', '{"id":1,"name":"a","extra":[1]}', '{"id":1,"id":2}', '{"name":"b","id":3}', 'null', '!{"name":"a"}', '!{"id":1,"name":[5]}', '![1,"a"]', '!"x"'];

    return [
        [static fn(string $name): object => int_schema($name), $ints],
        [static fn(string $name): object => int_schema($name, nullable: true), $ints],
        [static fn(string $name): object => float_schema($name), ['0.1', '-0.0', '1e400', '-1e400', '1e-400', '1E2', '5', '-0', '"3.14"', 'null', '!"abc"', '![1]']],
        [static fn(string $name): object => bool_schema($name), ['true', 'false', '"true"', '"off"', '"yes"', '1', '0', 'null', '!"x"', '![1]']],
        [static fn(string $name): object => str_schema($name), $strings],
        [static fn(string $name): object => str_schema($name, nullable: true), $strings],
        [static fn(string $name): object => datetime_schema($name), ['"2026-01-02T03:04:05Z"', '"2026-01-02 03:04:05"', '"2026-01-02T03:04:05.123456+02:00"', 'null', '!"@1700000000"', '!"not a date"', '![1]']],
        [static fn(string $name): object => date_schema($name), ['"2026-01-02"', '"2026-01-02T10:00:00Z"', 'null', '!"x"', '![1]']],
        [static fn(string $name): object => uuid_schema($name), ['"6c4b5d1e-4f2a-4b8e-9c3d-2e1f0a9b8c7d"', 'null', '!"6C4B5D1E-4F2A-4B8E-9C3D-2E1F0A9B8C7D"', '!"6c4b5d1e"', '!1']],
        [static fn(string $name): object => json_schema($name), ['{"a":1}', '{"a":1.0,"b":"\/"}', '[1,2]', '[]', '{}', '"{\"a\":1}"', "{\"{$u}00e9\":\"{$u}d83d{$u}de00\"}", 'null', '!"x"', '!1']],
        [static fn(string $name): object => list_schema($name, type_list(type_integer())), ['[1,2]', '[]', '["1"]', '{"0":1,"1":2}', 'null', '!["x"]', '![1,null]', '!5']],
        [static fn(string $name): object => map_schema($name, type_map(type_string(), type_integer())), ['{"a":1,"b":2}', '{"a":1,"a":"2"}', '{}', "{\"{$u}00e9\":3}", 'null', '!{"a":"x"}', '!5']],
        [static fn(string $name): object => map_schema($name, type_map(type_integer(), type_string())), ['{"1":"a","2":"b"}', '["a","b"]', '{"-1":"y","2":"a","2":"b"}', 'null', '!{"a":"b"}', '!{"01":"x"}', '!5']],
        [static fn(string $name): object => structure_schema($name, $person), $people],
        [static fn(string $name): object => list_schema($name, type_list($person)), ['[' . $people[0] . ',' . $people[1] . ']', '[' . $people[4] . ',' . $people[5] . ']', '[]', 'null', '![' . ltrim($people[7], '!') . ']']],
        [static fn(string $name): object => map_schema($name, type_map(type_string(), type_list(type_integer()))), ['{"a":[1,2],"b":[]}', '{"a":["2"],"a":[3]}', 'null', '!{"a":[1,"x"]}', '!{"a":[[1]]}']],
        [static fn(string $name): object => structure_schema($name, type_structure(['tags' => structure_element('tags', type_list(type_map(type_string(), type_integer())))])), ['{"tags":[{"a":1},{"b":2,"b":3}]}', '{"tags":[]}', '{"tags":[{"a":"1"}]}', 'null', '!{"tags":[{"a":[1]}]}', '!{}']],
    ];
}

/**
 * A member name as JSON text, sometimes with escapes that unescape to the same name.
 */
function json_read_key(string $name): string
{
    return match (mt_rand(0, 3)) {
        0 => '"' . implode('', array_map(static fn(string $char): string => sprintf('\\u%04x', ord($char)), str_split($name))) . '"',
        default => '"' . $name . '"',
    };
}

/**
 * Draws from a column's pool: a refusing draw ("!") only when $refuse, a null or an absence only when $nullable or
 * $refuse.
 *
 * @param list<string> $pool
 */
function json_read_draw(array $pool, bool $nullable, bool $refuse): string
{
    $draws = array_values(array_filter($pool, static fn(string $draw): bool => $refuse || (!str_starts_with($draw, '!') && ($nullable || $draw !== 'null'))));

    return ltrim($draws[mt_rand(0, count($draws) - 1)], '!');
}

/**
 * @param list<array{string, list<string>, bool}> $columns name, draw pool and nullability of every column
 */
function json_read_record(array $columns, bool $array, bool $refuse): string
{
    if ($array) {
        return '[' . implode(', ', array_map(static fn(array $column): string => json_read_draw($column[1], $column[2], $refuse), $columns)) . ']';
    }

    $members = [];

    foreach ($columns as [$name, $pool, $nullable]) {
        $draw = static fn(): string => json_read_draw($pool, $nullable, $refuse);

        // a duplicated key keeps its last value, so both stay in one group, in order
        match (($nullable || $refuse) ? mt_rand(0, 9) : mt_rand(1, 9)) {
            0 => null,
            1 => $members[] = json_read_key($name) . ':' . json_read_draw($pool, true, true) . ', ' . json_read_key($name) . ':' . $draw(),
            default => $members[] = json_read_key($name) . ' : ' . $draw(),
        };

        if (mt_rand(0, 9) === 0) {
            $members[] = '"extra' . '\\' . 'u00e9" :' . json_read_draw($pool, true, true);
        }
    }

    shuffle($members);

    return '{' . implode(",\n ", $members) . '}';
}

$frames = static function (iterable $batches, object $backend): array {
    $frames = [];

    foreach ($batches as $batch) {
        foreach ($batch->columns() as $column) {
            if ($backend instanceof PhpBackend && $column instanceof RustColumn) {
                return ['a RustColumn survived PhpBackend'];
            }
        }

        $frames[] = bin2hex($batch->encodeFrame());
    }

    return $frames;
};
// RustJsonOpenSource::batches() over reads of $chunk bytes
$bytes = static fn(string $raw, bool $lines, Schema $schema, object $backend, int $chunk): Iterator => (new RustJsonOpenSource(
    chunked_source_stream($raw, $chunk),
    $lines,
    'memory://phpt.json',
    '',
    0,
))->batches($schema, 2, $backend);

mt_srand(10);
$pool = json_read_pool();
$cases = 0;
$identical = 0;
$refused = 0;
$nulls = 0;

for ($t = 0; $t < 400; $t++) {
    $array = mt_rand(0, 4) === 0;
    $columns = [];
    $definitions = [];

    for ($c = mt_rand(1, 4) - 1; $c >= 0; $c--) {
        [$definition, $draws] = $pool[mt_rand(0, count($pool) - 1)];
        $name = $array ? (string) count($columns) : 'c' . count($columns);
        $definitions[] = $definition($name);
        $columns[] = [$name, $draws, end($definitions)->isNullable()];
    }

    $schema = schema(...$definitions);
    $refuse = mt_rand(0, 3) === 0;
    $records = [];

    for ($r = mt_rand(1, 5); $r > 0; $r--) {
        $records[] = mt_rand(0, 9) === 0 ? ['{}', '[ ]'][mt_rand(0, 1)] : json_read_record($columns, $array, $refuse);
    }

    foreach ([true, false] as $lines) {
        $raw = $lines
            ? implode(["\n", "\r\n", "\n \n"][mt_rand(0, 2)], array_map(static fn(string $record): string => str_replace("\n", '', $record), $records)) . ["\n", ''][mt_rand(0, 1)]
            : "[\n" . implode(" ,\n\t", $records) . "\n]\n";
        $filesystem = memory_filesystem();
        $stream = $filesystem->writeTo(path('memory://phpt.json'));
        $stream->append($raw);
        $stream->close();
        $source = new SourceFile(path('memory://phpt.json'));
        $reader = new JsonFileReader($filesystem, $lines ? JsonFormat::Lines : JsonFormat::Document, null, false, [$source]);
        $php = outcome(static fn(): array => $frames((new PhpJsonOpenSource($reader, $source))->batches($schema, 2, new PhpBackend()), new PhpBackend()));

        foreach ([new RustBackend(), new PhpBackend()] as $backend) {
            $natives = [
                'whole' => outcome(static fn(): array => $frames((new RustJsonOpenSource(new StringSourceStream(path('memory://phpt.json'), $raw), $lines, 'memory://phpt.json', '', 0))->batches($schema, 2, $backend), $backend)),
                'by byte' => outcome(static fn(): array => $frames($bytes($raw, $lines, $schema, $backend, 1), $backend)),
            ];

            foreach ($natives as $feed => $native) {
                $cases++;

                if ($native !== $php) {
                    echo ($lines ? 'lines ' : 'document ') . "{$feed} " . get_class($backend) . ' ' . json_encode($raw) . "\n  php:    {$php}\n  native: {$native}\n";

                    continue;
                }

                $identical++;
                $refused += refused($php) ? 1 : 0;
                $nulls += str_contains($raw, 'null') ? 1 : 0;
            }
        }
    }
}

echo "{$identical} of {$cases} identical, {$refused} refused, {$nulls} with a null\n";
?>
--EXPECT--
3200 of 3200 identical, 600 refused, 936 with a null
