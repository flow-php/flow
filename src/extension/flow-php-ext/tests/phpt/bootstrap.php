<?php

declare(strict_types=1);

require __DIR__ . '/../../../../../vendor/autoload.php';

use Flow\ETL\Adapter\Parquet\RustParquetOpenSource;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Column\RustBackend;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Engine\RustParquetFileReader;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\enum_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\ETL\DSL\xml_schema;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

/**
 * One column per Definition (html only where \Dom\HTMLDocument exists).
 */
function all_types_schema(string $zone = 'Europe/Warsaw'): Schema
{
    return schema(
        int_schema('int'),
        str_schema('string', nullable: true),
        float_schema('float'),
        bool_schema('bool'),
        datetime_schema('datetime', zone: $zone),
        date_schema('date'),
        time_schema('time'),
        uuid_schema('uuid'),
        json_schema('json'),
        enum_schema('enum', BasicEnum::class),
        time_zone_schema('time_zone'),
        xml_schema('xml'),
        list_schema('list', type_list(type_optional(type_integer()))),
        map_schema('map', type_map(type_string(), type_integer())),
        structure_schema('structure', type_structure([
            'a' => type_integer(),
            'b' => structure_element('b', type_optional(type_string()), true),
        ])),
        null_schema('null'),
        ...class_exists('\Dom\HTMLDocument') ? [html_schema('html')] : [],
    );
}

/**
 * Six rows: bytes that are not UTF-8, a pre-1970 datetime with microseconds, a DST pair, a negative time.
 *
 * @return list<array<string, mixed>>
 */
function all_types_values(): array
{
    $negative = (new DateTimeImmutable('2024-01-01 01:02:03'))->diff(new DateTimeImmutable('2024-01-01'));
    $rows = [
        [
            'int' => 1,
            'string' => "a\xFFb",
            'float' => 1.5,
            'bool' => true,
            'datetime' => '1969-07-20T20:17:40.123456Z',
            'date' => '1969-12-29',
            'time' => new DateInterval('PT1H2M3S'),
            'uuid' => '6c2f1d4e-8b3a-4c5d-9e6f-0a1b2c3d4e5f',
            'json' => '{"a":1}',
            'enum' => BasicEnum::one,
            'time_zone' => new DateTimeZone('Europe/Warsaw'),
            'xml' => '<a>1</a>',
            'list' => [1, null, 3],
            'map' => ['a' => 1],
            'structure' => ['a' => 1, 'b' => 'x'],
            'null' => null,
        ],
        [
            'int' => -2,
            'string' => null,
            'float' => -0.0,
            'bool' => false,
            'datetime' => '2024-10-27T00:30:00Z',
            'date' => '2024-02-29',
            'time' => $negative,
            'uuid' => '00000000-0000-0000-0000-000000000000',
            'json' => '[1,2]',
            'enum' => BasicEnum::two,
            'time_zone' => new DateTimeZone('UTC'),
            'xml' => '<b/>',
            'list' => [],
            'map' => [],
            'structure' => ['a' => 2],
            'null' => null,
        ],
        [
            'int' => PHP_INT_MAX,
            'string' => '',
            'float' => 1e300,
            'bool' => true,
            'datetime' => '2024-10-27T01:30:00Z',
            'date' => '2000-01-01',
            'time' => new DateInterval('PT0S'),
            'uuid' => 'ffffffff-ffff-ffff-ffff-ffffffffffff',
            'json' => '{}',
            'enum' => BasicEnum::three,
            'time_zone' => new DateTimeZone('America/New_York'),
            'xml' => '<c a="1">t</c>',
            'list' => [null],
            'map' => ['b' => 2, 'c' => 3],
            'structure' => ['a' => 3, 'b' => null],
            'null' => null,
        ],
        [
            'int' => PHP_INT_MIN,
            'string' => 'zażółć',
            'float' => 0.1,
            'bool' => false,
            'datetime' => '1970-01-01T00:00:00Z',
            'date' => '1970-01-01',
            'time' => new DateInterval('PT49H'),
            'uuid' => '6c2f1d4e-8b3a-4c5d-9e6f-0a1b2c3d4e5f',
            'json' => '[true]',
            'enum' => BasicEnum::one,
            'time_zone' => new DateTimeZone('Europe/Warsaw'),
            'xml' => '<d/>',
            'list' => [4],
            'map' => ['a' => 4],
            'structure' => ['a' => 4, 'b' => 'y'],
            'null' => null,
        ],
        [
            'int' => 0,
            'string' => 'e',
            'float' => 2.0,
            'bool' => true,
            'datetime' => '2038-01-19T03:14:08.000001Z',
            'date' => '1900-03-01',
            'time' => new DateInterval('PT59M59S'),
            'uuid' => '12345678-1234-1234-1234-123456789abc',
            'json' => '{"b":[1,{"c":null}]}',
            'enum' => BasicEnum::two,
            'time_zone' => new DateTimeZone('Asia/Tokyo'),
            'xml' => '<e>5</e>',
            'list' => [5, 6],
            'map' => ['z' => 0],
            'structure' => ['a' => 5],
            'null' => null,
        ],
        [
            'int' => 7,
            'string' => 'last',
            'float' => -3.25,
            'bool' => false,
            'datetime' => '1900-01-01T12:00:00.5Z',
            'date' => '2100-12-31',
            'time' => new DateInterval('PT1S'),
            'uuid' => 'abcdefab-cdef-abcd-efab-cdefabcdefab',
            'json' => '[]',
            'enum' => BasicEnum::three,
            'time_zone' => new DateTimeZone('UTC'),
            'xml' => '<f/>',
            'list' => [7, null],
            'map' => ['q' => 7],
            'structure' => ['a' => 7, 'b' => 'z'],
            'null' => null,
        ],
    ];

    if (class_exists('\\Dom\\HTMLDocument')) {
        foreach ($rows as $index => $row) {
            $rows[$index] = $row + ['html' => '<!DOCTYPE html><html><head></head><body><p>html</p></body></html>'];
        }
    }

    return $rows;
}

/**
 * @param list<array<array-key, mixed>> $values
 */
function php_rows(Schema $schema, array $values): Rows
{
    return (new RowsBuilder($schema, new PhpBackend()))
        ->appendRows($values)
        ->finish();
}

/**
 * @param list<array<array-key, mixed>> $values
 */
function native_rows(Schema $schema, array $values): Rows
{
    return (new RowsBuilder($schema, new RustBackend()))
        ->appendRows($values)
        ->finish();
}

/**
 * `serialize()` with DOM values, which it refuses, replaced by their class and markup.
 */
function comparable(mixed $value): string
{
    $plain = static function (mixed $value) use (&$plain): mixed {
        if (is_array($value)) {
            return array_map($plain, $value);
        }

        if ($value instanceof DOMDocument) {
            return [$value::class, $value->saveXML()];
        }

        if ($value instanceof DOMNode) {
            return [$value::class, $value->ownerDocument?->saveXML($value)];
        }

        if (!is_object($value) || !str_starts_with($value::class, 'Dom\\')) {
            return $value;
        }

        // @mago-expect analysis:unavailable-method,mixed-method-access,ambiguous-object-property-access
        return [
            $value::class,
            $value instanceof Dom\HTMLDocument ? $value->saveHtml() : $value->ownerDocument->saveHtml($value),
        ];
    };

    return serialize($plain($value));
}

/**
 * `RustParquetOpenSource::batches()` of `$schema`'s columns of the Parquet file in `$stream`, adopted by `RustBackend`.
 *
 * @param int<1, max> $batchSize
 */
function rust_parquet_batches(SourceStream $stream, Schema $schema, int $batchSize, ?int $offset, ?int $limit): Iterator
{
    return (new RustParquetOpenSource(new RustParquetFileReader($stream)))->batches(
        $schema,
        $batchSize,
        $offset,
        $limit,
        new RustBackend(),
    );
}

/**
 * `comparable()` of the result, or `class: message <- previous class: message` of what it threw.
 */
function outcome(callable $fn): string
{
    try {
        return comparable($fn());
    } catch (Throwable $e) {
        $previous = $e->getPrevious();

        return (
            get_class($e)
            . ': '
            . $e->getMessage()
            . ($previous === null ? '' : ' <- ' . get_class($previous) . ': ' . $previous->getMessage())
        );
    }
}

/**
 * Whether an outcome() is a refusal: the class and message of what the callable threw.
 */
function refused(string $outcome): bool
{
    return (bool) preg_match('/^[A-Za-z\\\\]+(Exception|Error|Overflow): /', $outcome);
}

function assert_same_outcome(callable $php, callable $native): void
{
    $expected = outcome($php);
    $actual = outcome($native);

    echo $expected === $actual ? "identical\n" : "php:    {$expected}\nnative: {$actual}\n";
}

/**
 * JSON of the interfaces flow_php registers as reflection sees them: methods, parameters, types, optional, variadic, by-ref.
 */
function interfaces_reflection(): string
{
    $interfaces = [];

    foreach ([
        'Flow\\ETL\\Column\\Backend',
        'Flow\\ETL\\Column\\Column',
        'Flow\\ETL\\Column\\ColumnBuilder',
        'Flow\\ETL\\Adapter\\Parquet\\ParquetOpenSink',
        'Flow\\ETL\\Adapter\\CSV\\CSVEncoder',
        'Flow\\ETL\\Adapter\\JSON\\JsonEncoder',
        'Flow\\ETL\\Adapter\\CSV\\CSVOpenSource',
        'Flow\\ETL\\Adapter\\JSON\\JsonOpenSource',
        'Flow\\ETL\\Adapter\\Parquet\\ParquetOpenSource',
    ] as $name) {
        $class = new ReflectionClass($name);
        // PHP 8.5 reports a userland `self` resolved to the class, an internal one stays `self` - the same type
        $type = static fn(?ReflectionType $type): string => (string) $type === 'self' ? $name : (string) $type;
        $interfaces[$name] = [
            $class->isInterface(),
            array_map(static fn(ReflectionMethod $method): array => [
                $method->getName(),
                $type($method->getReturnType()),
                array_map(static fn(ReflectionParameter $parameter): array => [
                    $parameter->getName(),
                    $type($parameter->getType()),
                    $parameter->isOptional(),
                    $parameter->isVariadic(),
                    $parameter->isPassedByReference(),
                ], $method->getParameters()),
            ], $class->getMethods()),
        ];
    }

    return (string) json_encode($interfaces);
}

function expect_exception(callable $fn): void
{
    try {
        $fn();
        echo "FAIL: no exception thrown\n";
    } catch (Flow\ETL\Exception\RuntimeException $e) {
        echo get_class($e), ': ', $e->getMessage(), "\n";
    }
}

/**
 * The PHP CSV path - CSVLineReader + CSVDecoder::decode() - as `[headers, list of array<array-key, mixed>::$values]`.
 *
 * @return array{list<string>, list<array<array-key, mixed>>}
 */
function csv_php_rows(
    string $raw,
    string $separator,
    string $enclosure,
    string $escape,
    bool $withHeader = true,
    bool $emptyToNull = true,
    bool $removeBOM = true,
): array {
    $decoder = new Flow\ETL\Adapter\CSV\CSVDecoder(
        withHeader: $withHeader,
        separator: $separator,
        enclosure: $enclosure,
        escape: $escape,
        emptyToNull: $emptyToNull,
    );
    $lines = new Flow\ETL\Adapter\CSV\CSVLineReader($enclosure, $separator, $escape, removeBOM: $removeBOM);
    $rows = [];

    foreach ($lines->readLines(
        new Flow\Filesystem\Stream\StringSourceStream(Flow\Filesystem\DSL\path('memory://phpt.csv'), $raw),
    ) as $line) {
        foreach ($decoder->decode([$line]) as $values) {
            $rows[] = $values;
        }
    }

    return [$decoder->headers() ?? [], $rows];
}

/**
 * RustCSVOpenSource over `$raw`, read in `$chunk`-byte pieces, as `[headers, list of array<array-key, mixed>::$values]`.
 *
 * @param positive-int $chunk
 *
 * @return array{list<string>, list<array<array-key, mixed>>}
 */
function csv_native_rows(
    string $raw,
    string $separator,
    string $enclosure,
    string $escape,
    bool $withHeader = true,
    bool $emptyToNull = true,
    bool $removeBOM = true,
    int $chunk = 4096,
): array {
    $source = new Flow\ETL\Adapter\CSV\RustCSVOpenSource(
        new Flow\Filesystem\Stream\StringSourceStream(Flow\Filesystem\DSL\path('memory://phpt.csv'), $raw),
        $separator,
        $enclosure,
        $escape,
        $withHeader,
        $emptyToNull,
        $removeBOM,
        $chunk,
    );
    $rows = iterator_to_array($source->records(), false);

    return [$source->headers(), $rows];
}

/**
 * Prints `<label>: identical`, or both sides when they differ.
 *
 * @param array{list<string>, list<array<array-key, mixed>>} $expected
 * @param array{list<string>, list<array<array-key, mixed>>} $actual
 */
function assert_csv_identical(string $label, array $expected, array $actual): void
{
    echo
        $label,
        ': ',
        $expected === $actual
            ? 'identical'
            : 'FAIL php=' . var_export($expected, true) . ' native=' . var_export($actual, true),
        "\n";
}

/**
 * The values of StringTypeNarrowerTest::fixtureValues(), read from its source - PHPUnit is not loadable here, and a
 * copy would drift from the test the native narrower must agree with.
 *
 * @return list<string>
 */
function string_type_narrower_fixture_values(): array
{
    $lexemes = token_get_all((string) file_get_contents(__DIR__
    . '/../../../../lib/types/tests/Flow/Types/Tests/Unit/Type/Native/String/StringTypeNarrowerTest.php'));
    $significant = array_values(array_filter(
        $lexemes,
        static fn(array|string $lexeme): bool => (
            !is_array($lexeme) || !in_array($lexeme[0], [T_WHITESPACE, T_COMMENT, T_DOC_COMMENT], true)
        ),
    ));
    $values = [];
    $inside = false;
    $depth = 0;

    foreach ($significant as $index => $lexeme) {
        if (is_array($lexeme) && $lexeme[0] === T_STRING && $lexeme[1] === 'fixtureValues') {
            $inside = true;
        }

        if (!$inside) {
            continue;
        }

        if ($lexeme === '{') {
            $depth++;
        } elseif ($lexeme === '}' && --$depth === 0) {
            break;
        }

        if (
            is_array($lexeme)
            && $lexeme[0] === T_CONSTANT_ENCAPSED_STRING
            && ($significant[$index - 1] ?? null) === '['
            && is_array($significant[$index - 2] ?? null)
            && $significant[$index - 2][0] === T_DOUBLE_ARROW
        ) {
            $values[] = str_replace(["\\\\", "\\'"], ['\\', "'"], substr($lexeme[1], 1, -1));
        }
    }

    return $values;
}

/**
 * The narrow-parity corpus: StringTypeNarrowerTest's fixtures, the byte-level traps, and a deterministic fuzz over
 * the characters the numeric, temporal and time zone rungs care about.
 *
 * @return list<string>
 */
function csv_narrow_corpus(): array
{
    $traps = [
        "\x0C5",
        "5\x0C",
        "\x0B5",
        "5\0",
        "\0",
        '  5  ',
        '',
        'null',
        'NULL',
        'Null',
        'NiL',
        "\u{FF2E}\u{FF35}\u{FF2C}\u{FF2C}",
        "N\u{130}L",
        '9223372036854775807',
        '9223372036854775808',
        '99999999999999999999',
        '-9223372036854775808',
        '-9223372036854775809',
        '1e5',
        '1E5',
        '.5',
        '5.',
        '+.5',
        '-.5e-3',
        '-0',
        '+5',
        '0123',
        '0x1A',
        'INF',
        'NAN',
        '1_000',
        '1e',
        'e5',
        ' 1',
        '1 ',
        "\t1",
        '{',
        '}',
        '{}',
        '[]',
        '[1,',
        '{"a":1}',
        '[{"a":[1,2]}]',
        '{"a":}',
        'F47AC10B-58CC-4372-A567-0E02B2C3D479',
        '{f47ac10b-58cc-4372-a567-0e02b2c3d479}',
        'f47ac10b58cc4372a5670e02b2c3d479',
        'True',
        'FALSE',
        'tRuE',
        'yes',
        'Europe/Warsaw',
        'europe/warsaw',
        'UTC',
        'utc',
        'Z',
        '+02:00',
        '+99:59',
        '+99:60',
        '+00:99',
        '-99:99',
        '+2:00',
        '+02:00:00',
        '20240305',
        '02-Jun-2022',
        '2024-01',
        '2024-02-30',
        '2024-02-29',
        '2023-02-29',
        '31/12/2024',
        '2024-01-01T00:00:00Z',
        '2024-01-01 00:00:00.5',
        'tomorrow noon',
        '2024-01-01 +1 day',
        '10:00',
        'Mon, 15 Aug 2005 15:52:01 +0000',
        '1/2/3',
        'a1b2c3',
        'abc 12 34',
        '2024 Jan 05',
        '05 January 2024',
        'Jan 2024',
        '0000-00-00',
        '9999-12-31 23:59:59',
        '1970-01-01 00:00:00',
        '32767-01-01',
        '32768-01-01',
        '2024-13-01',
        '2024-00-10',
        '12345678',
        '00000000',
        '2024-01-01 25:00',
        '1969-12-31T23:59:59.5Z',
        '2024-02-29T12:00:00+14:00',
        '2024-02-29T12:00:00-12:00',
        '2026-01-31T00:00:00+23:59',
        '9999-12-31T23:59:59.999999-23:59',
        '0001-01-01T00:00:00+23:59',
        '2038-01-19T03:14:08Z',
        '2026-01-02T03:04:05-00:00',
        '2026-01-02t03:04:05Z',
        '2026-01-02T03:04:05z',
        '2026-01-02T03:04:05,5Z',
        '2026-01-02T03:04:05+0100',
        '2026-01-02T03:04:05.Z',
        '2023-02-29T00:00:00Z',
        '0000-01-01T00:00:00Z',
        '2026-01-02T03:04:05+24:00',
        '2026-01-02T03:04:05+01:60',
        '2026-01-02T03:04:05.1234567+01:00',
    ];

    mt_srand(46);
    $alphabet = ['0', '1', '2', '9', '-', ':', '.', ' ', 'T', 'e', 'E', '+', '/', 'a', 'J', 'u', 'n', 'Z', ','];
    $fuzz = [];

    for ($i = 0; $i < 200; $i++) {
        $value = '';

        for ($j = mt_rand(1, 20); $j > 0; $j--) {
            $value .= $alphabet[mt_rand(0, count($alphabet) - 1)];
        }

        $fuzz[] = $value;
    }

    return array_values(array_merge(string_type_narrower_fixture_values(), $traps, $fuzz));
}

/**
 * RustCSVOpenSource::sniff() of one CSV column "v" (no names seeded), one row per value - a null value is an empty cell,
 * read as null with $emptyToNull.
 *
 * @param list<?string> $values
 */
function rust_sniff_column(
    array $values,
    Flow\Types\Type\TypeNarrower $typer,
    bool $emptyToNull = true,
): Flow\ETL\Schema\Inference\ColumnTypes {
    $csv = "v\n";

    foreach ($values as $value) {
        $csv .= ($value === null ? '' : '"' . str_replace('"', '""', $value) . '"') . "\n";
    }

    return (new Flow\ETL\Adapter\CSV\RustCSVOpenSource(
        new Flow\Filesystem\Stream\MemorySourceStream($csv),
        ',',
        '"',
        '',
        true,
        $emptyToNull,
        false,
    ))->sniff([], -1, new Flow\ETL\Schema\Inference\SchemaInference(), $typer);
}

/**
 * Prints every value whose one-row RustCSVOpenSource::sniff() differs from the PHP ColumnTypes::observe() fold of it -
 * StringTypeNarrower::narrow() of the value.
 *
 * @param list<Flow\Types\Type<mixed>> $candidates
 * @param list<string> $corpus
 */
function assert_narrow_parity(string $label, array $candidates, array $corpus): void
{
    $typer = new Flow\Types\Type\Native\String\StringTypeNarrower($candidates);
    $mismatches = 0;

    foreach ($corpus as $value) {
        $php = new Flow\ETL\Schema\Inference\ColumnTypes([], $typer);
        $php->observe(['v' => $value]);

        if ($php != rust_sniff_column([$value], $typer, false)) {
            $mismatches++;
            echo '  ', bin2hex($value), ' ', var_export($value, true), "\n";
        }
    }

    echo $label, ': ', $mismatches === 0 ? 'identical' : "{$mismatches} mismatches", "\n";
}

/**
 * JSON-shaped cells where a native JSON check could disagree with json_validate(): nesting depth, surrogate escapes,
 * UTF-8, control bytes, number grammar, whitespace and trailing input.
 *
 * @return array<string, string>
 */
function json_parity_cases(): array
{
    $nested = /** @param non-negative-int $depth */ static fn(int $depth): string => (
        str_repeat('[', $depth) . str_repeat(']', $depth)
    );
    $backslash = chr(92);

    return [
        'depth128' => $nested(128),
        'depth129' => $nested(129),
        // serde_json accepts the next nine and json_validate() rejects them; only the pre-filters make the two agree
        'depth512' => $nested(512),
        'depth513' => $nested(513),
        'depth100k' => $nested(100000),
        'obj_depth512' => str_repeat('{"a":', 511) . '{}' . str_repeat('}', 511),
        'obj_depth513' => str_repeat('{"a":', 512) . '{}' . str_repeat('}', 512),
        'lone_hi' => '["\ud800"]',
        'lone_lo' => '["\udc00"]',
        'hi_hi' => '["\ud800\ud800"]',
        'lone_hi_key' => '{"\ud800":1}',
        // a valid escaped pair also trips the surrogate pre-filter - PHP must still yield the same Json
        'pair_escaped' => '["' . $backslash . 'ud83d' . $backslash . 'ude00"]',
        'pair_escaped_upper' => '["' . $backslash . 'uD83D' . $backslash . 'uDE00"]',
        'pair_raw_utf8' => '["😀"]',
        'bad_utf8' => "[\"\xff\"]",
        'overlong' => "[\"\xc0\xaf\"]",
        'utf8_surrogate_cesu' => "[\"\xed\xa0\x80\"]",
        'bad_utf8_outside' => "[1]\xff]",
        'nul_in_str' => "[\"a\x00b\"]",
        'nul_outside' => "[1\x00]",
        'ctrl_1f' => "[\"\x1f\"]",
        'del' => "[\"\x7f\"]",
        'huge_exp' => '[1e999999999999]',
        'neg_exp' => '[1e-999999]',
        'bigint' => '[' . str_repeat('9', 5000) . ']',
        'neg_zero' => '[-0]',
        'leading_zero' => '[01]',
        'plus' => '[+1]',
        'dot' => '[1.]',
        'dot_lead' => '[.1]',
        'exp_empty' => '[1e]',
        'hex' => '[0x1]',
        'trailing_comma_arr' => '[1,]',
        'trailing_comma_obj' => '{"a":1,}',
        'two_values' => '[][]',
        'garbage' => '[1] x ]',
        'dup_keys' => '{"a":1,"a":2}',
        'ws_inside' => "[ \t\n\r1 ]",
        'vtab' => "[\x0b1]",
        'nbsp' => "[\xc2\xa01]",
        'single_quote' => "['a']",
        'nan' => '[NaN]',
        'inf' => '[Infinity]',
        'true' => '[true,false,null]',
        'True' => '[True]',
        'escape_bad' => '["\x"]',
        'escape_slash' => '["\/"]',
        'u_short' => '["\u12"]',
        'u_upper' => '["é"]',
        'u0000' => '["\u0000"]',
        'key_nonstr' => '{1:2}',
        'empty_obj' => '{}',
        'empty_arr' => '[]',
        'nested_mix' => '{"a":[{"b":{}}]}',
    ];
}

/**
 * JSON-shaped cells for the leak checks: valid ones are accepted natively, while a native reject, the deep one and
 * both surrogate escapes go to PHP.
 *
 * @return list<string>
 */
function json_leak_cells(): array
{
    return [
        '{"a":1}',
        '[1,2]',
        '{"a":[{"b":{}}]}',
        '[]',
        str_repeat('[', 600) . str_repeat(']', 600),
        '["\ud800"]',
        '["\ud83d\ude00"]',
        '[1,]',
        "[\"\xff\"]",
        '{"a":}',
    ];
}

/**
 * A random value of the text writers' fuzz alphabet: separators, enclosures, escapes, line breaks, slashes, controls,
 * multi-byte characters; `mt_srand()` first.
 */
function write_random_string(): string
{
    static $alphabet = [
        'a',
        'b',
        'Z',
        '0',
        '7',
        ',',
        ';',
        '|',
        '.',
        '-',
        '"',
        "'",
        '\\',
        "\n",
        "\r",
        "\t",
        ' ',
        '/',
        '<',
        '&',
        "\x01",
        "\x7f",
        'ż',
        '😀',
        "\u{2028}",
        '',
    ];
    $text = '';

    for ($i = mt_rand(0, 6); $i > 0; $i--) {
        $text .= $alphabet[mt_rand(0, count($alphabet) - 1)];
    }

    return $text;
}

/**
 * A random type of nesting depth <= $depth that a column can hold, and a generator of its values.
 *
 * @return array{Flow\Types\Type<mixed>, Closure(): mixed}
 */
function write_random_type(int $depth): array
{
    static $zones = ['UTC', 'Europe/Warsaw', 'America/New_York', '+02:30', '-05:00'];
    static $floats = [
        0.1 + 0.2,
        1.0,
        -0.0,
        1.0e25,
        1.0e-7,
        123456789012345.67,
        -2.5,
        1 / 3,
        PHP_FLOAT_MAX,
        PHP_FLOAT_MIN,
        5.0e-324,
    ];
    static $documents = [
        '{"a":1}',
        '[]',
        '{}',
        '{"a":{}}',
        '[1,{"0":"x"}]',
        '["s/t"]',
        '[5, 1.0]',
        '{"k":[1500.0,true,null]}',
        '{"a": "b c"}',
    ];
    static $markup = ['<a b="1"><c/></a>', '<root><a>1</a></root>', '<x>a &amp; b</x>'];
    static $keys = ['a', 'b c', 'k/1', 'ż', '"q"', 'x,y'];
    static $fields = ['a', 'b', 'c d', 'e"f', 'g/h'];

    $kind = mt_rand(0, $depth > 0 ? 15 : 11);

    [$type, $value] = match ($kind) {
        0 => [
            Flow\Types\DSL\type_integer(),
            static fn(): int => mt_rand(0, 3) === 0 ? mt_rand(-5, 5) : mt_rand() * (mt_rand(0, 1) ? 1 : -1),
        ],
        1 => [
            Flow\Types\DSL\type_float(),
            static fn(): float => mt_rand(0, 2) === 0
                ? $floats[mt_rand(0, count($floats) - 1)]
                : mt_rand(-100000, 100000) / 1000,
        ],
        2 => [Flow\Types\DSL\type_boolean(), static fn(): bool => mt_rand(0, 1) === 1],
        3 => [Flow\Types\DSL\type_string(), write_random_string(...)],
        4 => (static function () use ($zones): array {
            $zone = $zones[mt_rand(0, count($zones) - 1)];

            return [
                Flow\Types\DSL\type_datetime($zone),
                static fn(): DateTimeImmutable => (new DateTimeImmutable(
                    '@' . mt_rand(-3_000_000_000, 5_000_000_000),
                ))->modify('+' . mt_rand(0, 999_999) . ' usec'),
            ];
        })(),
        5 => [
            Flow\Types\DSL\type_date(),
            static fn(): DateTimeImmutable => new DateTimeImmutable('@' . (mt_rand(-50_000, 50_000) * 86_400)),
        ],
        6 => [
            Flow\Types\DSL\type_time(),
            static fn(): DateInterval => (new DateTimeImmutable('@0'))->diff(
                new DateTimeImmutable('@' . mt_rand(-200_000, 200_000)),
            ),
        ],
        7 => [
            Flow\Types\DSL\type_uuid(),
            static fn(): string => sprintf(
                '%08x-%04x-4%03x-%04x-%012x',
                mt_rand(0, 0xffffffff),
                mt_rand(0, 0xffff),
                mt_rand(0, 0xfff),
                mt_rand(0x8000, 0xbfff),
                mt_rand(0, 0xffffffffffff),
            ),
        ],
        8 => [Flow\Types\DSL\type_json(), static fn(): string => $documents[mt_rand(0, count($documents) - 1)]],
        9 => [Flow\Types\DSL\type_enum(BasicEnum::class), static fn(): BasicEnum => BasicEnum::cases()[mt_rand(0, 2)]],
        10 => [
            Flow\Types\DSL\type_time_zone(),
            static fn(): DateTimeZone => new DateTimeZone($zones[mt_rand(0, count($zones) - 1)]),
        ],
        11 => [Flow\Types\DSL\type_xml(), static fn(): string => $markup[mt_rand(0, count($markup) - 1)]],
        12 => (static function () use ($depth): array {
            [$element, $value] = write_random_type($depth - 1);

            return [
                type_list($element),
                static function () use ($value): array {
                    $list = [];

                    for ($i = mt_rand(0, 3); $i > 0; $i--) {
                        $list[] = $value();
                    }

                    return $list;
                },
            ];
        })(),
        13 => (static function () use ($depth, $keys): array {
            [$element, $value] = write_random_type($depth - 1);

            return [
                type_map(type_string(), $element),
                static function () use ($value, $keys): array {
                    $map = [];

                    for ($i = mt_rand(0, 3); $i > 0; $i--) {
                        $map[$keys[mt_rand(0, count($keys) - 1)]] = $value();
                    }

                    return $map;
                },
            ];
        })(),
        14 => (static function () use ($depth): array {
            [$element, $value] = write_random_type($depth - 1);

            return [
                type_map(type_integer(), $element),
                static function () use ($value): array {
                    $map = [];

                    for ($i = mt_rand(0, 3); $i > 0; $i--) {
                        $map[mt_rand(-3, 40)] = $value();
                    }

                    return $map;
                },
            ];
        })(),
        15 => (static function () use ($depth, $fields): array {
            $elements = [];
            $values = [];

            foreach (array_rand(array_flip($fields), mt_rand(2, 3)) as $name) {
                [$element, $value] = write_random_type($depth - 1);
                $optional = mt_rand(0, 3) === 0;
                // an optional element is present or absent, never an explicit null
                $elements[$name] = structure_element(
                    $name,
                    $optional && $element instanceof Flow\Types\Type\Logical\OptionalType ? $element->base() : $element,
                    $optional,
                );
                $values[$name] = [
                    $optional,
                    $optional && $element instanceof Flow\Types\Type\Logical\OptionalType
                        ? static function () use ($value): mixed {
                            do {
                                // @mago-ignore analysis:mixed-assignment
                                $generated = $value();
                            } while ($generated === null);

                            return $generated;
                        } : $value,
                ];
            }

            return [
                type_structure($elements),
                static function () use ($values): array {
                    $structure = [];

                    foreach ($values as $name => [$optional, $value]) {
                        if (!$optional || mt_rand(0, 1) === 1) {
                            $structure[$name] = $value();
                        }
                    }

                    return $structure;
                },
            ];
        })(),
        default => throw new LogicException('no such kind'),
    };

    if (mt_rand(0, 3) > 0) {
        return [$type, $value];
    }

    return [type_optional($type), static fn(): mixed => mt_rand(0, 5) === 0 ? null : $value()];
}

/**
 * A value of `$type` (from write_random_type()) in a random input form: as generated, in a raw form a reader hands over
 * (numeric and ISO strings, Uuid and Json objects, a datetime in another zone), in a form the cast refuses (a wrong
 * scalar, a scalar for a container, non-list keys, integer keys for string keys, a missing required element, a present
 * null, an extra key), or null; containers recurse into their elements. `mt_srand()` first.
 *
 * @param Flow\Types\Type<mixed> $type
 */
function cast_random_input(Flow\Types\Type $type, mixed $value): mixed
{
    static $zones = ['UTC', 'Europe/Warsaw', 'America/New_York', '+02:30', '-05:00'];

    $roll = mt_rand(0, 19);

    if ($value === null || $roll === 0) {
        return null;
    }

    $type = $type instanceof Flow\Types\Type\Logical\OptionalType ? $type->base() : $type;

    if ($roll === 1) {
        return $type instanceof Flow\Types\Type\Logical\ListType
        || $type instanceof Flow\Types\Type\Logical\MapType
        || $type instanceof Flow\Types\Type\Logical\StructureType
            ? [5, 'x', true][mt_rand(0, 2)]
            : ['not a value', [1], -1.5][mt_rand(0, 2)];
    }

    $raw = $roll < 10;

    return match (true) {
        $type instanceof Flow\Types\Type\Logical\ListType => (static function () use ($type, $value, $roll): array {
            $list = array_map(static fn(mixed $item): mixed => cast_random_input(
                $type->element(),
                $item,
            ), Flow\Types\DSL\type_list(Flow\Types\DSL\type_mixed())->assert($value));

            return $roll === 2 && $list !== [] ? array_combine(range(1, count($list)), $list) : $list;
        })(),
        $type instanceof Flow\Types\Type\Logical\MapType => (static function () use ($type, $value, $roll): array {
            $map = array_map(static fn(mixed $item): mixed => cast_random_input(
                $type->value(),
                $item,
            ), Flow\Types\DSL\type_array()->assert($value));

            if ($roll === 2 && $map !== []) {
                $map[mt_rand(100, 200)] = reset($map);
            }

            return $map;
        })(),
        $type instanceof Flow\Types\Type\Logical\StructureType => (static function () use (
            $type,
            $value,
            $roll,
        ): array {
            $structure = [];

            // @mago-ignore analysis:mixed-assignment
            foreach (Flow\Types\DSL\type_array()->assert($value) as $name => $item) {
                $structure[$name] = cast_random_input(
                    $type->element($name)->type ?? throw new LogicException('no such element'),
                    $item,
                );
            }

            $names = array_keys($structure);
            $name = $names === [] ? 'a' : $names[mt_rand(0, count($names) - 1)];

            return match ($roll) {
                2 => array_diff_key($structure, [$name => true]),
                3 => array_replace($structure, [$name => null]),
                4 => $structure + ['extra key' => 1],
                default => $structure,
            };
        })(),
        !$raw => $value,
        $type instanceof Flow\Types\Type\Native\IntegerType,
        $type instanceof Flow\Types\Type\Native\FloatType,
            => Flow\Types\DSL\type_string()->cast($value),
        $type instanceof Flow\Types\Type\Native\BooleanType => ['true', 'false', '1', '0', 'yes', 'off'][mt_rand(0, 5)],
        $type instanceof Flow\Types\Type\Logical\DateTimeType,
        $type instanceof Flow\Types\Type\Logical\DateType,
            => mt_rand(0, 1) === 0
            ? Flow\Types\DSL\type_instance_of(DateTimeImmutable::class)
                ->assert($value)
                ->format($type instanceof Flow\Types\Type\Logical\DateType ? 'Y-m-d' : 'Y-m-d\\TH:i:s.uP')
            : Flow\Types\DSL\type_instance_of(DateTimeImmutable::class)
                ->assert($value)
                ->setTimezone(new DateTimeZone($zones[mt_rand(0, count($zones) - 1)])),
        $type instanceof Flow\Types\Type\Logical\UuidType
            => new Flow\Types\Value\Uuid(Flow\Types\DSL\type_string()->assert($value)),
        $type instanceof Flow\Types\Type\Logical\JsonType
            => new Flow\Types\Value\Json(Flow\Types\DSL\type_string()->assert($value)),
        default => $value,
    };
}

/**
 * A random batch for the text writers: 1-6 columns of random types (nesting <= 3) under plain and awkward names, 0-40
 * rows.
 *
 * @return array{Schema, list<array<array-key, mixed>>}
 */
function write_random_batch(): array
{
    static $names = ['id', 'first name', 'a"b', 'ż', 'x/y', 'name', 'value', 'at', '0', '7', 'a,b'];
    $definitions = [];
    $values = [];

    foreach ((array) array_rand(array_flip($names), mt_rand(1, 6)) as $name) {
        [$type, $value] = write_random_type(3);
        $definitions[] = Flow\ETL\DSL\definition_from_type((string) $name, $type);
        $values[(string) $name] = $value;
    }

    $rows = [];

    for ($i = [0, 1, mt_rand(2, 40), mt_rand(2, 40)][mt_rand(0, 3)]; $i > 0; $i--) {
        $row = [];

        foreach ($values as $name => $value) {
            $row[$name] = $value();
        }

        $rows[] = $row;
    }

    return [schema(...$definitions), $rows];
}

/**
 * The bytes a text sink wrote, or the class and message of what it threw.
 *
 * @param callable(Flow\Filesystem\Stream\StringDestinationStream): void $write
 */
function written(callable $write): string
{
    $stream = new Flow\Filesystem\Stream\StringDestinationStream(Flow\Filesystem\DSL\path('memory://out'));

    try {
        $write($stream);
    } catch (Throwable $e) {
        return $e::class . ': ' . $e->getMessage();
    }

    return $stream->content();
}

/**
 * A stream over `$raw` whose read() returns at most `$chunk` bytes - a reader fed in pieces of that size.
 *
 * @param positive-int $chunk
 */
function chunked_source_stream(string $raw, int $chunk): Flow\Filesystem\SourceStream
{
    return new class(
        new Flow\Filesystem\Stream\StringSourceStream(Flow\Filesystem\DSL\path('memory://phpt.json'), $raw),
        $chunk,
    ) implements Flow\Filesystem\SourceStream {
        /**
         * @param positive-int $chunk
         */
        public function __construct(
            private readonly Flow\Filesystem\Stream\StringSourceStream $stream,
            private readonly int $chunk,
        ) {}

        public function close(): void
        {
            $this->stream->close();
        }

        public function content(): string
        {
            return $this->stream->content();
        }

        public function isOpen(): bool
        {
            return $this->stream->isOpen();
        }

        /**
         * @param positive-int $length
         */
        public function iterate(int $length = 1): Generator
        {
            return $this->stream->iterate(min($length, $this->chunk));
        }

        public function path(): Flow\Filesystem\Path
        {
            return $this->stream->path();
        }

        /**
         * @param positive-int $length
         */
        public function read(int $length, int $offset): string
        {
            return $this->stream->read(min($length, $this->chunk), $offset);
        }

        public function readLines(string $separator = "\n", ?int $length = null): Generator
        {
            return $this->stream->readLines($separator, $length);
        }

        public function size(): ?int
        {
            return $this->stream->size();
        }
    };
}

/**
 * Stands in for the PhpCSVEncoder a RustCSVEncoder holds: renders through it and records every column left to it.
 */
final class RecordingPhpCSVEncoder
{
    /**
     * @var list<Flow\ETL\Column\Column>
     */
    public array $columns = [];

    public function __construct(
        private readonly Flow\ETL\Adapter\CSV\PhpCSVEncoder $php,
    ) {}

    /**
     * @return list<?string>
     */
    public function cells(Flow\Types\Type $type, Flow\ETL\Column\Column $column): array
    {
        $this->columns[] = $column;

        return $this->php->cells($type, $column);
    }

    /**
     * @param list<string> $headers
     */
    public function encodeHeader(array $headers): string
    {
        return $this->php->encodeHeader($headers);
    }
}

/**
 * Stands in for the PhpJsonEncoder a RustJsonEncoder holds: renders through it and records every column left to it.
 */
final class RecordingPhpJsonEncoder
{
    /**
     * @var list<Flow\ETL\Column\Column>
     */
    public array $columns = [];

    public function __construct(
        private readonly Flow\ETL\Adapter\JSON\PhpJsonEncoder $php,
    ) {}

    /**
     * @return list<string>
     */
    public function fragments(Flow\Types\Type $type, Flow\ETL\Column\Column $column): array
    {
        $this->columns[] = $column;

        return $this->php->fragments($type, $column);
    }
}

/**
 * The names of the columns of `$rows` among `$recorded`.
 *
 * @param list<Flow\ETL\Column\Column> $recorded
 *
 * @return list<string>
 */
function recorded_names(array $recorded, Rows $rows): array
{
    return array_map(
        'strval',
        array_keys(array_filter($rows->columns(), static fn(Flow\ETL\Column\Column $column): bool => in_array(
            $column,
            $recorded,
            true,
        ))),
    );
}
