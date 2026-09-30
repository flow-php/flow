<?php

declare(strict_types=1);

require __DIR__ . '/../../../../../vendor/autoload.php';

use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
use Flow\ETL\Schema;
use Flow\ETL\Tests\Fixtures\Enum\BasicEnum;

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
            $rows[$index] = $row + ['html' => '<p>html</p>'];
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
    return (new RowsBuilder($schema, new DefaultBackend()))
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
 * JSON of the three column interfaces as reflection sees them: methods, parameters, types, variadics.
 */
function interfaces_reflection(): string
{
    $interfaces = [];

    foreach (['Backend', 'Column', 'ColumnBuilder'] as $name) {
        $class = new ReflectionClass('Flow\\ETL\\Column\\' . $name);
        $interfaces[$name] = [
            $class->isInterface(),
            array_map(static fn(ReflectionMethod $method): array => [
                $method->getName(),
                (string) $method->getReturnType(),
                array_map(static fn(ReflectionParameter $parameter): array => [
                    $parameter->getName(),
                    (string) $parameter->getType(),
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
    } catch (Flow\Floe\Exception\ExtensionException $e) {
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
 * RustCSVReaderNative fed `$raw` in `$chunk`-byte pieces, as `[headers, list of array<array-key, mixed>::$values]`.
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
    $reader = new Flow\ETL\Adapter\CSV\RustCSVReaderNative(
        $separator,
        $enclosure,
        $escape,
        $withHeader,
        $emptyToNull,
        $removeBOM,
    );
    $rows = [];
    $drain = static function () use ($reader, &$rows): void {
        while (($batch = $reader->next(3)) !== []) {
            foreach ($batch as $values) {
                $rows[] = $values;
            }
        }
    };

    foreach ($raw === '' ? [] : str_split($raw, $chunk) as $piece) {
        $reader->feed($piece);
        $drain();
    }

    $reader->finish();
    $drain();

    return [$reader->headers(), $rows];
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
 * Prints every value where RustColumnFoldNative::narrowOne() and StringTypeNarrower::narrow() disagree.
 *
 * @param list<Flow\Types\Type<mixed>> $candidates
 * @param list<string> $corpus
 */
function assert_narrow_parity(string $label, array $candidates, array $corpus): void
{
    $php = new Flow\Types\Type\Native\String\StringTypeNarrower($candidates);
    $native = new Flow\ETL\Adapter\CSV\RustColumnFoldNative([], array_map(
        static fn(Flow\Types\Type $type): string => $type->toString(),
        $candidates,
    ));
    $mismatches = 0;

    foreach ($corpus as $value) {
        $expected = $php->narrow($value)->toString();
        $actual = $native->narrowOne($value);

        if ($expected !== $actual) {
            $mismatches++;
            echo '  ', bin2hex($value), ' ', var_export($value, true), ": php={$expected} native={$actual}\n";
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
