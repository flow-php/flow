<?php

declare(strict_types=1);

require __DIR__ . '/../../../../../vendor/autoload.php';

use Flow\ETL\Row;
use Flow\ETL\Row\NativeRowHydrator;
use Flow\ETL\Row\PhpRowHydrator;
use Flow\ETL\Rows;
use Flow\Floe\Format;
use Flow\Floe\NativeFloeEncoder;
use Flow\Floe\PhpFloeEncoder;

/**
 * In-memory marker for the schema-carrying entry in the reference frame lists
 * below. The Floe format stores the schema in the footer, not in a frame; this
 * scaffold threads the schema through the frame list to drive the encoders.
 */
const SCHEMA_ENTRY = 0x01;

/**
 * Pure-PHP reference framing built from the Floe encoder primitives: a write
 * session carries one schema, so the whole Rows is framed as a single schema
 * entry (its union) followed by one ROW frame per row.
 *
 * @return array<int, array{type: int, body: string}>
 */
function php_frames(Rows $rows): array
{
    if ($rows->count() === 0) {
        return [];
    }

    $hydrator = new PhpRowHydrator();
    $schemaBody = json_encode($rows->schema()->normalize(), JSON_THROW_ON_ERROR);
    $encoder = new PhpFloeEncoder(Flow\ETL\DSL\schema_from_json($schemaBody));

    $frames = [['type' => SCHEMA_ENTRY, 'body' => $schemaBody]];

    foreach ($rows->all() as $row) {
        $frames[] = [
            'type' => Format::FRAME_ROW,
            'body' => $encoder->encode($hydrator->dehydrate(new Rows($rows->schema(), $row)))[0],
        ];
    }

    return $frames;
}

/**
 * Native counterpart of php_frames(): identical single-schema framing (PHP owns
 * it), ROW bodies produced by NativeFloeEncoder. Compared against php_frames()
 * to prove the native encode is byte-identical to the pure-PHP encode.
 *
 * @return array<int, array{type: int, body: string}>
 */
function ext_frames(Rows $rows): array
{
    if ($rows->count() === 0) {
        return [];
    }

    $hydrator = new NativeRowHydrator();
    $schemaBody = json_encode($rows->schema()->normalize(), JSON_THROW_ON_ERROR);
    $encoder = new NativeFloeEncoder(Flow\ETL\DSL\schema_from_json($schemaBody));

    $frames = [['type' => SCHEMA_ENTRY, 'body' => $schemaBody]];

    foreach ($rows->all() as $row) {
        $frames[] = [
            'type' => Format::FRAME_ROW,
            'body' => $encoder->encode($hydrator->dehydrate(new Rows($rows->schema(), $row)))[0],
        ];
    }

    return $frames;
}

/**
 * Pure-PHP reference decode of frame bodies via the Floe encoder and the row
 * hydrator - the byte-for-byte oracle the extension is compared against.
 *
 * @param array<int, array{type: int, body: string}> $frames
 *
 * @return array<int, Row>
 */
function php_decode_frames(array $frames): array
{
    $hydrator = new PhpRowHydrator();
    $encoder = null;
    $schema = null;
    $rows = [];

    foreach ($frames as $frame) {
        if ($frame['type'] === SCHEMA_ENTRY) {
            $schema = Flow\ETL\DSL\schema_from_json($frame['body']);
            $encoder = new PhpFloeEncoder($schema);
        } elseif ($schema === null || $encoder === null) {
            throw new RuntimeException('row frame before any schema entry');
        } else {
            foreach ($hydrator->hydrate($encoder->decode([$frame['body']]), $schema)->all() as $row) {
                $rows[] = $row;
            }
        }
    }

    return $rows;
}

/**
 * Drives the native two-layer pipeline over frame bodies, rebinding the schema
 * per schema entry (mirrors how FloeStreamReader sources the schema).
 *
 * @param array<int, array{type: int, body: string}> $frames
 *
 * @return array<int, Row>
 */
function ext_decode_frames(array $frames): array
{
    $hydrator = new NativeRowHydrator();
    $encoder = null;
    $schema = null;
    $rows = [];

    foreach ($frames as $frame) {
        if ($frame['type'] === SCHEMA_ENTRY) {
            $schema = Flow\ETL\DSL\schema_from_json($frame['body']);
            $encoder = new NativeFloeEncoder($schema);
        } elseif ($schema === null || $encoder === null) {
            throw new RuntimeException('row frame before any schema entry');
        } else {
            foreach ($hydrator->hydrate($encoder->decode([$frame['body']]), $schema)->all() as $row) {
                $rows[] = $row;
            }
        }
    }

    return $rows;
}

/**
 * Byte-identical comparison against the pure-PHP decoder on fresh objects.
 * Row::isEqual is intentionally avoided before serialize() - it memoizes
 * schema state asymmetrically and breaks byte-level comparison.
 *
 * @param array<int, Row> $expected
 * @param array<int, Row> $actual
 */
function assert_rows_identical(array $expected, array $actual): void
{
    if (count($expected) !== count($actual)) {
        echo 'FAIL: row count ', count($expected), ' !== ', count($actual), "\n";

        return;
    }

    foreach ($expected as $i => $row) {
        if (serialize($row) !== serialize($actual[$i])) {
            echo "FAIL: row {$i} differs from the PHP decoder output\n";

            return;
        }
    }

    echo "identical\n";
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
 * The PHP CSV path - CSVLineReader + CSVEncoder::decode() - as `[headers, list of RawRowValues::$values]`.
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
    $encoder = new Flow\ETL\Adapter\CSV\CSVEncoder(
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
        foreach ($encoder->decode([$line]) as $values) {
            $rows[] = $values->values;
        }
    }

    return [$encoder->headers() ?? [], $rows];
}

/**
 * RustCSVReaderNative fed `$raw` in `$chunk`-byte pieces, as `[headers, list of RawRowValues::$values]`.
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
                $rows[] = $values->values;
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
