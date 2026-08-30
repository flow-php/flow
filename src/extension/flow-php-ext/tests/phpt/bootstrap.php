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
