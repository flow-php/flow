<?php

declare(strict_types=1);

require __DIR__ . '/../../../../../vendor/autoload.php';

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\Instantiators;
use Flow\ETL\Rows;
use Flow\Floe\FloeWriter;
use Flow\Floe\Format;
use Flow\Floe\RowEncoder;
use Flow\Floe\RowHydrator;
use Flow\Floe\RowsDecoder;
use Flow\Floe\SchemaDecoder;
use Flow\Floe\SchemaTracker;
use Flow\Floe\ValueDecoder;

/**
 * Pure-PHP reference framing built from the Floe encoder primitives: turns Rows
 * into the ordered list of bare SCHEMA/ROW frame bodies the extension decodes.
 * A new SCHEMA frame is emitted whenever the row no longer matches the plan.
 *
 * @return array<int, array{type: int, body: string}>
 */
function php_frames(Rows $rows): array
{
    $rowEncoder = new RowEncoder();
    $tracker = new SchemaTracker();
    $plan = null;
    $frames = [];

    foreach ($rows->all() as $row) {
        if ($plan === null || !$tracker->fits($plan, $row)) {
            $plan = FloeWriter::growSectionPlan(null, $row);
            $frames[] = ['type' => Format::FRAME_SCHEMA, 'body' => $plan->schemaBody];
        }

        $frames[] = ['type' => Format::FRAME_ROW, 'body' => $rowEncoder->encode($plan, $row)];
    }

    return $frames;
}

/**
 * Pure-PHP reference decode of frame bodies via the Floe schema decoder and the
 * row hydrator - the byte-for-byte oracle the extension is compared against.
 *
 * @param array<int, array{type: int, body: string}> $frames
 *
 * @return array<int, Row>
 */
function php_decode_frames(array $frames): array
{
    $schemaDecoder = new SchemaDecoder(new ValueDecoder(), new Instantiators());
    $rowHydrator = new RowHydrator();
    $plan = null;
    $rows = [];

    foreach ($frames as $frame) {
        if ($frame['type'] === Format::FRAME_SCHEMA) {
            $plan = $schemaDecoder->decode($frame['body']);
        } elseif ($plan === null) {
            throw new RuntimeException('row frame before any schema frame');
        } else {
            $position = 0;
            $rows[] = $rowHydrator->hydrate($plan, $frame['body'], $position);
        }
    }

    return $rows;
}

/**
 * Drives a RowsDecoder over frame bodies, priming schemas and decoding rows in
 * order (mirrors what FloeReader does on the hot path).
 *
 * @param array<int, array{type: int, body: string}> $frames
 *
 * @return array<int, Row>
 */
function decoder_decode_frames(RowsDecoder $decoder, array $frames): array
{
    $rows = [];

    foreach ($frames as $frame) {
        if ($frame['type'] === Format::FRAME_SCHEMA) {
            $decoder->schema($frame['body']);
        } else {
            $rows[] = $decoder->row($frame['body']);
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
