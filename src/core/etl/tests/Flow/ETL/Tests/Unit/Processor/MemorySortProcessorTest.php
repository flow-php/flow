<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;

final class MemorySortProcessorTest extends FlowTestCase
{
    public function test_handles_empty_input(): void
    {
        $processor = new MemorySortProcessor(refs(ref('id')));

        $generator = (static function () {
            yield from [];
        })();

        static::assertCount(0, iterator_to_array($processor->process($generator, flow_context())));
    }

    public function test_sorts_across_multiple_batches(): void
    {
        $processor = new MemorySortProcessor(refs(ref('id')));

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 3]));
            yield rows(schema(int_schema('id')), row(['id' => 1]));
            yield rows(schema(int_schema('id')), row(['id' => 2]));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals([['id' => 1], ['id' => 2], ['id' => 3]], $allRows);
    }

    public function test_sorts_rows_ascending(): void
    {
        $processor = new MemorySortProcessor(refs(ref('id')));

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 3]), row(['id' => 1]), row(['id' => 2]));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals([['id' => 1], ['id' => 2], ['id' => 3]], $allRows);
    }

    public function test_sorts_rows_descending(): void
    {
        $processor = new MemorySortProcessor(refs(ref('id')->desc()));

        $generator = (static function () {
            yield rows(schema(int_schema('id')), row(['id' => 1]), row(['id' => 3]), row(['id' => 2]));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals([['id' => 3], ['id' => 2], ['id' => 1]], $allRows);
    }
}
