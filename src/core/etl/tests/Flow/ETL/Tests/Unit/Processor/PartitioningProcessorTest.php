<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Cache\Implementation\InMemoryCache;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Processor\PartitioningProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class PartitioningProcessorTest extends FlowTestCase
{
    public function test_handles_empty_input(): void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->build());

        $processor = new PartitioningProcessor([ref('category')]);

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, $context));

        static::assertCount(0, $result);
    }

    public function test_partitions_rows_by_column(): void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->build());

        $processor = new PartitioningProcessor([ref('category')]);

        $generator = (static function () {
            yield rows(
                row(str_entry('category', 'a'), int_entry('id', 1)),
                row(str_entry('category', 'a'), int_entry('id', 2)),
                row(str_entry('category', 'b'), int_entry('id', 3)),
            );
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, $context));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(3, $allRows);
    }

    public function test_partitions_with_order_by(): void
    {
        $cache = new InMemoryCache();
        $context = flow_context(config_builder()->cache($cache)->build());

        $processor = new PartitioningProcessor([ref('category')], [ref('id')->desc()]);

        $generator = (static function () {
            yield rows(
                row(str_entry('category', 'a'), int_entry('id', 1)),
                row(str_entry('category', 'a'), int_entry('id', 2)),
            );
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, $context));
        /** @var list<array<array-key, mixed>> $allRows */
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals(2, $allRows[0]['id']);
        static::assertEquals(1, $allRows[1]['id']);
    }

    public function test_throws_exception_without_partition_columns(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('PartitioningProcessor requires at least one partitionBy entry');

        new PartitioningProcessor([]);
    }
}
