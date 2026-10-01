<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\Tests\Integration;

use Flow\ETL\Adapter\JSON\JSONMachine\JsonFormat;
use Flow\ETL\Adapter\JSON\PhpJsonOpenSource;
use Flow\ETL\Adapter\JSON\Tests\Context\JsonFixtureContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;

final class PhpJsonOpenSourceTest extends FlowTestCase
{
    public function test_batches_are_sized_and_the_tail_is_shorter(): void
    {
        $open = new PhpJsonOpenSource(JsonFixtureContext::reader(), JsonFixtureContext::source('five_rows.json'));

        $batches = iterator_to_array($open->batches(schema(int_schema('id')), 2, new PhpBackend()), false);

        static::assertSame([2, 2, 1], array_map(static fn(Rows $rows): int => $rows->count(), $batches));
        static::assertSame([['id' => 5]], $batches[2]->toArray());
    }

    public function test_a_refusal_carries_the_row_of_its_batch(): void
    {
        $open = new PhpJsonOpenSource(
            JsonFixtureContext::reader(JsonFormat::Document),
            JsonFixtureContext::source('misfit.json'),
        );
        $batches = $open->batches(schema(int_schema('id')), 2, new PhpBackend());

        static::assertSame([['id' => 1], ['id' => 2]], $batches->current()->toArray());

        try {
            $batches->next();
            static::fail('the second batch holds "n/a"');
        } catch (SchemaMismatchException $e) {
            static::assertSame(1, $e->rowIndex);
        }
    }
}
