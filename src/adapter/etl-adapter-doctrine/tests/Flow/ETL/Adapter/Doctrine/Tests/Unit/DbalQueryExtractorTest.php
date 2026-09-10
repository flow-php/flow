<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\Logging\Middleware;
use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\Adapter\Doctrine\ParametersSet;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InMemorySqlite;
use Flow\ETL\Adapter\Doctrine\Tests\Context\SelectQueryCounter;
use Flow\ETL\Tests\FlowTestCase;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_string;

final class DbalQueryExtractorTest extends FlowTestCase
{
    public function test_a_parameter_set_still_describes(): void
    {
        $extractor = (new DbalQueryExtractor(
            InMemorySqlite::withUsers(InMemorySqlite::connection(), 3),
            'SELECT id, name FROM users WHERE id > :min',
        ))->withParameters(new ParametersSet(['min' => 1], ['min' => 2]));

        static::assertSame(['id', 'name'], $extractor->schema()->references()->names());

        $rows = 0;

        foreach ($extractor->extract(flow_context()) as $batch) {
            $rows += $batch->count();
        }

        static::assertSame(3, $rows);
    }

    public function test_schema_is_derived_when_it_was_not_declared(): void
    {
        static::assertEquals(
            schema(str_schema('id', true), str_schema('name', true), str_schema('amount', true)),
            (new DbalQueryExtractor(
                InMemorySqlite::withUsers(InMemorySqlite::connection(), 1),
                'SELECT * FROM users',
            ))->schema(),
        );
    }

    public function test_schema_is_the_declared_one(): void
    {
        $extractor = new DbalQueryExtractor(InMemorySqlite::connection(), 'SELECT * FROM users');

        static::assertEquals(schema(int_schema('id')), $extractor->withSchema(schema(int_schema('id')))->schema());
    }

    public function test_sqlite_describes_as_all_string(): void
    {
        $schema = (new DbalQueryExtractor(
            InMemorySqlite::withUsers(InMemorySqlite::connection(), 1),
            'SELECT id, name AS bb, amount * 2 AS calc FROM users',
        ))->schema();

        static::assertSame(['id', 'bb', 'calc'], $schema->references()->names());

        foreach ($schema->definitions() as $definition) {
            static::assertTrue($definition->isNullable());
            static::assertEquals(type_string(), $definition->type());
        }
    }

    public function test_the_derived_schema_is_memoised(): void
    {
        $extractor = new DbalQueryExtractor(
            InMemorySqlite::withUsers(InMemorySqlite::connection(), 1),
            'SELECT * FROM users',
        );

        static::assertSame($extractor->schema(), $extractor->schema());
    }

    #[TestWith([5, 5])]
    #[TestWith([100, 10])]
    public function test_pushed_limit_stops_querying_further_parameter_sets(int $batchSize, int $extractedRows): void
    {
        $counter = new SelectQueryCounter();
        $extractor = (new DbalQueryExtractor(
            InMemorySqlite::withUsers(InMemorySqlite::connection(new Middleware($counter)), 30),
            'SELECT id, name FROM users WHERE id > :min AND id <= :max',
        ))
            ->withParameters(
                new ParametersSet(['min' => 0, 'max' => 10], ['min' => 10, 'max' => 20], ['min' => 20, 'max' => 30]),
            )
            ->withBatchSize($batchSize);
        $extractor->pushLimit(5);
        $counter->reset();

        // at 100 the first set is one batch that overshoots the limit - trimming it is the limit operator's job
        self::assertExtractedRowsCount($extractedRows, $extractor);
        static::assertSame(1, $counter->count);
    }

    public function test_is_repeatable(): void
    {
        static::assertTrue(
            (new DbalQueryExtractor(InMemorySqlite::connection(), 'SELECT * FROM users'))->isRepeatable(),
        );
    }
}
