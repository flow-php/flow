<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Flow\ETL\Adapter\Doctrine\DbalQueryExtractor;
use Flow\ETL\Adapter\Doctrine\ParametersSet;
use Flow\ETL\Adapter\Doctrine\Tests\Context\InMemorySqlite;
use Flow\ETL\Tests\FlowTestCase;

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

    public function test_is_repeatable(): void
    {
        static::assertTrue(
            (new DbalQueryExtractor(InMemorySqlite::connection(), 'SELECT * FROM users'))->isRepeatable(),
        );
    }
}
