<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Native\UnionType;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\execution_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\union_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;
use function iterator_to_array;

final class ArrayExtractorTest extends FlowTestCase
{
    public function test_array_extractor(): void
    {
        $extractor = from_array([
            ['id' => 1, 'name' => 'Norbert'],
            ['id' => 2, 'name' => 'Michal'],
        ]);

        $rows = iterator_to_array($extractor->extract(execution_context(config_builder()->build())));

        static::assertCount(2, $rows);
        static::assertInstanceOf(Rows::class, $rows[0]);
        static::assertInstanceOf(Rows::class, $rows[1]);
        static::assertSame(['id' => 1, 'name' => 'Norbert'], $rows[0]->first()->toArray());
        static::assertSame(['id' => 2, 'name' => 'Michal'], $rows[1]->first()->toArray());
    }

    public function test_extraction_with_a_union_column_in_the_schema(): void
    {
        /** @var UnionType<mixed, mixed> $union */
        $union = type_union(type_string(), type_integer());

        $extractor = from_array(
            [
                ['id' => 1, 'a' => 42],
                ['id' => 2, 'a' => 'x'],
                ['id' => 3, 'a' => null],
            ],
            schema: schema(int_schema('id'), union_schema('a', $union, true)),
        );

        $rows = iterator_to_array($extractor->extract(execution_context(config_builder()->build())));

        static::assertInstanceOf(IntegerEntry::class, $rows[0]->first()->get('a'));
        static::assertSame(42, $rows[0]->first()->get('a')->value());
        static::assertInstanceOf(StringEntry::class, $rows[1]->first()->get('a'));
        static::assertSame('x', $rows[1]->first()->get('a')->value());
        static::assertInstanceOf(StringEntry::class, $rows[2]->first()->get('a'));
        static::assertNull($rows[2]->first()->get('a')->value());
    }

    public function test_generator_extraction(): void
    {
        $generator = static function () {
            yield ['id' => 1, 'name' => 'Norbert'];
            yield ['id' => 2, 'name' => 'Michal'];
        };

        $extractor = from_array($generator());

        $rows = iterator_to_array($extractor->extract(execution_context(config())));

        static::assertCount(2, $rows);
        static::assertInstanceOf(Rows::class, $rows[0]);
        static::assertInstanceOf(Rows::class, $rows[1]);
        static::assertSame(['id' => 1, 'name' => 'Norbert'], $rows[0]->first()->toArray());
        static::assertSame(['id' => 2, 'name' => 'Michal'], $rows[1]->first()->toArray());
    }
}
