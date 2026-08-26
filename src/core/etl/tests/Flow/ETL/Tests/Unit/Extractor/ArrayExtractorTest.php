<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Definition\UnionDefinition;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Native\UnionType;
use PHPUnit\Framework\Attributes\TestWith;

use function array_keys;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\execution_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
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
            schema: schema(int_schema('id'), new UnionDefinition('a', $union, true)),
        );

        $rows = iterator_to_array($extractor->extract(execution_context(config_builder()->build())));

        static::assertInstanceOf(IntegerEntry::class, $rows[0]->first()->get('a'));
        static::assertSame(42, $rows[0]->first()->get('a')->value());
        static::assertInstanceOf(StringEntry::class, $rows[1]->first()->get('a'));
        static::assertSame('x', $rows[1]->first()->get('a')->value());
        static::assertInstanceOf(StringEntry::class, $rows[2]->first()->get('a'));
        static::assertNull($rows[2]->first()->get('a')->value());
    }

    public function test_generator_extraction_with_a_declared_schema(): void
    {
        $generator = static function () {
            yield ['id' => 1, 'name' => 'Norbert'];
            yield ['id' => 2, 'name' => 'Michal'];
        };

        $extractor = from_array($generator())->withSchema(schema(int_schema('id'), str_schema('name')));

        $rows = iterator_to_array($extractor->extract(execution_context(config())));

        static::assertCount(2, $rows);
        static::assertInstanceOf(Rows::class, $rows[0]);
        static::assertInstanceOf(Rows::class, $rows[1]);
        static::assertSame(['id' => 1, 'name' => 'Norbert'], $rows[0]->first()->toArray());
        static::assertSame(['id' => 2, 'name' => 'Michal'], $rows[1]->first()->toArray());
    }

    public function test_extracting_a_generator_without_a_declared_schema_is_refused(): void
    {
        $generator = static function () {
            yield ['id' => 1];
        };

        $this->expectException(SchemaNotDerivableException::class);
        $this->expectExceptionMessage('cannot read its dataset twice');

        iterator_to_array(from_array($generator())->extract(execution_context(config())));
    }

    /**
     * An int-keyed row is a positional record. The derived schema must name its columns exactly as
     * array_to_rows() does, or the schema names never match the entries.
     *
     * @param list<array<mixed>> $dataset
     * @param list<non-empty-string> $expected
     */
    #[TestWith([[['a', 'b']], ['e00', 'e01']])]
    #[TestWith([[[10 => 'x', 20 => 'y']], ['e10', 'e20']])]
    #[TestWith([[['0' => 'z']], ['e00']])]
    #[TestWith([[['id' => 1, 'name' => 'n']], ['id', 'name']])]
    public function test_positional_rows_are_named_like_the_hydrator_names_them(array $dataset, array $expected): void
    {
        static::assertSame($expected, array_keys(from_array($dataset)->schema()->definitions()));
        static::assertSame(
            $expected,
            array_keys(data_frame()->read(from_array($dataset))->fetch()->first()->toArray()),
        );
    }

    public function test_an_empty_array_does_not_downgrade_the_column_to_json(): void
    {
        static::assertSame(
            'list<?string>',
            data_frame()
                ->read(from_array([['tags' => ['a', 'b']], ['tags' => []]]))
                ->schema()
                ->findDefinition('tags')
                ?->type()
                ->toString(),
        );
    }
}
