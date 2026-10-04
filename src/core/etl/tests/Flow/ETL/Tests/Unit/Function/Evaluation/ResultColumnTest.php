<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function\Evaluation;

use ArrayObject;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Column\ValueColumn;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\call;
use function Flow\ETL\DSL\json_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_html_element;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function Flow\Types\DSL\type_xml;
use function Flow\Types\DSL\type_xml_element;

final class ResultColumnTest extends TestCase
{
    public function test_a_not_null_return_still_accepts_null(): void
    {
        $column = (new ResultColumn(new PhpBackend()))->of(lit(5), [5, null]);

        static::assertSame([5, null], $column->values());
        static::assertEquals(type_integer(), $column->type());
    }

    public function test_an_unresolved_tree_throws(): void
    {
        $this->expectException(InvalidLogicException::class);

        (new ResultColumn(new PhpBackend()))->of(ref('a'), [1]);
    }

    public function test_casts_each_value_into_the_returned_type(): void
    {
        static::assertSame([5, 6], (new ResultColumn(new PhpBackend()))->of(lit(1), ['5', 6])->values());
    }

    public function test_builds_through_the_backend_it_was_given(): void
    {
        $backend = new SpyBackend();

        (new ResultColumn($backend))->of(lit(1), [1, 2]);
        (new ResultColumn($backend))->of(lit(1), ['1', 2]);

        static::assertSame(2, $backend->builders());
    }

    public function test_null_type_builds_a_null_column(): void
    {
        $column = (new ResultColumn(new PhpBackend()))->of(lit(null), [null, null]);

        static::assertEquals(type_null(), $column->type());
        static::assertSame(2, $column->nullCount());
    }

    public function test_refusal_names_the_row_of_the_value(): void
    {
        try {
            (new ResultColumn(new PhpBackend()))->of(lit(1), [1, 2, 'x']);
            static::fail('expected a SchemaMismatchException');
        } catch (SchemaMismatchException $e) {
            static::assertSame(2, $e->rowIndex);
        }
    }

    public function test_type_without_a_column_kind_is_an_untyped_column(): void
    {
        $column = (new ResultColumn(new PhpBackend()))->of(call(lit('strval'), type_instance_of(ArrayObject::class)), [
            new ArrayObject(),
            null,
        ]);

        static::assertInstanceOf(ValueColumn::class, $column);
        static::assertSame(2, $column->count());
    }

    public function test_underivable_return_is_an_untyped_column(): void
    {
        $function = (new ReferenceResolver())->resolve(
            ref('j')->jsonDecode()->arrayGet('name'),
            schema(json_schema('j')),
        );

        $column = (new ResultColumn(new PhpBackend()))->of($function, ['a', ['b' => 1]]);

        static::assertInstanceOf(ValueColumn::class, $column);
        static::assertSame(['a', ['b' => 1]], $column->values());
    }

    /**
     * @return Generator<string, array{Type<mixed>, bool}>
     */
    public static function intermediates(): Generator
    {
        yield 'integer' => [type_integer(), true];
        yield 'list of structures' => [type_list(type_structure(['a' => type_string()])), true];
        yield 'untyped array' => [type_array(), false];
        yield 'list of untyped arrays' => [type_list(type_array()), false];
        yield 'xml' => [type_xml(), false];
        yield 'xml element in a map' => [type_map(type_string(), type_xml_element()), false];
        yield 'html element in a structure' => [type_structure(['a' => type_optional(type_html_element())]), false];
        yield 'html in a list' => [type_list(type_html()), false];
        yield 'mixed' => [type_mixed(), false];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('intermediates')]
    public function test_a_typed_column_holds_an_intermediate_only_when_it_keeps_the_values(
        Type $type,
        bool $holds,
    ): void {
        static::assertSame($holds, (new ResultColumn(new PhpBackend()))->holdsLosslessly($type));
    }

    public function test_an_untyped_array_keeps_its_objects(): void
    {
        $object = new ArrayObject();
        $column = (new ResultColumn(new PhpBackend()))->typed(type_optional(type_array()), [[$object, 'count']]);

        static::assertInstanceOf(ValueColumn::class, $column);
        static::assertSame([[$object, 'count']], $column->values());
    }

    public function test_lists_of_an_expand_are_a_list_column(): void
    {
        $column = (new ResultColumn(new PhpBackend()))->lists(lit(1), [[1, 2], []]);

        static::assertEquals(type_list(type_integer()), $column->type());
        static::assertSame([[1, 2], []], $column->values());
    }
}
