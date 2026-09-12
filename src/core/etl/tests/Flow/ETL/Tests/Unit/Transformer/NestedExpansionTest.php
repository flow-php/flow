<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\Context\NestedExpansionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ListColumnsMother;
use Flow\ETL\Transformer\NestedExpansion;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\structure;
use function Flow\ETL\DSL\when;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;

final class NestedExpansionTest extends FlowTestCase
{
    /**
     * @param array<string, mixed> $override
     * @param list<mixed> $eval
     */
    #[DataProvider('expansions')]
    public function test_declares_the_zipped_type_and_gives_one_value_per_zipped_position(
        ScalarFunction $tree,
        array $override,
        string $returns,
        array $eval,
    ): void {
        $expansion = NestedExpansionContext::of($tree);

        static::assertSame($returns, $expansion->returns()->toString());
        static::assertSame($eval, $expansion->eval(ListColumnsMother::row($override), flow_context(config())));
    }

    /**
     * @return Generator<string, array{ScalarFunction, array<string, mixed>, string, list<mixed>}>
     */
    public static function expansions(): Generator
    {
        yield 'one expand' => [
            structure(['tag' => ref('tags')->expand()]),
            [],
            'structure{tag: string}',
            [['tag' => 'x'], ['tag' => 'y']],
        ];
        yield 'zip pads the shorter' => [
            structure(['n' => ref('nums')->expand(), 'tag' => ref('tags')->expand()]),
            [],
            'structure{n: ?integer, tag: ?string}',
            [['n' => 1, 'tag' => 'x'], ['n' => 2, 'tag' => 'y'], ['n' => 3, 'tag' => null]],
        ];
        yield 'concat skips a padded null' => [
            concat(ref('nums')->expand(), ref('tags')->expand()),
            [],
            'string',
            ['1x', '2y', '3'],
        ];
        yield 'sole empty list' => [
            structure(['tag' => ref('tags')->expand()]),
            ['tags' => []],
            'structure{tag: string}',
            [],
        ];
        yield 'empty beside non-empty' => [
            structure(['n' => ref('nums')->expand(), 'tag' => ref('tags')->expand()]),
            ['nums' => [1, 2], 'tags' => []],
            'structure{n: ?integer, tag: ?string}',
            [['n' => 1, 'tag' => null], ['n' => 2, 'tag' => null]],
        ];
        yield 'onEach operand' => [
            ref('lists')->expand()->onEach(concat(ref('element'), lit('!'))),
            [],
            'list<string>',
            [['q!', 'r!'], ['s!']],
        ];

        $tags = ref('tags')->expand();

        yield 'a node holding a reference, used twice, gives two padded columns' => [
            structure(['a' => $tags, 'b' => $tags]),
            [],
            'structure{a: ?string, b: ?string}',
            [['a' => 'x', 'b' => 'x'], ['a' => 'y', 'b' => 'y']],
        ];

        $literal = lit(['x', 'y'])->expand();

        yield 'a node without references, used twice, gives one column' => [
            structure(['a' => $literal, 'b' => $literal]),
            [],
            'structure{a: string, b: string}',
            [['a' => 'x', 'b' => 'x'], ['a' => 'y', 'b' => 'y']],
        ];
        yield 'branch not taken still expands' => [
            when(ref('id')->equals(lit('b')), ref('tags')->expand(), lit('none')),
            [],
            'string',
            ['none', 'none'],
        ];
    }

    public function test_a_null_list_throws(): void
    {
        $expansion = NestedExpansionContext::of(structure(['tag' => ref('tags')->expand()]));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayExpand requires non-null array');

        $expansion->eval(ListColumnsMother::row(['tags' => null]), flow_context(config()));
    }

    public function test_a_when_guard_does_not_stop_the_expand(): void
    {
        $expansion = NestedExpansionContext::of(when(ref('tags')->isNull(), lit('none'), ref('tags')->expand()));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('ArrayExpand requires non-null array');

        $expansion->eval(ListColumnsMother::row(['tags' => null]), flow_context(config()));
    }

    public function test_a_padded_null_follows_the_null_rule_of_the_function_reading_it(): void
    {
        $expansion = NestedExpansionContext::of(structure([
            'n' => ref('nums')->expand()->plus(lit(1)),
            't' => ref('tags')->expand(),
        ]));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Plus function requires non-null values');

        $expansion->eval(ListColumnsMother::row(['nums' => [1, 2], 'tags' => ['x', 'y', 'z']]), flow_context(config()));
    }

    public function test_of_is_null_for_a_root_expand(): void
    {
        static::assertNull(NestedExpansion::of(
            (new ReferenceResolver())->resolve(ref('tags')->expand(), ListColumnsMother::schema()),
            ListColumnsMother::schema(),
        ));
    }

    public function test_of_is_null_without_an_expand(): void
    {
        static::assertNull(NestedExpansion::of(
            (new ReferenceResolver())->resolve(concat(ref('id'), lit('x')), ListColumnsMother::schema()),
            ListColumnsMother::schema(),
        ));
    }

    public function test_zipped_expands_over_mixed_lists_stay_mixed(): void
    {
        $expansion = NestedExpansionContext::of(
            structure(['a' => ref('a')->expand(), 'b' => ref('b')->expand()]),
            schema(list_schema('a', type_list(type_mixed())), list_schema('b', type_list(type_mixed()))),
        );

        static::assertSame('structure{a: mixed, b: mixed}', $expansion->returns()->toString());
        static::assertSame(
            [['a' => 1, 'b' => 'x'], ['a' => 2, 'b' => null]],
            $expansion->eval(row(['a' => [1, 2], 'b' => ['x']]), flow_context(config())),
        );
    }

    public function test_an_expand_over_a_map_reads_its_values_by_position(): void
    {
        static::assertSame(
            [['v' => 1], ['v' => 2]],
            NestedExpansionContext::of(structure([
                'v' => ref('m')->expand(),
            ]), schema(map_schema('m', type_map(type_string(), type_integer()))))->eval(row(['m' => [
                'a' => 1,
                'b' => 2,
            ]]), flow_context(config())),
        );
    }

    public function test_the_synthesized_column_does_not_shadow_an_input_column(): void
    {
        static::assertSame(
            [['c' => 'kept', 't' => 'x']],
            NestedExpansionContext::of(
                structure(['c' => ref("\0expand:0"), 't' => ref('tags')->expand()]),
                schema(str_schema("\0expand:0"), list_schema('tags', type_list(type_string()))),
            )->eval(row(["\0expand:0" => 'kept', 'tags' => ['x']]), flow_context(config())),
        );
    }
}
