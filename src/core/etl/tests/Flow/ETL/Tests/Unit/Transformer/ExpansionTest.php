<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row\ResolvedReference;
use Flow\ETL\Tests\Context\ExpansionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ListColumnsMother;
use Flow\ETL\Transformer\Expansion;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\structure;
use function Flow\ETL\DSL\when;

final class ExpansionTest extends FlowTestCase
{
    #[DataProvider('expansions')]
    public function test_declares_the_zipped_type(ScalarFunction $tree, string $returns): void
    {
        static::assertSame($returns, ExpansionContext::of($tree)->returns()->toString());
    }

    /**
     * @return Generator<string, array{ScalarFunction, string}>
     */
    public static function expansions(): Generator
    {
        yield 'one expand' => [
            structure(['tag' => ref('tags')->expand()]),
            'structure{tag: string}',
        ];
        yield 'zip pads the shorter' => [
            structure(['n' => ref('nums')->expand(), 'tag' => ref('tags')->expand()]),
            'structure{n: ?integer, tag: ?string}',
        ];
        yield 'concat skips a padded null' => [
            concat(ref('nums')->expand(), ref('tags')->expand()),
            'string',
        ];
        yield 'sole empty list' => [
            structure(['tag' => ref('tags')->expand()]),
            'structure{tag: string}',
        ];
        yield 'empty beside non-empty' => [
            structure(['n' => ref('nums')->expand(), 'tag' => ref('tags')->expand()]),
            'structure{n: ?integer, tag: ?string}',
        ];
        yield 'onEach operand' => [
            ref('lists')->expand()->onEach(concat(ref('element'), lit('!'))),
            'list<string>',
        ];

        $tags = ref('tags')->expand();

        yield 'a node holding a reference, used twice, gives two padded columns' => [
            structure(['a' => $tags, 'b' => $tags]),
            'structure{a: ?string, b: ?string}',
        ];

        $literal = lit(['x', 'y'])->expand();

        yield 'a node without references, used twice, gives one column' => [
            structure(['a' => $literal, 'b' => $literal]),
            'structure{a: string, b: string}',
        ];
        yield 'branch not taken still expands' => [
            when(ref('id')->equals(lit('b')), ref('tags')->expand(), lit('none')),
            'string',
        ];
        yield 'a null list gives no values' => [
            structure(['tag' => ref('tags')->expand()]),
            'structure{tag: string}',
        ];
        yield 'a when guard does not stop the expand of a null list' => [
            when(ref('tags')->isNull(), lit('none'), ref('tags')->expand()),
            'string',
        ];
    }

    public function test_a_root_expand_is_rewritten_into_its_synthesized_reference(): void
    {
        $root = ExpansionContext::of(ref('tags')->expand())->root();

        static::assertInstanceOf(ResolvedReference::class, $root);
        static::assertSame("\0expand:0", $root->name());
    }

    public function test_positions_name_the_source_row_and_the_element_of_every_output_position(): void
    {
        static::assertSame(
            [[0, 0, 1], ["\0expand:0" => ['x', 'y', 'z']]],
            ExpansionContext::of(ref('tags')->expand())->positions(array_to_rows([
                ['id' => 'a', 'tags' => ['x', 'y']],
                ['id' => 'b', 'tags' => ['z']],
            ], ListColumnsMother::tagsSchema()), flow_context(config())),
        );
    }

    public function test_elements_append_the_cells_of_the_window_starting_at_the_offset(): void
    {
        $rows = array_to_rows([
            ['id' => 'a', 'tags' => ['x']],
            ['id' => 'b', 'tags' => ['y']],
        ], ListColumnsMother::tagsSchema());

        static::assertSame(
            [
                ['id' => 'a', 'tags' => ['x'], "\0expand:0" => 'y'],
                ['id' => 'b', 'tags' => ['y'], "\0expand:0" => 'z'],
            ],
            ExpansionContext::of(ref('tags')->expand())->elements(
                $rows,
                ["\0expand:0" => ['x', 'y', 'z']],
                1,
                flow_context(config())->backend(),
            )->toArray(),
        );
    }

    public function test_synthesized_declares_one_nullable_column_per_distinct_expand(): void
    {
        static::assertSame(
            ["\0expand:0", "\0expand:1"],
            ExpansionContext::of(structure([
                'n' => ref('nums')->expand(),
                't' => ref('tags')->expand(),
            ]))
                ->synthesized()
                ->references()
                ->names(),
        );
    }

    public function test_of_is_null_without_an_expand(): void
    {
        static::assertNull(Expansion::of(
            (new ReferenceResolver())->resolve(concat(ref('id'), lit('x')), ListColumnsMother::schema()),
            ListColumnsMother::schema(),
        ));
    }
}
