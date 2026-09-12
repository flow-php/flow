<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExpandingFunctions;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\concat;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\structure;

final class ExpandingFunctionsTest extends FlowTestCase
{
    public function test_in_is_empty_without_an_expand(): void
    {
        static::assertSame([], (new ExpandingFunctions())->in(concat(ref('id'), lit('x'))));
    }

    public function test_in_includes_the_root(): void
    {
        $expand = ref('tags')->expand();

        static::assertSame([$expand], (new ExpandingFunctions())->in($expand));
    }

    public function test_in_lists_expands_in_pre_order(): void
    {
        $tags = ref('tags')->expand();
        $nums = ref('nums')->expand();

        static::assertSame(
            [$tags, $nums],
            (new ExpandingFunctions())->in(structure(['a' => $tags, 'b' => concat(lit('-'), $nums)])),
        );
    }

    public function test_in_finds_an_expand_that_is_an_on_each_operand(): void
    {
        $expand = ref('lists')->expand();

        static::assertSame(
            [$expand],
            (new ExpandingFunctions())->in($expand->onEach(concat(ref('element'), lit('!')))),
        );
    }

    public function test_refuse_names_the_site(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'filter() cannot contain array_expand(), it turns one row into many rows. Expand with withEntry() first, then use the new column.',
        );

        (new ExpandingFunctions())->refuse(ref('tags')->expand()->equals(lit('x')), 'filter');
    }

    public function test_refuse_passes_a_tree_without_an_expand(): void
    {
        $this->expectNotToPerformAssertions();

        (new ExpandingFunctions())->refuse(ref('id')->equals(lit('x')), 'filter');
    }

    #[DataProvider('expand_inside_expand_provider')]
    public function test_refuse_nested_refuses_an_expand_below_an_expand(ScalarFunction $tree): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'array_expand() cannot contain another array_expand(). Expand one level per withEntry().',
        );

        (new ExpandingFunctions())->refuseNested($tree);
    }

    /**
     * @return Generator<string, array{ScalarFunction}>
     */
    public static function expand_inside_expand_provider(): Generator
    {
        yield 'directly' => [ref('lists')->expand()->expand()];
        yield 'through a scalar' => [ref('lists')->expand()->arrayReverse()->expand()];
        yield 'inside a structure' => [structure(['v' => ref('lists')->expand()->expand()])];
    }

    public function test_refuse_nested_passes_sibling_expands(): void
    {
        $this->expectNotToPerformAssertions();

        (new ExpandingFunctions())->refuseNested(structure([
            'n' => ref('nums')->expand(),
            't' => ref('tags')->expand(),
        ]));
    }
}
