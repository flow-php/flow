<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Loader;

use Flow\ETL\Loader\LoaderTree;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\ETL\Tests\Double\WrappingLoader;
use Flow\ETL\Tests\FlowTestCase;

final class LoaderTreeTest extends FlowTestCase
{
    public function test_flattening_a_loader_reachable_through_more_than_one_branch(): void
    {
        $shared = new SpyLoader();
        $left = new WrappingLoader($shared);
        $right = new WrappingLoader($shared);
        $root = new WrappingLoader($left, $right);

        static::assertSame([$root, $left, $right, $shared], (new LoaderTree())->flatten($root));
    }

    public function test_flattening_a_loader_that_overrides_itself(): void
    {
        $loader = new WrappingLoader();
        $loader->wrapped = [$loader];

        static::assertSame([$loader], (new LoaderTree())->flatten($loader));
    }

    public function test_flattening_a_loader_wrapping_many_loaders(): void
    {
        $first = new SpyLoader();
        $second = new SpyLoader();
        $root = new WrappingLoader($first, $second);

        static::assertSame([$root, $first, $second], (new LoaderTree())->flatten($root));
    }

    public function test_flattening_a_plain_loader(): void
    {
        $loader = new SpyLoader();

        static::assertSame([$loader], (new LoaderTree())->flatten($loader));
    }

    public function test_flattening_a_wrapper_that_overrides_nobody(): void
    {
        $root = new WrappingLoader();

        static::assertSame([$root], (new LoaderTree())->flatten($root));
    }

    public function test_flattening_nested_wrappers(): void
    {
        $innermost = new SpyLoader();
        $middle = new WrappingLoader($innermost);
        $root = new WrappingLoader($middle);

        static::assertSame([$root, $middle, $innermost], (new LoaderTree())->flatten($root));
    }

    public function test_flattening_two_loaders_pointing_at_each_other(): void
    {
        $first = new WrappingLoader();
        $second = new WrappingLoader($first);
        $first->wrapped = [$second];

        static::assertSame([$first, $second], (new LoaderTree())->flatten($first));
    }
}
