<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Path\Filter;

use Flow\Filesystem\Path\Filter\Filters;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use PHPUnit\Framework\TestCase;

final class FiltersTest extends TestCase
{
    public function test_filters_are_the_members_in_the_order_they_were_given(): void
    {
        $onlyFiles = new OnlyFiles();
        $keepAll = new KeepAll();

        static::assertSame([$onlyFiles, $keepAll], (new Filters($onlyFiles))->add($keepAll)->filters());
    }
}
