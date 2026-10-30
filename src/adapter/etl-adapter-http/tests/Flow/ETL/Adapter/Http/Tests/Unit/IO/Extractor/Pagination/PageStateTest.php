<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\Tests\Unit\IO\Extractor\Pagination;

use Flow\ETL\Adapter\Http\IO\Extractor\Pagination\PageState;
use Flow\ETL\Tests\FlowTestCase;

final class PageStateTest extends FlowTestCase
{
    public function test_advance_increments_pages_and_accumulates_results(): void
    {
        $state = (new PageState())
            ->advance(25)
            ->advance(25);

        static::assertSame(2, $state->pagesFetched);
        static::assertSame(50, $state->resultsFetched);
    }
}
