<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Flow\ETL\Adapter\Doctrine\Pages;
use Flow\ETL\Tests\FlowTestCase;

final class PagesTest extends FlowTestCase
{
    public function test_total_even_pages(): void
    {
        static::assertSame(100, (new Pages(1000, 10))->pages());
    }

    public function test_total_pages_with_last_page_not_full(): void
    {
        static::assertSame(100, (new Pages(999, 10))->pages());
    }

    public function test_total_pages_with_less_elements_than_page_size(): void
    {
        static::assertSame(1, (new Pages(8, 10))->pages());
    }
}
