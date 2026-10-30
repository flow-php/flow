<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\Grid;

use Flow\ETL\IO\Extractor\Grid\GridHeader;
use Flow\ETL\Tests\FlowTestCase;

final class GridHeaderTest extends FlowTestCase
{
    public function test_the_names_and_the_source_are_carried_together(): void
    {
        $header = new GridHeader(['id', 'name'], 'file:///a.csv');

        static::assertSame(['id', 'name'], $header->names);
        static::assertSame('file:///a.csv', $header->source);
    }

    public function test_it_normalizes_and_rebuilds_equal(): void
    {
        static::assertSame(
            ['names' => ['id'], 'source' => 'file:///a.csv'],
            (new GridHeader(['id'], 'file:///a.csv'))->normalize(),
        );
        static::assertEquals(
            new GridHeader(['id'], 'file:///a.csv'),
            GridHeader::fromArray((new GridHeader(['id'], 'file:///a.csv'))->normalize()),
        );
        static::assertEquals(new GridHeader([], null), GridHeader::fromArray((new GridHeader([], null))->normalize()));
    }
}
