<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Unit\Pagination\StopWhen;

use Flow\ETL\Adapter\Http\Pagination\PageState;
use Flow\ETL\Adapter\Http\Pagination\StopWhen\PathMissing;
use Flow\ETL\Adapter\HTTP\Tests\Mother\PaginationMother;
use Flow\ETL\Tests\FlowTestCase;

final class PathMissingTest extends FlowTestCase
{
    public function test_empty_string_token_stops(): void
    {
        static::assertTrue((new PathMissing('meta.next'))->shouldStop(PaginationMother::decoded(['meta' => [
            'next' => '',
        ]]), new PageState()));
    }

    public function test_missing_path_stops(): void
    {
        static::assertTrue((new PathMissing('meta.next'))->shouldStop(
            PaginationMother::decoded(['meta' => []]),
            new PageState(),
        ));
    }

    public function test_null_token_stops(): void
    {
        static::assertTrue((new PathMissing('meta.next'))->shouldStop(PaginationMother::decoded(['meta' => [
            'next' => null,
        ]]), new PageState()));
    }

    public function test_present_token_does_not_stop(): void
    {
        static::assertFalse((new PathMissing('meta.next'))->shouldStop(PaginationMother::decoded(['meta' => [
            'next' => 'abc',
        ]]), new PageState()));
    }
}
