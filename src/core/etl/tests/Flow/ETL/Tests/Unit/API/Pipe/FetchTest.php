<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\fetch;
use function Flow\ETL\DSL\from_array;

final class FetchTest extends FlowTestCase
{
    public function test_fetch_returns_the_limited_rows(): void
    {
        static::assertSame([['a' => 1]], fetch(1)(df()->read(from_array([['a' => 1], ['a' => 2]])))->toArray());
    }
}
