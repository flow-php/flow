<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Pipe;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\read;

final class ReadTest extends FlowTestCase
{
    public function test_read_starts_a_frame_from_the_extractor(): void
    {
        static::assertSame(
            [['a' => 1], ['a' => 2]],
            read(from_array([['a' => 1], ['a' => 2]]))(df())->fetch()->toArray(),
        );
    }
}
