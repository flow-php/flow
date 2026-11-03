<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\Double\ThrowingTransformer;
use Flow\ETL\Tests\FlowTestCase;
use RuntimeException;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\on_error;
use function Flow\ETL\DSL\skip_rows_handler;

final class OnErrorTest extends FlowTestCase
{
    public function test_on_error_through_invoke(): void
    {
        static::assertSame(
            [],
            on_error(skip_rows_handler())(
                df()->read(from_array([['a' => 1]]))->with(new ThrowingTransformer(new RuntimeException('failed'))),
            )
                ->fetch()
                ->toArray(),
        );
    }

    public function test_on_error_through_with(): void
    {
        static::assertSame(
            [],
            df()
                ->read(from_array([['a' => 1]]))
                ->with(new ThrowingTransformer(new RuntimeException('failed')))
                ->with(on_error(skip_rows_handler()))
                ->fetch()
                ->toArray(),
        );
    }
}
