<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\API\Transformation;

use Flow\ETL\Tests\Double\StaticExtractorFactory;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\join_each;
use function Flow\ETL\DSL\join_on;

final class JoinEachTest extends FlowTestCase
{
    public function test_join_each_through_invoke(): void
    {
        static::assertSame(
            [['id' => 1, 'a' => 'x', 'r_id' => 1, 'r_b' => 'z']],
            join_each(
                new StaticExtractorFactory(from_array([['id' => 1, 'b' => 'z']])),
                join_on(['id' => 'id'], 'r_'),
                'inner',
            )(df()->read(from_array([['id' => 1, 'a' => 'x'], ['id' => 2, 'a' => 'y']])))
                ->fetch()
                ->toArray(),
        );
    }

    public function test_join_each_through_with(): void
    {
        static::assertSame(
            [['id' => 1, 'a' => 'x', 'r_id' => 1, 'r_b' => 'z']],
            df()
                ->read(from_array([['id' => 1, 'a' => 'x'], ['id' => 2, 'a' => 'y']]))
                ->with(join_each(
                    new StaticExtractorFactory(from_array([['id' => 1, 'b' => 'z']])),
                    join_on(['id' => 'id'], 'r_'),
                    'inner',
                ))
                ->fetch()
                ->toArray(),
        );
    }
}
