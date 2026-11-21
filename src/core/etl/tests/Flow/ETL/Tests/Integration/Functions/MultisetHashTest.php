<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Functions;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\multiset_hash;

final class MultisetHashTest extends FlowTestCase
{
    public function test_multiset_hash_with_group_by(): void
    {
        static::assertSame(
            [
                ['g' => 'a', '_multiset_hash' => '1a61bf177bb5a2b671c0494779768cc9'],
                ['g' => 'b', '_multiset_hash' => '5df1c2802504f984036241b9f1e54ea0'],
            ],
            df()
                ->read(from_array([['g' => 'a', 'v' => 1], ['g' => 'b', 'v' => 1], ['g' => 'a', 'v' => 2]]))
                ->groupBy('g')
                ->aggregate(multiset_hash())
                ->fetch()
                ->toArray(),
        );
    }
}
