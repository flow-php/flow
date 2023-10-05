<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayReverseTest extends TestCase
{
    public function test_array_reverse_array_entry() : void
    {
        $this->assertSame(
            [5, 3, 10, 4],
            ref('a')->arrayReverse()
                ->eval(
                    Row::create(
                        Entry::array('a', [4, 10, 3, 5]),
                    ),
                )
        );
    }

    public function test_array_reverse_non_array_entry() : void
    {
        $this->assertNull(
            ref('a')->arrayReverse()
                ->eval(
                    Row::create(
                        Entry::int('a', 123),
                    ),
                )
        );
    }
}
