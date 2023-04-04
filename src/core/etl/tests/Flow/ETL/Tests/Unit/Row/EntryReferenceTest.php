<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class EntryReferenceTest extends TestCase
{
    public function test_executing_equals_expression() : void
    {
        $ref = ref('a')->equals(ref('b'));

        $this->assertTrue(
            $ref->eval(Row::create(Entry::integer('a', 1), Entry::integer('b', 1)))
        );
    }

    public function test_executing_expression() : void
    {
        $ref = ref('b')->literal(100);

        $this->assertSame(
            100,
            $ref->eval(Row::create(Entry::integer('a', 1)))
        );
    }
}
