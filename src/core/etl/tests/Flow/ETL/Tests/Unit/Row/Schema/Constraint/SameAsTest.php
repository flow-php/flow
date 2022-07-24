<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Constraint;

use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Schema\Constraint\SameAs;
use PHPUnit\Framework\TestCase;

final class SameAsTest extends TestCase
{
    public function test_entry_value_same_as_constraint() : void
    {
        $this->assertTrue((new SameAs(10))->isSatisfiedBy(new IntegerEntry('integer', 10)));
        $this->assertFalse((new SameAs(20))->isSatisfiedBy(new IntegerEntry('integer', 10)));
    }
}
