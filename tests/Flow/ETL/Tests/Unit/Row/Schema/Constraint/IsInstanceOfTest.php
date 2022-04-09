<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Constraint;

use Flow\ETL\Row\Entry\ObjectEntry;
use Flow\ETL\Row\Schema\Constraint\IsInstanceOf;
use PHPUnit\Framework\TestCase;

final class IsInstanceOfTest extends TestCase
{
    public function test_entry_value_same_as_constraint() : void
    {
        $this->assertTrue((new IsInstanceOf(\ArrayIterator::class))->isSatisfiedBy(new ObjectEntry('array', new \ArrayIterator())));
        $this->assertFalse((new IsInstanceOf(\DateTimeImmutable::class))->isSatisfiedBy(new ObjectEntry('array', new \ArrayIterator())));
    }
}
