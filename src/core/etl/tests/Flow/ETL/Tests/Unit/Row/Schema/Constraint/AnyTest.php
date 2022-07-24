<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Schema\Constraint;

use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Schema\Constraint\Any;
use Flow\ETL\Row\Schema\Constraint\SameAs;
use PHPUnit\Framework\TestCase;

final class AnyTest extends TestCase
{
    public function test_constraint_any() : void
    {
        $this->assertTrue(
            (new Any(
                new SameAs(10),
                new SameAs('test'),
                new SameAs(false),
            ))
                ->isSatisfiedBy(new StringEntry('type', 'test'))
        );

        $this->assertFalse(
            (new Any(
                new SameAs(10),
                new SameAs('test'),
                new SameAs(false),
            ))
                ->isSatisfiedBy(new StringEntry('type', 'not-value'))
        );
    }
}
