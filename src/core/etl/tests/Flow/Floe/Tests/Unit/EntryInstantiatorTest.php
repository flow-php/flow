<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\Floe\EntryInstantiator;
use PHPUnit\Framework\TestCase;

final class EntryInstantiatorTest extends TestCase
{
    public function test_factories_are_memoized_per_class(): void
    {
        $instantiator = new EntryInstantiator();

        static::assertSame(
            $instantiator->factoryFor(IntegerEntry::class),
            $instantiator->factoryFor(IntegerEntry::class),
        );
    }
}
