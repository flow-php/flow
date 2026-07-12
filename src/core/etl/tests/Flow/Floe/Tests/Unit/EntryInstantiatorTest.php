<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Row\Entry\Instantiators;
use Flow\ETL\Row\Entry\IntegerEntry;
use PHPUnit\Framework\TestCase;

final class EntryInstantiatorTest extends TestCase
{
    public function test_factories_are_memoized_per_class(): void
    {
        $instantiator = new Instantiators();

        static::assertSame($instantiator->for(IntegerEntry::class), $instantiator->for(IntegerEntry::class));
    }
}
