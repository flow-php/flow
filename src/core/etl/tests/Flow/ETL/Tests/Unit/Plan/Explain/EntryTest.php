<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Plan\Explain\Entry;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\NodeMother;

final class EntryTest extends FlowTestCase
{
    public function test_title_puts_the_number_before_the_name(): void
    {
        static::assertSame('#3 Read', (new Entry(NodeMother::read(), 3, false, []))->title('Read'));
    }

    public function test_title_of_an_entry_without_a_number_is_the_name(): void
    {
        static::assertSame('Outputs', (new Entry(NodeMother::read(), null, false, []))->title('Outputs'));
    }
}
