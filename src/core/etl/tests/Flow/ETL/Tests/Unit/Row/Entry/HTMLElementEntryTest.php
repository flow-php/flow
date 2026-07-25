<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Entry;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\html_element_entry;

final class HTMLElementEntryTest extends FlowTestCase
{
    public function test_prevents_from_creating_entry_with_empty_entry_name(): void
    {
        $this->expectExceptionMessage('Entry name cannot be empty');

        html_element_entry('', null);
    }

    public function test_creating_entry_with_null_value(): void
    {
        static::assertNull(html_element_entry('element', null)->value());
        static::assertSame('element', html_element_entry('element', null)->name());
    }
}
