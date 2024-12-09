<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{coalesce, int_entry, lit, ref, row};
use PHPUnit\Framework\TestCase;

final class CoalesceTest extends TestCase
{
    public function test_coalesce_entries() : void
    {
        self::assertSame(
            1,
            coalesce(ref('name'), ref('id'), lit('N/A'))->eval(row(int_entry('id', 1)))
        );
    }

    public function test_coalesce_on_lit_and_non_existing_entries() : void
    {
        self::assertSame(
            'N/A',
            coalesce(ref('non_existing'), ref('string'), lit('N/A'))->eval(row(int_entry('id', 1)))
        );
    }

    public function test_coalesce_on_ref() : void
    {
        self::assertSame(
            1,
            ref('name')->coalesce(ref('id'), lit('N/A'))->eval(row(int_entry('id', 1)))
        );
    }
}
