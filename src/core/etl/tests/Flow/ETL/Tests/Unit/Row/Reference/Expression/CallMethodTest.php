<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\call_method;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class CallMethodTest extends TestCase
{
    public function test_call_method() : void
    {
        $row = Row::create(
            Entry::datetime('object', '2023-01-01 00:00:00 UTC'),
            Entry::str('method', 'format'),
            Entry::str('method_param', 'H:i:s Y-m-d'),
        );

        $this->assertEquals(
            '00:00:00 2023-01-01',
            call_method(
                ref('object'),
                ref('method'),
                ref('method_param'),
            )->eval($row)
        );
    }

    public function test_method_not_string() : void
    {
        $row = Row::create(
            Entry::datetime('object', '2023-01-01 00:00:00 UTC'),
            Entry::datetime('method', '2023-01-01 00:00:00 UTC'),
        );

        $this->assertNull(
            call_method(
                ref('object'),
                ref('method')
            )->eval($row)
        );
    }

    public function test_not_existing_method() : void
    {
        $row = Row::create(
            Entry::datetime('object', '2023-01-01 00:00:00 UTC'),
            Entry::str('method', 'method_that_not_exists'),
        );

        $this->assertNull(
            call_method(
                ref('object'),
                ref('method')
            )->eval($row)
        );
    }

    public function test_null_method() : void
    {
        $row = Row::create(
            Entry::datetime('object', '2023-01-01 00:00:00 UTC'),
            Entry::null('method'),
        );

        $this->assertNull(
            call_method(
                ref('object'),
                ref('method')
            )->eval($row)
        );
    }

    public function test_null_object() : void
    {
        $row = Row::create(
            Entry::null('object'),
            Entry::str('method', 'getTimestamp'),
        );

        $this->assertNull(
            call_method(
                ref('object'),
                ref('method')
            )->eval($row)
        );
    }
}
