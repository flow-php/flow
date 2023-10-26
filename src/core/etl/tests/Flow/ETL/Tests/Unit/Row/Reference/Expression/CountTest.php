<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference\Expression\Count;
use PHPUnit\Framework\TestCase;

final class CountTest extends TestCase
{
    public function test_count_array_entry() : void
    {
        $this->assertSame(
            2,
            (new Count(ref('array')))->eval(Row::create(Entry::array('array', ['foo', 'bar'])))
        );
        $this->assertSame(
            2,
            count(ref('array'))->eval(Row::create(Entry::array('array', ['foo', 'bar'])))
        );
        $this->assertTrue(
            ref('array')->count()->equals(lit(2))->eval(Row::create(Entry::array('array', ['foo', 'bar'])))
        );
    }

    public function test_count_string() : void
    {
        $this->assertNull(
            (new Count(ref('string')))->eval(Row::create(Entry::string('string', 'foo')))
        );
    }
}
