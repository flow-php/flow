<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use Flow\ETL\Row\References;
use PHPUnit\Framework\TestCase;

final class ReferencesTest extends TestCase
{
    public function test_lazy_without() : void
    {
        $refs = refs()->without('id')->add('id')->add('name');

        $this->assertEquals(
            refs('name')->all(),
            $refs->all()
        );
    }

    public function test_that_reference_with_alias_exists() : void
    {
        $refs = new References(ref('id')->as('test'), ref('name'));

        $this->assertFalse($refs->has(ref('id')));
        $this->assertTrue($refs->has(ref('test')));
    }

    public function test_that_reference_without_alias_exists() : void
    {
        $refs = new References(ref('id'), ref('name'));

        $this->assertTrue($refs->has(ref('id')));
        $this->assertFalse($refs->has(ref('test')));
    }

    public function test_without() : void
    {
        $this->assertEquals(
            refs('name')->all(),
            refs('id', 'name')->without('id')->all()
        );
    }
}
