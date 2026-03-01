<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\{ref, refs};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\References;
use Flow\ETL\Tests\FlowTestCase;

final class ReferencesTest extends FlowTestCase
{
    public function test_empty() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('References cannot be empty.');

        refs();
    }

    public function test_lazy_without() : void
    {
        $refs = refs('id')->without('id')->add('id')->add('name');

        self::assertEquals(
            refs('name')->all(),
            $refs->all()
        );
    }

    public function test_references_names() : void
    {
        $refs = refs('id', 'name');

        self::assertEquals(
            ['id', 'name'],
            $refs->names()
        );
    }

    public function test_that_reference_with_alias_exists() : void
    {
        $refs = new References(ref('id')->as('test'), ref('name'));

        self::assertFalse($refs->has(ref('id')));
        self::assertTrue($refs->has(ref('test')));
    }

    public function test_that_reference_without_alias_exists() : void
    {
        $refs = new References(ref('id'), ref('name'));

        self::assertTrue($refs->has(ref('id')));
        self::assertFalse($refs->has(ref('test')));
    }

    public function test_without() : void
    {
        self::assertEquals(
            refs('name')->all(),
            refs('id', 'name')->without('id')->all()
        );
    }
}
