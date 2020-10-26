<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\AddStampToStringEntryTransformer;
use Flow\ETL\Transformers;
use PHPUnit\Framework\TestCase;

final class TransformersTest extends TestCase
{
    public function test_applies_transformers_in_order_of_addition() : void
    {
        $row = Row::create(new StringEntry('name', 'zero'));

        $transformers = new Transformers(
            AddStampToStringEntryTransformer::divideBySemicolon('name', 'one'),
            AddStampToStringEntryTransformer::divideBySemicolon('name', 'two')
        );
        $transformers = $transformers->add(
            AddStampToStringEntryTransformer::divideBySemicolon('name', 'three')
        );
        $transformers = $transformers->add(
            AddStampToStringEntryTransformer::divideBySemicolon('name', 'four'),
            AddStampToStringEntryTransformer::divideBySemicolon('name', 'five')
        );

        $this->assertEquals(
            new Rows(Row::create(new StringEntry('name', 'zero:one:two:three:four:five'))),
            $transformers->transform(new Rows($row)),
        );
    }
}
