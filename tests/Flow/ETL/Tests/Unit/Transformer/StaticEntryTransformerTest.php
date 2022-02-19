<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\StaticEntryTransformer;
use PHPUnit\Framework\TestCase;

final class StaticEntryTransformerTest extends TestCase
{
    public function test_string_contact() : void
    {
        $transformer = new StaticEntryTransformer(new Row\Entry\BooleanEntry('active', false));

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\StringEntry('id', '1'),
                new Row\Entry\StringEntry('first_name', 'Norbert'),
                new Row\Entry\StringEntry('last_name', 'Orzechowicz'),
            )
        ));

        $this->assertSame(
            [
                [
                    'id' => '1',
                    'first_name' => 'Norbert',
                    'last_name' => 'Orzechowicz',
                    'active' => false,
                ],
            ],
            $rows->toArray()
        );
    }
}
