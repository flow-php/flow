<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer\StringConcatTransformer;
use PHPUnit\Framework\TestCase;

final class StringConcatTransformerTest extends TestCase
{
    public function test_string_contact() : void
    {
        $transformer = new StringConcatTransformer([
            'id', 'first_name', 'last_name',
        ]);

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
                    'element' => '1 Norbert Orzechowicz',
                ],
            ],
            $rows->toArray()
        );
    }

    public function test_string_contact_with_non_string_value() : void
    {
        $transformer = new StringConcatTransformer([
            'id', 'first_name', 'last_name',
        ]);

        $rows = $transformer->transform(new Rows(
            Row::create(
                new Row\Entry\IntegerEntry('id', 1),
                new Row\Entry\StringEntry('first_name', 'Norbert'),
                new Row\Entry\StringEntry('last_name', 'Orzechowicz'),
            )
        ));

        $this->assertSame(
            [
                [
                    'id' => 1,
                    'first_name' => 'Norbert',
                    'last_name' => 'Orzechowicz',
                    'element' => 'Norbert Orzechowicz',
                ],
            ],
            $rows->toArray()
        );
    }
}
