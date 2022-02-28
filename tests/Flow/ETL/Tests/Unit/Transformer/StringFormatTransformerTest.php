<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Transformer;

use Flow\ETL\DSL\Transform;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class StringFormatTransformerTest extends TestCase
{
    public function test_string_format_transformer() : void
    {
        $transformer = Transform::string_format('id', 'https://examlpe.com/resource/%d');

        $rows = $transformer->transform(new Rows(
            new Row(new Row\Entries(new Row\Entry\IntegerEntry('id', 1))),
            new Row(new Row\Entries(new Row\Entry\IntegerEntry('id', 2))),
            new Row(new Row\Entries(new Row\Entry\IntegerEntry('id', 3))),
        ));

        $this->assertSame(
            [
                ['id' => 'https://examlpe.com/resource/1'],
                ['id' => 'https://examlpe.com/resource/2'],
                ['id' => 'https://examlpe.com/resource/3'],
            ],
            $rows->toArray()
        );
    }
}
