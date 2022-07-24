<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Double\Stub;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

final class TransformTestData implements Transformer
{
    public function __serialize() : array
    {
    }

    public function __unserialize(array $data) : void
    {
    }

    public function transform(Rows $rows, FlowContext $context) : Rows
    {
        return $rows->map(
            fn (Row $row) : Row => Row::create(
                new Row\Entry\IntegerEntry('id', $row->valueOf('row')['id']),
                new Row\Entry\StringEntry('name', $row->valueOf('row')['name']),
                new Row\Entry\StringEntry('description', $row->valueOf('row')['description'])
            )
        );
    }
}
