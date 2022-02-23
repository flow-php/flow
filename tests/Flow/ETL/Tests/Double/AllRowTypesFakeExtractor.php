<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ArrayEntry;
use Flow\ETL\Row\Entry\BooleanEntry;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\FloatEntry;
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\NullEntry;
use Flow\ETL\Row\Entry\StringEntry;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Rows;

final class AllRowTypesFakeExtractor implements Extractor
{
    private int $total;

    private int $rowsSize;

    public function __construct(int $total, int $rowsSize)
    {
        $this->total = $total;
        $this->rowsSize = $rowsSize;
    }

    /**
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract() : \Generator
    {
        for ($i = 0; $i < $this->total; $i++) {
            $id = $this->rowsSize * $i;

            $rows = [];

            for ($r = 0; $r < $this->rowsSize; $r++) {
                $rows[] = Row::create(
                    new IntegerEntry('id', $id + $r),
                    new FloatEntry('price', \random_int(100, 100000) / 100),
                    new BooleanEntry('deleted', false),
                    new DateTimeEntry('created-at', $createdAt = new \DateTimeImmutable()),
                    new NullEntry('phase'),
                    new IntegerEntry('status', 0),
                    new ArrayEntry(
                        'array',
                        [
                            ['id' => 1, 'status' => 'NEW'],
                            ['id' => 2, 'status' => 'PENDING'],
                        ]
                    ),
                    new StructureEntry(
                        'items',
                        new IntegerEntry('item-id', 1),
                        new StringEntry('name', 'one'),
                    ),
                    new Row\Entry\CollectionEntry(
                        'tags',
                        new Row\Entries(new IntegerEntry('item-id', 1), new StringEntry('name', 'one')),
                        new Row\Entries(new IntegerEntry('item-id', 2), new StringEntry('name', 'two')),
                        new Row\Entries(new IntegerEntry('item-id', 3), new StringEntry('name', 'three'))
                    ),
                    new Row\Entry\ObjectEntry('object', new \ArrayIterator([1, 2, 3]))
                );
            }

            \shuffle($rows);

            yield new Rows(...$rows);
        }
    }
}
