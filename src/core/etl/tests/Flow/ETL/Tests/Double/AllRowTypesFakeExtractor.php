<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\PHP\Type\Logical\Map\MapKey;
use Flow\ETL\PHP\Type\Logical\Map\MapValue;
use Flow\ETL\PHP\Type\Logical\MapType;
use Flow\ETL\PHP\Type\Logical\Structure\StructureElement;
use Flow\ETL\PHP\Type\Logical\StructureType;
use Flow\ETL\PHP\Type\Native\ScalarType;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;

final class AllRowTypesFakeExtractor implements Extractor
{
    public function __construct(private readonly int $total, private readonly int $rowsSize)
    {
    }

    /**
     * @param FlowContext $context
     *
     * @return \Generator<int, Rows, mixed, void>
     */
    public function extract(FlowContext $context) : \Generator
    {
        for ($i = 0; $i < $this->total; $i++) {
            $id = $this->rowsSize * $i;

            $rows = [];

            for ($r = 0; $r < $this->rowsSize; $r++) {
                $rows[] = Row::create(
                    Entry::integer('id', $id + $r),
                    Entry::float('price', \random_int(100, 100000) / 100),
                    Entry::boolean('deleted', false),
                    Entry::datetime('created-at', new \DateTimeImmutable('now')),
                    Entry::null('phase'),
                    Entry::integer('status', 0),
                    Entry::array(
                        'array',
                        [
                            ['id' => 1, 'status' => 'NEW'],
                            ['id' => 2, 'status' => 'PENDING'],
                        ]
                    ),
                    Entry::list_of_int('list', [1, 2, 3]),
                    Entry::map(
                        'map',
                        ['NEW', 'PENDING'],
                        new MapType(MapKey::integer(), MapValue::string())
                    ),
                    Entry::structure(
                        'items',
                        ['item-id' => 1, 'name' => 'one'],
                        new StructureType(
                            new StructureElement('item-id', ScalarType::integer()),
                            new StructureElement('name', ScalarType::string())
                        )
                    ),
                    Entry::object('object', new \ArrayIterator([1, 2, 3])),
                    Entry::enum('enum', BackedStringEnum::three)
                );
            }

            \shuffle($rows);

            yield new Rows(...$rows);
        }
    }
}
