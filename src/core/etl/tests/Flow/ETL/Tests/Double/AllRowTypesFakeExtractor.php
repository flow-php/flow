<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\enum_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\map_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\object_entry;
use function Flow\ETL\DSL\struct_element;
use function Flow\ETL\DSL\struct_entry;
use function Flow\ETL\DSL\struct_type;
use function Flow\ETL\DSL\type_int;
use function Flow\ETL\DSL\type_list;
use function Flow\ETL\DSL\type_map;
use function Flow\ETL\DSL\type_string;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
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
                    int_entry('id', $id + $r),
                    float_entry('price', \random_int(100, 100000) / 100),
                    bool_entry('deleted', false),
                    datetime_entry('created-at', new \DateTimeImmutable('now')),
                    null_entry('phase'),
                    int_entry('status', 0),
                    array_entry(
                        'array',
                        [
                            ['id' => 1, 'status' => 'NEW'],
                            ['id' => 2, 'status' => 'PENDING'],
                        ]
                    ),
                    list_entry('list', [1, 2, 3], type_list(type_int())),
                    map_entry(
                        'map',
                        ['NEW', 'PENDING'],
                        type_map(type_int(), type_string())
                    ),
                    struct_entry(
                        'items',
                        ['item-id' => 1, 'name' => 'one'],
                        struct_type(
                            struct_element('item-id', type_int()),
                            struct_element('name', type_string())
                        )
                    ),
                    object_entry('object', new \ArrayIterator([1, 2, 3])),
                    enum_entry('enum', BackedStringEnum::three)
                );
            }

            \shuffle($rows);

            yield new Rows(...$rows);
        }
    }
}
