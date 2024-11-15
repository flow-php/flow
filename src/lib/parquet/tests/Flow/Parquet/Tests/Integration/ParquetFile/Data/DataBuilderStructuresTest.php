<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile\Data;

use Flow\Dremel\{Dremel, Repetition};
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, NestedColumn};
use PHPUnit\Framework\TestCase;

final class DataBuilderStructuresTest extends TestCase
{
    public function test_optional_struct__optional_struct__optional_string____() : void
    {
        $schema = Schema::with(
            NestedColumn::struct(
                'struct',
                [
                    NestedColumn::struct(
                        'struct_01',
                        [
                            FlatColumn::string('name'),
                        ]
                    ),
                ]
            )
        );

        $rows = [
            null,
            null,
            null,
            'Bob',
        ];

        $repetitions = \array_map(
            fn (Schema\Repetition $r) => $r->toDremel(),
            $schema->get('struct.struct_01.name')->repetitions()
        );

        self::assertEquals(
            [Repetition::OPTIONAL, Repetition::OPTIONAL, Repetition::OPTIONAL],
            $repetitions
        );

        $shreddedData = (new Dremel())->shred(
            $rows,
            $repetitions
        );

        self::assertEquals([0, 0, 0, 0], $shreddedData->repetitionLevels);
        self::assertEquals([0, 1, 2, 3], $shreddedData->definitionLevels);

        $assembledData = (new Dremel())->assemble(
            $shreddedData,
            $repetitions,
            $schema->get('struct.struct_01.name')->maxDefinitionsLevel()
        );

        self::assertEquals(
            $rows,
            $assembledData->rows
        );
    }
}
