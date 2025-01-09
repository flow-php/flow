<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageSizeCalculator;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, NestedColumn};
use Flow\Parquet\ParquetFile\{Compressions, RowGroupBuilder, Schema};
use Flow\Parquet\{Options};
use PHPUnit\Framework\TestCase;

final class RowGroupBuilderStructuresTest extends TestCase
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

        $builder = new RowGroupBuilder(
            $schema,
            Compressions::UNCOMPRESSED,
            $options = Options::default(),
            DataConverter::initialize($options),
            new PageSizeCalculator($options)
        );

        $builder->addRow([
            'struct' => null,
        ]);
        $builder->addRow([
            'struct' => [
                'struct_01' => null,
            ],
        ]);
        $builder->addRow([
            'struct' => [
                'struct_01' => [
                    'name' => null,
                ],
            ],
        ]);
        $builder->addRow([
            'struct' => [
                'struct_01' => [
                    'name' => 'Alice',
                ],
            ],
        ]);

        $flatColumnName = $builder->chunkBuilders()['struct.struct_01.name']->rows();
        self::assertEquals(4, $flatColumnName->rowsCount());
        self::assertEquals(3, $flatColumnName->nullCount());
    }
}
