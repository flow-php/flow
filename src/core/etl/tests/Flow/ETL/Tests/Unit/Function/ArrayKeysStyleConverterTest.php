<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\array_keys_style_convert;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayKeysStyleConverterTest extends TestCase
{
    public function test_for_invalid_style() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unrecognized style invalid, please use one of following:');

        $row = Row::create(
            Entry::array('invalid_entry', []),
        );

        array_keys_style_convert(ref('invalid_entry'), 'invalid')->eval($row);
    }

    public function test_for_not_array_entry() : void
    {
        $row = Row::create(
            Entry::integer('invalid_entry', 1),
        );

        $this->assertNull(array_keys_style_convert(ref('invalid_entry'), 'snake')->eval($row));
    }

    public function test_transforms_case_style_for_all_keys_in_array_entry() : void
    {
        $row = Row::create(
            new Row\Entry\ArrayEntry(
                'arrayEntry',
                [
                    'itemId' => 1,
                    'itemStatus' => 'PENDING',
                    'itemEnabled' => true,
                    'itemVariants' => [
                        'variantStatuses' => [
                            [
                                'statusId' => 1000,
                                'statusName' => 'NEW',
                            ],
                            [
                                'statusId' => 2000,
                                'statusName' => 'ACTIVE',
                            ],
                        ],
                        'variantName' => 'Variant Name',
                    ],
                ],
            )
        );

        $this->assertEquals(
            [
                'item_id' => 1,
                'item_status' => 'PENDING',
                'item_enabled' => true,
                'item_variants' => [
                    'variant_statuses' => [
                        [
                            'status_id' => 1000,
                            'status_name' => 'NEW',
                        ],
                        [
                            'status_id' => 2000,
                            'status_name' => 'ACTIVE',
                        ],
                    ],
                    'variant_name' => 'Variant Name',
                ],
            ],
            array_keys_style_convert(ref('arrayEntry'), 'snake')->eval($row)
        );
    }
}
