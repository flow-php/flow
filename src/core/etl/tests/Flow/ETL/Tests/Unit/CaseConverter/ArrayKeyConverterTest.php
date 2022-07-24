<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\CaseConverter;

use Flow\ETL\Transformer\StyleConverter\ArrayKeyConverter;
use Jawira\CaseConverter\Convert;
use PHPUnit\Framework\TestCase;

final class ArrayKeyConverterTest extends TestCase
{
    public function test_converts_all_keys_to_snake_case() : void
    {
        $transformer = new ArrayKeyConverter(
            fn (string $key) : string => (new Convert($key))->toSnake()
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
            $transformer->convert([
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
            ]),
        );
    }
}
