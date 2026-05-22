<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use DateTimeImmutable;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\ETL\DSL\structure_schema;
use function Flow\ETL\DSL\uuid_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function random_int;

final readonly class FakeStaticOrdersExtractor implements Extractor
{
    public function __construct(
        private int $count = 1_000,
    ) {}

    public static function schema(): Schema
    {
        return schema(
            integer_schema('index'),
            uuid_schema('order_id'),
            datetime_schema('created_at'),
            datetime_schema('updated_at', true),
            float_schema('discount', true),
            string_schema('email'),
            string_schema('customer'),
            structure_schema('address', type_structure([
                'street' => type_string(),
                'city' => type_string(),
                'zip' => type_string(),
                'country' => type_string(),
            ])),
            list_schema('notes', type_list(type_string())),
            list_schema(
                'items',
                type_list(type_structure([
                    'sku' => type_string(),
                    'quantity' => type_integer(),
                    'price' => type_float(),
                ])),
            ),
        );
    }

    public function extract(FlowContext $context): Generator
    {
        foreach ($this->rawData() as $row) {
            yield array_to_rows($row, $context->entryFactory(), schema: self::schema());
        }
    }

    /**
     * @return \Generator<array<string, mixed>>
     */
    public function rawData(): Generator
    {
        $skus = [
            ['sku' => 'SKU_0001', 'name' => 'Product 1', 'price' => 0.14],
            ['sku' => 'SKU_0002', 'name' => 'Product 2', 'price' => 25.13],
            ['sku' => 'SKU_0003', 'name' => 'Product 3', 'price' => 128.92],
            ['sku' => 'SKU_0004', 'name' => 'Product 4', 'price' => 99.99],
            ['sku' => 'SKU_0005', 'name' => 'Product 5', 'price' => 145.00],
        ];

        for ($i = 0; $i < $this->count; $i++) {
            // @mago-ignore analysis:mixed-assignment
            $signal = yield [
                'index' => $i,
                'order_id' => '254d61c5-22c8-4407-83a2-76f1cab53af2',
                'created_at' => new DateTimeImmutable('2025-01-01 12:00:00'),
                'updated_at' => random_int(0, 1) === 1 ? new DateTimeImmutable('2025-01-01 12:10:00') : null,
                'discount' => random_int(0, 1) === 1 ? 24.4 : null,
                'email' => 'user-' . $i . '@example.com',
                'customer' => 'John Doe ' . $i,
                'address' => [
                    'street' => '123 Main St, Apt ' . $i,
                    'city' => 'City ',
                    'zip' => '12345-' . $i,
                    'country' => 'PL',
                ],
                'notes' => [
                    'Note 1 for order ' . $i,
                    'Note 2 for order ' . $i,
                    'Note 3 for order ' . $i,
                ],
                'items' => [
                    [
                        'sku' => $skus[0]['sku'],
                        'quantity' => 1,
                        'price' => $skus[0]['price'],
                    ],
                    [
                        'sku' => $skus[1]['sku'],
                        'quantity' => 2,
                        'price' => $skus[1]['price'],
                    ],
                ],
            ];

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }

    public function toRows(EntryFactory $entryFactory = new EntryFactory()): Rows
    {
        $rows = rows();

        foreach ($this->rawData() as $row) {
            $rows = $rows->merge(array_to_rows($row, entryFactory: $entryFactory, schema: self::schema()));
        }

        return $rows;
    }
}
