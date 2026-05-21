<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use DateTimeImmutable;
use Faker\Factory;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Schema;
use Generator;

use function array_map;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\list_schema;
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
use function range;

final readonly class FakeRandomOrdersExtractor implements Extractor
{
    public function __construct(
        private int $count = 1_000,
    ) {}

    public static function schema(): Schema
    {
        return schema(
            uuid_schema('order_id'),
            uuid_schema('seller_id'),
            datetime_schema('created_at'),
            datetime_schema('updated_at', true),
            datetime_schema('cancelled_at', true),
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
        $faker = Factory::create();

        $skus = [
            ['sku' => 'SKU_0001', 'name' => 'Product 1', 'price' => $faker->randomFloat(2, 0, 500)],
            ['sku' => 'SKU_0002', 'name' => 'Product 2', 'price' => $faker->randomFloat(2, 0, 500)],
            ['sku' => 'SKU_0003', 'name' => 'Product 3', 'price' => $faker->randomFloat(2, 0, 500)],
            ['sku' => 'SKU_0004', 'name' => 'Product 4', 'price' => $faker->randomFloat(2, 0, 500)],
            ['sku' => 'SKU_0005', 'name' => 'Product 5', 'price' => $faker->randomFloat(2, 0, 500)],
        ];

        $sellers = [
            $faker->uuid,
            $faker->uuid,
            $faker->uuid,
            $faker->uuid,
            $faker->uuid,
        ];

        for ($i = 0; $i < $this->count; $i++) {
            $createdAt = DateTimeImmutable::createFromMutable($faker->dateTimeThisYear);
            $cancelledAt = random_int(1, 10) === 1
                ? $createdAt->modify('+' . $faker->numberBetween(1, 5) . ' hours')
                : null;

            if ($cancelledAt) {
                $updatedAt = $cancelledAt;
            } else {
                $updatedAt = random_int(1, 3) === 1
                    ? $createdAt->modify('+' . $faker->numberBetween(1, 3) . ' days')
                    : null;
            }

            // @mago-ignore analysis:mixed-assignment
            $signal = yield [
                'order_id' => $faker->uuid,
                'seller_id' => $sellers[random_int(0, count($sellers) - 1)],
                'created_at' => $createdAt,
                'updated_at' => $updatedAt,
                'cancelled_at' => $cancelledAt,
                'discount' => random_int(0, 1) === 1 ? $faker->randomFloat(2, 0, 50) : null,
                'email' => $faker->email,
                'customer' => $faker->firstName . ' ' . $faker->lastName,
                'address' => [
                    'street' => $faker->streetAddress,
                    'city' => $faker->city,
                    'zip' => $faker->postcode,
                    'country' => $faker->country,
                ],
                'notes' => array_map(static fn($i) => $faker->sentence, range(1, $faker->numberBetween(1, 5))),
                'items' => array_map(
                    static fn(int $index) => [
                        'sku' => $skus[$skuIndex = $faker->numberBetween(1, 4)]['sku'],
                        'quantity' => $faker->numberBetween(1, 10),
                        'price' => $skus[$skuIndex]['price'],
                    ],
                    range(1, $faker->numberBetween(1, 4)),
                ),
            ];

            if ($signal === Signal::STOP) {
                return;
            }
        }
    }
}
