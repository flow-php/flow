<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use function Flow\ETL\DSL\{array_to_rows, datetime_schema, float_schema, list_schema, schema, string_schema, structure_schema, uuid_schema};
use function Flow\Types\DSL\{type_float, type_integer, type_list, type_string, type_structure};
use Faker\Factory;
use Flow\ETL\{Extractor, FlowContext, Schema};

final readonly class FakeRandomOrdersExtractor implements Extractor
{
    public function __construct(private int $count = 1_000)
    {
    }

    public static function schema() : Schema
    {
        return schema(
            uuid_schema('order_id'),
            datetime_schema('created_at'),
            datetime_schema('updated_at', true),
            float_schema('discount', true),
            string_schema('email'),
            string_schema('customer'),
            structure_schema(
                'address',
                type_structure([
                    'street' => type_string(),
                    'city' => type_string(),
                    'zip' => type_string(),
                    'country' => type_string(),
                ])
            ),
            list_schema('notes', type_list(type_string())),
            list_schema('items', type_list(
                type_structure([
                    'sku' => type_string(),
                    'quantity' => type_integer(),
                    'price' => type_float(),
                ])
            ))
        );
    }

    public function extract(FlowContext $context) : \Generator
    {
        $faker = Factory::create();

        $skus = [
            ['sku' => 'SKU_0001', 'name' => 'Product 1', 'price' => $faker->randomFloat(2, 0, 500)],
            ['sku' => 'SKU_0002', 'name' => 'Product 2', 'price' => $faker->randomFloat(2, 0, 500)],
            ['sku' => 'SKU_0003', 'name' => 'Product 3', 'price' => $faker->randomFloat(2, 0, 500)],
            ['sku' => 'SKU_0004', 'name' => 'Product 4', 'price' => $faker->randomFloat(2, 0, 500)],
            ['sku' => 'SKU_0005', 'name' => 'Product 5', 'price' => $faker->randomFloat(2, 0, 500)],
        ];

        for ($i = 0; $i < $this->count; $i++) {
            yield array_to_rows(
                [
                    'order_id' => $faker->uuid,
                    'created_at' => $faker->dateTimeThisYear,
                    'updated_at' => \random_int(0, 1) === 1 ? $faker->dateTimeThisMonth : null,
                    'discount' => \random_int(0, 1) === 1 ? $faker->randomFloat(2, 0, 50) : null,
                    'email' => $faker->email,
                    'customer' => $faker->firstName . ' ' . $faker->lastName,
                    'address' => [
                        'street' => $faker->streetAddress,
                        'city' => $faker->city,
                        'zip' => $faker->postcode,
                        'country' => $faker->country,
                    ],
                    'notes' => \array_map(
                        static fn ($i) => $faker->sentence,
                        \range(1, $faker->numberBetween(1, 5))
                    ),
                    'items' => \array_map(
                        static fn (int $index) => [
                            'sku' => $skus[$skuIndex = $faker->numberBetween(1, 4)]['sku'],
                            'quantity' => $faker->numberBetween(1, 10),
                            'price' => $skus[$skuIndex]['price'],
                        ],
                        \range(1, $faker->numberBetween(1, 4))
                    ),
                ],
                schema: self::schema()
            );
        }
    }
}
