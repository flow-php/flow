<?php

declare(strict_types=1);

function generateOrders(int $count) : Generator
{
    $faker = Faker\Factory::create();

    $skus = [
        ['sku' => 'SKU_0001', 'name' => 'Product 1', 'price' => $faker->randomFloat(2, 0, 500)],
        ['sku' => 'SKU_0002', 'name' => 'Product 2', 'price' => $faker->randomFloat(2, 0, 500)],
        ['sku' => 'SKU_0003', 'name' => 'Product 3', 'price' => $faker->randomFloat(2, 0, 500)],
        ['sku' => 'SKU_0004', 'name' => 'Product 4', 'price' => $faker->randomFloat(2, 0, 500)],
        ['sku' => 'SKU_0005', 'name' => 'Product 5', 'price' => $faker->randomFloat(2, 0, 500)],
    ];

    for ($i = 0; $i < $count; $i++) {
        yield [
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
        ];
    }
}
