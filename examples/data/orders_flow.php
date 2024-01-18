<?php declare(strict_types=1);

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_avro;
use function Flow\ETL\DSL\to_csv;
use function Flow\ETL\DSL\to_json;
use function Flow\ETL\DSL\to_parquet;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;

include __DIR__ . '/../../vendor/autoload.php';

$faker = Faker\Factory::create();
$orders = \array_map(
    static fn (int $i) : array => [
        'order_id' => $faker->uuid,
        'created_at' => $faker->dateTimeThisYear->format(DateTimeInterface::RFC3339),
        'updated_at' => $faker->dateTimeThisMonth->format(DateTimeInterface::RFC3339),
        'cancelled_at' => $faker->optional(0.1)->dateTimeThisMonth?->format(DateTimeInterface::RFC3339),
        'total_price' => $faker->randomFloat(2, 0, 500),
        'discount' => $faker->randomFloat(2, 0, 50),
        'customer' => [
            'name' => $faker->firstName,
            'last_name' => $faker->lastName,
            'email' => $faker->email,
            'has_account' => $faker->boolean(30),
        ],
        'address' => [
            'street' => $faker->streetAddress,
            'city' => $faker->city,
            'zip' => $faker->postcode,
            'country' => $faker->country,
            'location' => ['latitude' => $faker->latitude, 'longitude' => $faker->longitude],
        ],
        'notes' => \array_map(
            static fn ($i) => $faker->sentence,
            \range(1, $faker->numberBetween(1, 5))
        ),
    ],
    \range(1, 10_000)
);

(new Flow())
    ->read(from_array($orders))
    ->mode(SaveMode::Overwrite)
    ->write(to_parquet(__DIR__ . '/orders_flow.parquet'))
    ->write(to_json(__DIR__ . '/orders_flow.json'))
    ->write(to_avro(__DIR__ . '/orders_flow.avro'))
    ->withEntry('order_id', ref('order_id')->cast('string'))
    ->withEntry('customer', ref('customer')->cast('string'))
    ->withEntry('address', ref('customer')->cast('string'))
    ->withEntry('notes', ref('customer')->cast('string'))
    ->write(to_csv(__DIR__ . '/orders_flow.csv'))
    ->run();
