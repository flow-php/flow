<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Benchmark\EntryFactory;

use function Flow\ETL\DSL\array_to_rows;
use Faker\Factory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\ParamProviders;

#[Groups(['building_blocks'])]
final class NativeEntryFactoryBench
{
    #[ParamProviders('provideRows')]
    public function bench_entry_factory(array $params) : void
    {
        array_to_rows($params['rows'], new NativeEntryFactory());
    }

    public function provideRows() : \Generator
    {
        $faker = Factory::create();

        $callback = static fn (int $i) : array => [
            'order_id' => $faker->uuid,
            'created_at' => $faker->dateTimeThisYear->format(\DateTimeInterface::RFC3339),
            'updated_at' => $faker->dateTimeThisMonth->format(\DateTimeInterface::RFC3339),
            'cancelled_at' => ($i % 10) === 0 ? $faker->dateTimeThisMonth->format(\DateTimeInterface::RFC3339) : null,
            'active' => !(($i % 20) === 0),
            'total_price' => $faker->randomFloat(2, 0, 500),
            'discount' => $faker->randomFloat(2, 0, 50),
            'customer' => [
                'name' => $faker->firstName,
                'last_name' => $faker->lastName,
                'email' => $faker->email,
            ],
            'address' => [
                'street' => $faker->streetAddress,
                'city' => $faker->city,
                'zip' => $faker->postcode,
                'country' => $faker->country,
                'location' => [
                    'lat' => $faker->latitude,
                    'lng' => $faker->longitude,
                ],
            ],
            'notes' => \array_map(
                static fn ($i) => $faker->sentence,
                \range(1, 3)
            ),
        ];

        yield '10k' => ['rows' => \array_map($callback, \range(1, 10_000))];

        yield '5k' => ['rows' => \array_map($callback, \range(1, 5000))];

        yield '1k' => ['rows' => \array_map($callback, \range(1, 1000))];
    }
}
