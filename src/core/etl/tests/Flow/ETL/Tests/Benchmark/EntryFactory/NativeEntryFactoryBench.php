<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Benchmark\EntryFactory;

use function Flow\ETL\DSL\array_to_rows;
use Faker\Factory;
use Flow\ETL\Row\Factory\NativeEntryFactory;
use PhpBench\Attributes\BeforeMethods;
use PhpBench\Attributes\Groups;
use PhpBench\Attributes\Iterations;
use PhpBench\Attributes\Revs;

#[Iterations(5)]
#[Groups(['building_blocks'])]
final class NativeEntryFactoryBench
{
    private array $rowsArray = [];

    public function setUp() : void
    {
        $faker = Factory::create();

        $this->rowsArray = \array_map(
            static fn (int $i) : array => [
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
                    \range(1, $faker->numberBetween(1, 5))
                ),
            ],
            \range(1, 10_000)
        );
    }

    #[BeforeMethods(['setUp'])]
    #[Revs(5)]
    public function bench_10k_rows() : void
    {
        array_to_rows($this->rowsArray, new NativeEntryFactory());
    }

    #[BeforeMethods(['setUp'])]
    #[Revs(5)]
    public function bench_1k_rows() : void
    {
        array_to_rows(\array_slice($this->rowsArray, 0, 1_000), new NativeEntryFactory());
    }

    #[BeforeMethods(['setUp'])]
    #[Revs(5)]
    public function bench_5k_rows() : void
    {
        array_to_rows(\array_slice($this->rowsArray, 0, 5_000), new NativeEntryFactory());
    }
}
