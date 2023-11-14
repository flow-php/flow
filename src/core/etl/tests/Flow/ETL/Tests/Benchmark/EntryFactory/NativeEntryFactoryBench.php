<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Benchmark\EntryFactory;

use function Flow\ETL\DSL\array_to_rows;
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
        $createdAt = (new \DateTimeImmutable('-10 day ago'))->format(\DateTimeInterface::RFC3339);
        $updatedAt = (new \DateTimeImmutable('+1 week'))->format(\DateTimeInterface::RFC3339);

        $callback = static fn (int $i) : array => [
            'order_id' => '2d76fb83-c1a7-4b6e-9d68-46258af783b4',
            'created_at' => $createdAt,
            'updated_at' => $updatedAt,
            'active' => true,
            'total_price' => 123.45,
            'discount' => 5.5,
            'customer' => [
                'name' => 'firstName',
                'last_name' => 'lastName',
                'email' => 'foo@bar.test',
            ],
            'address' => [
                'street' => 'streetAddress',
                'city' => 'city',
                'zip' => '12-345',
                'country' => 'country',
                'location' => [
                    'lat' => 66.6,
                    'lng' => 33.3,
                ],
            ],
            'notes' => "Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.",
        ];

        yield '10k' => ['rows' => \array_map($callback, \range(1, 10_000))];

        yield '5k' => ['rows' => \array_map($callback, \range(1, 5000))];

        yield '1k' => ['rows' => \array_map($callback, \range(1, 1000))];
    }
}
