<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

use Stringable;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;

final readonly class SellersDataset
{
    public function __construct(
        private int $rows,
    ) {}

    public function parquet(): string
    {
        $fixture = new FixturePath('sellers', $this->rows, FixtureFormat::parquet);

        if ($fixture->exists()) {
            return $fixture->path();
        }

        $fixture->prune();

        $ordersPath = (new OrdersDataset($this->rows))->parquet();
        $sellerIds = [];

        foreach (data_frame()->read(from_parquet($ordersPath)->withColumns(['seller_id']))->get() as $batch) {
            foreach ($batch->all() as $row) {
                $sellerId = $row->get('seller_id');

                if (is_scalar($sellerId) || $sellerId instanceof Stringable) {
                    $sellerIds[(string) $sellerId] = true;
                }
            }
        }

        $sellers = [];
        $index = 1;

        foreach (array_keys($sellerIds) as $sellerId) {
            $sellers[] = [
                'id' => $sellerId,
                'name' => 'Seller ' . $index,
                'country' => ['PL', 'DE', 'FR', 'US', 'GB'][($index - 1) % 5],
                'commission_rate' => round(0.05 + ((($index - 1) % 10) * 0.01), 2),
            ];
            $index++;
        }

        data_frame()->read(from_array($sellers))->write(to_parquet($fixture->path()))->run();

        return $fixture->path();
    }
}
