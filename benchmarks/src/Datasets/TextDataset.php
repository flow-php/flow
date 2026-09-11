<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Datasets;

use Stringable;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Text\to_text;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;

final readonly class TextDataset
{
    public function __construct(
        private int $rows,
    ) {}

    public function path(): string
    {
        $fixture = new FixturePath('orders', $this->rows, FixtureFormat::text);

        if ($fixture->exists()) {
            return $fixture->path();
        }

        $fixture->prune();

        $parquetPath = (new OrdersDataset($this->rows))->parquet();
        $lines = [];

        foreach (data_frame()->read(from_parquet($parquetPath)->withColumns(['customer', 'notes']))->get() as $batch) {
            foreach ($batch->all() as $row) {
                $customer = $row->get('customer');
                $notes = $row->get('notes');

                $customerText = is_scalar($customer) || $customer instanceof Stringable ? (string) $customer : '';
                $notesText = is_array($notes)
                    ? implode(' ', array_map(static fn(mixed $note): string => is_scalar($note)
                        || $note instanceof Stringable
                            ? (string) $note
                            : '', $notes))
                    : '';

                $lines[] = ['text' => $customerText . ': ' . $notesText];
            }
        }

        data_frame()->read(from_array($lines))->write(to_text($fixture->path()))->run();

        return $fixture->path();
    }
}
