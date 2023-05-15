<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Chart;

use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Rows;

final class PieChart implements Chart
{
    /**
     * @var array{
     *   labels: array<string>,
     *   datasets: array<string, array{data: array}>
     * }
     */
    private array $data = [
        'labels' => [],
        'datasets' => [],
    ];

    /**
     * @var array<array-key, mixed>
     */
    private array $options = [];

    /**
     * @param array<EntryReference> $datasets
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly array $datasets,
    ) {
        if (!\count($this->datasets)) {
            throw new InvalidArgumentException('Bar chart must have at least one dataset, please provide at least one entry reference');
        }

        foreach ($this->datasets as $dataset) {
            $this->options[$dataset->name()] = [];
            $this->data['labels'][] = $dataset->name();
        }
    }

    public function collect(Rows $rows) : void
    {
        foreach ($rows as $row) {
            foreach ($this->datasets as $dataset) {
                if (!\array_key_exists('pie', $this->data['datasets'])) {
                    $this->data['datasets']['pie'] = [
                        'data' => [$row->valueOf($dataset)],
                    ];
                } else {
                    $this->data['datasets']['pie']['data'][] = $row->valueOf($dataset);
                }
            }
        }
    }

    public function data() : array
    {
        /** @var array<array-key, mixed> $options */
        $options = $this->options['pie'] ?? [];

        return [
            'type' => 'pie',
            'data' => [
                'labels' => $this->data['labels'],
                'datasets' => \array_values(\array_map(
                    fn (array $dataset) : array => \array_merge($dataset, $options),
                    $this->data['datasets']
                )),
            ],
        ];
    }

    /**
     * @throws InvalidArgumentException
     */
    public function setDatasetOptions(EntryReference $dataset, array $options) : self
    {
        throw new RuntimeException('Please use setOptions while using PieChart');
    }

    public function setOptions(array $options) : self
    {
        $this->options['pie'] = $options;

        return $this;
    }
}
