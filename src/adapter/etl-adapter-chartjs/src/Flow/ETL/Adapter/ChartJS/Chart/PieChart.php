<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Chart;

use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;

final class PieChart implements Chart
{
    /**
     * @var array{
     *   labels: array<string>,
     *   datasets: array<string, array{data: array, label: ?string}>
     * }
     */
    private array $data = [
        'labels' => [],
        'datasets' => [],
    ];

    /**
     * @var array<array-key, mixed>
     */
    private array $datasetOptions = [];

    private array $options = [];

    /**
     * @param array<Reference> $datasets
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly Reference $label,
        private readonly array|References $datasets,
    ) {
        if (!\count($this->datasets)) {
            throw new InvalidArgumentException('Bar chart must have at least one dataset, please provide at least one entry reference');
        }

        foreach ($this->datasets as $dataset) {
            $this->datasetOptions[$dataset->name()] = [];
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
                        'label' => (string) $row->valueOf($this->label),
                    ];
                } else {
                    $this->data['datasets']['pie']['data'][] = $row->valueOf($dataset);
                    $this->data['datasets']['pie']['label'] = (string) $row->valueOf($this->label);
                }
            }
        }
    }

    public function data() : array
    {
        /** @var array<array-key, mixed> $options */
        $options = $this->datasetOptions['pie'] ?? [];

        $data = [
            'type' => 'pie',
            'data' => [
                'labels' => $this->data['labels'],
                'datasets' => \array_values(\array_map(
                    fn (array $dataset) : array => \array_merge($dataset, $options),
                    $this->data['datasets']
                )),
            ],
        ];

        if ($this->options) {
            $data['options'] = $this->options;
        }

        return $data;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function setDatasetOptions(Reference $dataset, array $options) : self
    {
        if (!\array_key_exists($dataset->name(), $this->datasetOptions)) {
            throw new InvalidArgumentException(\sprintf('Dataset "%s" does not exists', $dataset->name()));
        }

        $this->datasetOptions[$dataset->name()] = $options;

        return $this;
    }

    public function setOptions(array $options) : self
    {
        $this->options = $options;

        return $this;
    }
}
