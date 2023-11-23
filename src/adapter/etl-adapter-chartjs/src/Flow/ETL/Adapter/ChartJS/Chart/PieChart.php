<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Chart;

use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;

final class PieChart implements Chart
{
    /**
     * @var array{
     *   datasets: array<string, array{data: array, label: ?string}>
     * }
     */
    private array $data = [
        'datasets' => [],
    ];

    /**
     * @var array<array-key, mixed>
     */
    private array $datasetOptions = [];

    private array $options = [];

    public function __construct(
        private readonly Reference $label,
        private readonly References $datasets,
    ) {
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
        $labels = [];

        foreach ($this->datasets as $dataset) {
            $labels[] = $dataset->name();
        }

        /** @var array<array-key, mixed> $options */
        $options = $this->datasetOptions['pie'] ?? [];

        $data = [
            'type' => 'pie',
            'data' => [
                'labels' => $labels,
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

    public function setDatasetOptions(Reference $dataset, array $options) : self
    {
        $this->datasetOptions[$dataset->name()] = $options;

        return $this;
    }

    public function setOptions(array $options) : self
    {
        $this->options = $options;

        return $this;
    }
}
