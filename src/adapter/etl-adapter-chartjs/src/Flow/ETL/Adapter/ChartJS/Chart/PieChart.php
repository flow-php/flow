<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Chart;

use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Row\{Reference, References};
use Flow\ETL\Rows;

final class PieChart implements Chart
{
    /**
     * @var array{
     *   datasets: array<string, array{data: array<mixed>, label: ?string}>
     * }
     */
    private array $data = [
        'datasets' => [],
    ];

    /**
     * @var array<array-key, mixed>
     */
    private array $datasetOptions = [];

    /**
     * @var array<array-key, mixed>
     */
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
                        'label' => \is_scalar($row->valueOf($this->label)) ? (string) $row->valueOf($this->label) : '',
                    ];
                } else {
                    $this->data['datasets']['pie']['data'][] = $row->valueOf($dataset);
                    $labelValue = $row->valueOf($this->label);
                    $this->data['datasets']['pie']['label'] = \is_scalar($labelValue) ? (string) $labelValue : '';
                }
            }
        }
    }

    /**
     * @return array<array-key, mixed>
     */
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

    /**
     * @param array<array-key, mixed> $options
     */
    public function setDatasetOptions(Reference $dataset, array $options) : self
    {
        $this->datasetOptions[$dataset->name()] = $options;

        return $this;
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function setOptions(array $options) : self
    {
        $this->options = $options;

        return $this;
    }
}
