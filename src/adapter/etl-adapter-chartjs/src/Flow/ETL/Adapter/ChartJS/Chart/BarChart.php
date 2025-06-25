<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Chart;

use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Row\{Reference, References};
use Flow\ETL\Rows;

final class BarChart implements Chart
{
    /**
     * @var array{
     *   labels: array<string>,
     *   datasets: array<string, array{label: string, data: array<mixed>}>
     * }
     */
    private array $data = [
        'labels' => [],
        'datasets' => [],
    ];

    /**
     * @var array<string, array<array-key, mixed>>
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
            $labelValue = $row->valueOf($this->label);
            $this->data['labels'][] = \is_scalar($labelValue) ? (string) $labelValue : '';

            foreach ($this->datasets as $dataset) {
                if (!\array_key_exists($dataset->name(), $this->data['datasets'])) {
                    $this->data['datasets'][$dataset->name()] = [
                        'label' => $dataset->name(),
                        'data' => [$row->valueOf($dataset)],
                    ];
                } else {
                    $this->data['datasets'][$dataset->name()]['data'][] = $row->valueOf($dataset);
                }
            }
        }
    }

    /**
     * @return array<array-key, mixed>
     */
    public function data() : array
    {
        $data = [
            'type' => 'bar',
            'data' => [
                'labels' => $this->data['labels'],
                'datasets' => \array_values(\array_map(
                    function (array $dataset) : array {
                        /** @var array<array-key, mixed> $options */
                        $options = $this->datasetOptions[$dataset['label']] ?? [];

                        return \array_merge($dataset, $options);
                    },
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
