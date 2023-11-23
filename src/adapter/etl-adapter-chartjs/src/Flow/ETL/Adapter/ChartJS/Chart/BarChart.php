<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Chart;

use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;

final class BarChart implements Chart
{
    /**
     * @var array{
     *   labels: array<string>,
     *   datasets: array<string, array{label: string, data: array}>
     * }
     */
    private array $data = [
        'labels' => [],
        'datasets' => [],
    ];

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
            $this->data['labels'][] = (string) $row->valueOf($this->label);

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
