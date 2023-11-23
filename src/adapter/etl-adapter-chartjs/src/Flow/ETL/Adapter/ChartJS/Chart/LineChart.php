<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Chart;

use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;

final class LineChart implements Chart
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

    /**
     * @param Reference $label
     * @param array<Reference> $datasets
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly Reference $label,
        private readonly array $datasets,
    ) {
        if (!\count($this->datasets)) {
            throw new InvalidArgumentException('Bar chart must have at least one dataset, please provide at least one entry reference');
        }

        foreach ($this->datasets as $dataset) {
            $this->datasetOptions[$dataset->name()] = [];
        }
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
            'type' => 'line',
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
