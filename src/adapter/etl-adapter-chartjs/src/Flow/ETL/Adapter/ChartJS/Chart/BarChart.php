<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Chart;

use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row\EntryReference;
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

    private array $options = [];

    /**
     * @param EntryReference $label
     * @param array<EntryReference> $datasets
     *
     * @throws InvalidArgumentException
     */
    public function __construct(
        private readonly EntryReference $label,
        private readonly array $datasets,
    ) {
        if (!\count($this->datasets)) {
            throw new InvalidArgumentException('Bar chart must have at least one dataset, please provide at least one entry reference');
        }

        foreach ($this->datasets as $dataset) {
            $this->options[$dataset->name()] = [];
        }
    }

    public function collect(Rows $rows) : void
    {
        foreach ($rows as $row) {
            /** @phpstan-ignore-next-line */
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
        return [
            'type' => 'bar',
            'data' => [
                'labels' => $this->data['labels'],
                'datasets' => \array_values(\array_map(
                    function (array $dataset) : array {
                        /** @var array<array-key, mixed> $options */
                        $options = $this->options[$dataset['label']] ?? [];

                        return \array_merge($dataset, $options);
                    },
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
        if (!\array_key_exists($dataset->name(), $this->options)) {
            throw new InvalidArgumentException(\sprintf('Dataset "%s" does not exists', $dataset->name()));
        }

        $this->options[$dataset->name()] = $options;

        return $this;
    }

    public function setOptions(array $options) : self
    {
        throw new RuntimeException('Please use setDatasetOptions while using BarChart');
    }
}
