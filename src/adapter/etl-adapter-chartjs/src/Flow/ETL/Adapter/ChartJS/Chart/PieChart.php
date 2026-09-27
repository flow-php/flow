<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Chart;

use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Rows;

use function array_map;
use function array_merge;
use function array_values;
use function is_scalar;

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
    ) {}

    public function collect(Rows $rows): void
    {
        if ($rows->count() === 0) {
            return;
        }

        $label = $rows->column($this->label->base())->values();
        $columns = [];

        foreach ($this->datasets as $dataset) {
            $columns[$dataset->name()] = $rows->column($dataset->base())->values();
        }

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            foreach ($this->datasets as $dataset) {
                $this->data['datasets']['pie']['data'][] = $columns[$dataset->name()][$i];
                $this->data['datasets']['pie']['label'] = is_scalar($label[$i]) ? (string) $label[$i] : '';
            }
        }
    }

    /**
     * @return array<array-key, mixed>
     */
    public function data(): array
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
                'datasets' => array_values(array_map(static fn(array $dataset): array => array_merge(
                    $dataset,
                    $options,
                ), $this->data['datasets'])),
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
    public function setDatasetOptions(Reference $dataset, array $options): self
    {
        $this->datasetOptions[$dataset->name()] = $options;

        return $this;
    }

    /**
     * @param array<array-key, mixed> $options
     */
    public function setOptions(array $options): self
    {
        $this->options = $options;

        return $this;
    }
}
