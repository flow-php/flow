<?php

declare(strict_types=1);

namespace Flow\CLI\Formatter;

use function Flow\CLI\option_bool;
use Flow\ETL\Dataset\Report;
use Symfony\Component\Console\Helper\TableSeparator;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

final readonly class PipelineReportFormatter
{
    public function __construct(private Report $report, private SymfonyStyle $style, private InputInterface $input)
    {

    }

    public function format() : void
    {
        $schema = $this->report->schema();

        if (option_bool('stats-schema', $this->input) && $schema) {
            $this->style->newLine();
            $this->style->section('Schema');

            $normalizedSchema = [];

            foreach ($schema->definitions() as $definition) {
                $normalizedSchema[] = [
                    'name' => $definition->entry()->name(),
                    'type' => $definition->type()->toString(),
                    'nullable' => $definition->isNullable() ? 'true' : 'false',
                    'metadata' => $definition->metadata() !== null ? json_encode($definition->metadata(), JSON_PRETTY_PRINT) : null,
                ];
            }

            $this->style->createTable()
                ->setHeaders(['Name', 'Type', 'Nullable', 'Metadata'])
                ->setRows($normalizedSchema)
                ->setStyle('box')
                ->render();
        }

        $columnsStatistics = $this->report->statistics()->columns;

        if (option_bool('stats-columns', $this->input) && $columnsStatistics) {
            $normalizedColumnStatistics = [];

            $valueFormatter = new ValueFormatter();

            foreach ($columnsStatistics->all() as $columnStatistics) {
                $normalizedColumnStatistics[] = [
                    'name' => $columnStatistics->name(),
                    'type' => $columnStatistics->type()->toString(),
                    'nulls_count' => $valueFormatter->format($columnStatistics->nullCount()),
                    'distinct_count' => $valueFormatter->format($columnStatistics->distinctCount()),
                    'min' => $valueFormatter->format($columnStatistics->min()),
                    'max' => $valueFormatter->format($columnStatistics->max()),
                    'min_length' => $valueFormatter->format($columnStatistics->minLength()),
                    'max_length' => $valueFormatter->format($columnStatistics->maxLength()),
                    'min_elements_count' => $valueFormatter->format($columnStatistics->minElementsCount()),
                    'max_elements_count' => $valueFormatter->format($columnStatistics->maxElementsCount()),
                ];
            }

            $this->style->newLine();
            $this->style->section('Columns');

            $this->style->createTable()
                ->setHeaders(['Name', 'Type', 'Nulls', 'Distinct Values', 'Min', 'Max', 'Min Length', 'Max Length', 'Min Elements Count', 'Max Elements Count'])
                ->setRows($normalizedColumnStatistics)
                ->setStyle('box')
                ->render();
        }

        $this->style->newLine();
        $this->style->definitionList(
            'Statistics',
            new TableSeparator(),
            ['Analyzed Rows' => \number_format($this->report->statistics()->totalRows())],
            ['Execution Time' => $this->report->statistics()->executionTime->highResolutionTime->toString()]
        );
    }
}
