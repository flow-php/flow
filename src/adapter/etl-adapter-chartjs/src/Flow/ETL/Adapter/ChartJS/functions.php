<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS;

use Flow\ETL\Adapter\ChartJS\Chart\{BarChart, LineChart, PieChart};
use Flow\ETL\Attribute\{DocumentationDSL, Module, Type};
use Flow\ETL\Row\{EntryReference, References};
use Flow\Filesystem\Path;

#[DocumentationDSL(module: Module::CHART_JS, type: Type::HELPER)]
function bar_chart(EntryReference $label, References $datasets) : BarChart
{
    return new BarChart($label, $datasets);
}

#[DocumentationDSL(module: Module::CHART_JS, type: Type::HELPER)]
function line_chart(EntryReference $label, References $datasets) : LineChart
{
    return new LineChart($label, $datasets);
}

#[DocumentationDSL(module: Module::CHART_JS, type: Type::HELPER)]
function pie_chart(EntryReference $label, References $datasets) : PieChart
{
    return new PieChart($label, $datasets);
}

#[DocumentationDSL(module: Module::CHART_JS, type: Type::LOADER)]
function to_chartjs_file(Chart $type, Path|string|null $output = null, Path|string|null $template = null) : ChartJSLoader
{
    if (\is_string($output)) {
        $output = Path::realpath($output);
    }

    if (null === $template) {
        return new ChartJSLoader($type, $output);
    }

    if (\is_string($template)) {
        $template = Path::realpath($template);
    }

    return new ChartJSLoader($type, output: $output, template: $template);
}

#[DocumentationDSL(module: Module::CHART_JS, type: Type::LOADER)]
function to_chartjs_var(Chart $type, array &$output) : ChartJSLoader
{
    /** @psalm-suppress ReferenceConstraintViolation */
    return new ChartJSLoader($type, outputVar: $output);
}
