<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS;

use Flow\ETL\Adapter\ChartJS\Chart\BarChart;
use Flow\ETL\Adapter\ChartJS\Chart\LineChart;
use Flow\ETL\Adapter\ChartJS\Chart\PieChart;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Row\EntryReference;
use Flow\ETL\Row\References;

function bar_chart(EntryReference $label, References $datasets) : BarChart
{
    return new BarChart($label, $datasets);
}

function line_chart(EntryReference $label, References $datasets) : LineChart
{
    return new LineChart($label, $datasets);
}

function pie_chart(EntryReference $label, References $datasets) : PieChart
{
    return new PieChart($label, $datasets);
}

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

function to_chartjs_var(Chart $type, array &$output) : ChartJSLoader
{
    /** @psalm-suppress ReferenceConstraintViolation */
    return new ChartJSLoader($type, outputVar: $output);
}
