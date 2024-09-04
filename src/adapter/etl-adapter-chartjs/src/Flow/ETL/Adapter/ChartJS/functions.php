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
function to_chartjs(Chart $type) : ChartJSLoader
{
    return new ChartJSLoader($type);
}

/**
 * @param Chart $type
 * @param null|Path|string $output - @deprecated use $loader->withOutputPath() instead
 * @param null|Path|string $template - @deprecated use $loader->withTemplate() instead
 */
#[DocumentationDSL(module: Module::CHART_JS, type: Type::LOADER)]
function to_chartjs_file(Chart $type, Path|string|null $output = null, Path|string|null $template = null) : ChartJSLoader
{
    if (\is_string($output)) {
        $output = Path::realpath($output);
    }

    if (null === $template) {
        $loader = (new ChartJSLoader($type));

        if ($output !== null) {
            return $loader->withOutputPath($output);
        }

        return $loader;
    }

    if (\is_string($template)) {
        $template = Path::realpath($template);
    }

    $loader = (new ChartJSLoader($type))
        ->withTemplate($template);

    if ($output !== null) {
        return $loader->withOutputPath($output);
    }

    return $loader;
}

/**
 * @param Chart $type
 * @param array $output - @deprecated use $loader->withOutputVar() instead
 */
#[DocumentationDSL(module: Module::CHART_JS, type: Type::LOADER)]
function to_chartjs_var(Chart $type, array &$output) : ChartJSLoader
{
    return (new ChartJSLoader($type))->withOutputVar($output);
}
