<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS;

use Flow\ETL\Adapter\ChartJS\Chart\BarChart;
use Flow\ETL\Adapter\ChartJS\Chart\LineChart;
use Flow\ETL\Adapter\ChartJS\Chart\PieChart;
use Flow\ETL\Attribute\DocumentationDSL;
use Flow\ETL\Attribute\Module;
use Flow\ETL\Attribute\Type;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function Flow\Filesystem\DSL\path_real;
use function is_string;

#[DocumentationDSL(module: Module::CHART_JS, type: Type::HELPER)]
function bar_chart(Reference $label, References $datasets): BarChart
{
    return new BarChart($label, $datasets);
}

#[DocumentationDSL(module: Module::CHART_JS, type: Type::HELPER)]
function line_chart(Reference $label, References $datasets): LineChart
{
    return new LineChart($label, $datasets);
}

#[DocumentationDSL(module: Module::CHART_JS, type: Type::HELPER)]
function pie_chart(Reference $label, References $datasets): PieChart
{
    return new PieChart($label, $datasets);
}

#[DocumentationDSL(module: Module::CHART_JS, type: Type::LOADER)]
function to_chartjs(Chart $type): ChartJSLoader
{
    return new ChartJSLoader($type);
}

/**
 * @param Chart $type
 * @param null|Path|string $output - @deprecated use $loader->withOutputPath() instead
 * @param null|Path|string $template - @deprecated use $loader->withTemplate() instead
 */
#[DocumentationDSL(module: Module::CHART_JS, type: Type::LOADER)]
function to_chartjs_file(
    Chart $type,
    Path|string|null $output = null,
    Path|string|null $template = null,
    Filesystem $filesystem = new NativeLocalFilesystem(),
): ChartJSLoader {
    if (is_string($output)) {
        $output = path_real($output);
    }

    if (is_string($template)) {
        $template = path_real($template);
    }

    $loader = new ChartJSLoader($type);

    if ($template) {
        $loader->withTemplate($template, $filesystem);
    }

    if ($output !== null) {
        $loader->withOutputPath($output, $filesystem);
    }

    return $loader;
}

/**
 * @param Chart $type
 * @param array<array-key, mixed> $output - @deprecated use $loader->withOutputVar() instead
 */
#[DocumentationDSL(module: Module::CHART_JS, type: Type::LOADER)]
function to_chartjs_var(Chart $type, array &$output): ChartJSLoader
{
    return (new ChartJSLoader($type))->withOutputVar($output);
}
