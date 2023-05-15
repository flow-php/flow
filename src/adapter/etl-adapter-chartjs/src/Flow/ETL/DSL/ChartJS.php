<?php

declare(strict_types=1);

namespace Flow\ETL\DSL;

use Flow\ETL\Adapter\ChartJS\Chart;
use Flow\ETL\Adapter\ChartJS\ChartJSLoader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Loader;
use Flow\ETL\Row\EntryReference;

class ChartJS
{
    /**
     * @param EntryReference $label
     * @param array<EntryReference> $datasets
     *
     * @throws InvalidArgumentException
     *
     * @return Chart
     */
    final public static function bar(EntryReference $label, array $datasets) : Chart
    {
        return new Chart\BarChart($label, $datasets);
    }

    /**
     * @param Chart $type
     *
     * @return Loader
     */
    final public static function chart(Chart $type, Path|string $output = null, Path|string $template = __DIR__ . '/../Adapter/ChartJS/Resources/template/full_page.html') : Loader
    {
        if (\is_string($output)) {
            $output = Path::realpath($output);
        }

        if (\is_string($template)) {
            $template = Path::realpath($template);
        }

        return new ChartJSLoader($type, $output, $template);
    }

    /**
     * @param EntryReference $label
     * @param array<EntryReference> $datasets
     *
     * @throws InvalidArgumentException
     *
     * @return Chart
     */
    final public static function line(EntryReference $label, array $datasets) : Chart
    {
        return new Chart\LineChart($label, $datasets);
    }

    /**
     * @param array<EntryReference> $datasets
     *
     * @throws InvalidArgumentException
     *
     * @return Chart
     */
    final public static function pie(array $datasets) : Chart
    {
        return new Chart\PieChart($datasets);
    }
}
