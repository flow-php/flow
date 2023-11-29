<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Tests\Integration;

use function Flow\ETL\DSL\bar_chart;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\from_memory;
use function Flow\ETL\DSL\line_chart;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\pie_chart;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\to_chartjs_file;
use function Flow\ETL\DSL\to_chartjs_var;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class ChartJSLoaderTest extends TestCase
{
    public function test_loading_data_to_bar_chart() : void
    {
        $data = [
            ['Date' => '2023-01-01', 'Revenue' => 10000.53, 'CM' => 5000.12, 'Ads Spends' => 2000.78, 'Storage Costs' => 1000.34, 'Shipping Costs' => 1500.45, 'Currency' => 'USD'],
            ['Date' => '2023-01-02', 'Revenue' => 10234.56, 'CM' => 5102.23, 'Ads Spends' => 2050.12, 'Storage Costs' => 1050.78, 'Shipping Costs' => 1550.99, 'Currency' => 'USD'],
            ['Date' => '2023-01-03', 'Revenue' => 11000.98, 'CM' => 5200.32, 'Ads Spends' => 2100.67, 'Storage Costs' => 1100.87, 'Shipping Costs' => 1600.34, 'Currency' => 'USD'],
            ['Date' => '2023-01-04', 'Revenue' => 10890.34, 'CM' => 5300.98, 'Ads Spends' => 2150.56, 'Storage Costs' => 1150.67, 'Shipping Costs' => 1650.87, 'Currency' => 'USD'],
            ['Date' => '2023-01-05', 'Revenue' => 13750.12, 'CM' => 5950.78, 'Ads Spends' => 2750.78, 'Storage Costs' => 1750.78, 'Shipping Costs' => 2250.12, 'Currency' => 'USD'],
            ['Date' => '2023-02-06', 'Revenue' => 14000.23, 'CM' => 6000.89, 'Ads Spends' => 2800.89, 'Storage Costs' => 1800.89, 'Shipping Costs' => 2300.23, 'Currency' => 'USD'],
        ];

        df()
            ->read(from_memory(new ArrayMemory($data)))
            ->withEntry('Profit', ref('Revenue')->minus(ref('CM'))->minus(ref('Ads Spends'))->minus(ref('Storage Costs'))->minus(ref('Shipping Costs'))->round(lit(2)))
            ->write(
                to_chartjs_file(
                    $chart = bar_chart(
                        ref('Date'),
                        refs(
                            ref('Revenue'),
                            ref('CM'),
                            ref('Ads Spends'),
                            ref('Storage Costs'),
                            ref('Shipping Costs'),
                            ref('Profit'),
                        )
                    ),
                    $output = __DIR__ . '/Output/bar_chart.html'
                )
            )
            ->run();

        $this->assertSame(
            [
                'type' => 'bar',
                'data' => [
                    'labels' => ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05', '2023-02-06'],
                    'datasets' => [
                        [
                            'label' => 'Revenue',
                            'data' => [10000.53, 10234.56, 11000.98, 10890.34, 13750.12, 14000.23],
                        ],
                        [
                            'label' => 'CM',
                            'data' => [5000.12, 5102.23, 5200.32, 5300.98, 5950.78, 6000.89],
                        ],
                        [
                            'label' => 'Ads Spends',
                            'data' => [2000.78, 2050.12, 2100.67, 2150.56, 2750.78, 2800.89],
                        ],
                        [
                            'label' => 'Storage Costs',
                            'data' => [1000.34, 1050.78, 1100.87, 1150.67, 1750.78, 1800.89],
                        ],
                        [
                            'label' => 'Shipping Costs',
                            'data' => [1500.45, 1550.99, 1600.34, 1650.87, 2250.12, 2300.23],
                        ],
                        [
                            'label' => 'Profit',
                            'data' => [498.84, 480.44, 998.78, 637.26, 1047.66, 1097.33],
                        ],
                    ],
                ],
            ],
            $chart->data(),
        );
        $this->assertFileExists($output);
    }

    public function test_loading_data_to_bar_chart_output_variable() : void
    {
        $data = [
            ['Date' => '2023-01-01', 'Revenue' => 10000.53, 'CM' => 5000.12, 'Ads Spends' => 2000.78, 'Storage Costs' => 1000.34, 'Shipping Costs' => 1500.45, 'Currency' => 'USD'],
            ['Date' => '2023-01-02', 'Revenue' => 10234.56, 'CM' => 5102.23, 'Ads Spends' => 2050.12, 'Storage Costs' => 1050.78, 'Shipping Costs' => 1550.99, 'Currency' => 'USD'],
            ['Date' => '2023-01-03', 'Revenue' => 11000.98, 'CM' => 5200.32, 'Ads Spends' => 2100.67, 'Storage Costs' => 1100.87, 'Shipping Costs' => 1600.34, 'Currency' => 'USD'],
            ['Date' => '2023-01-04', 'Revenue' => 10890.34, 'CM' => 5300.98, 'Ads Spends' => 2150.56, 'Storage Costs' => 1150.67, 'Shipping Costs' => 1650.87, 'Currency' => 'USD'],
            ['Date' => '2023-01-05', 'Revenue' => 13750.12, 'CM' => 5950.78, 'Ads Spends' => 2750.78, 'Storage Costs' => 1750.78, 'Shipping Costs' => 2250.12, 'Currency' => 'USD'],
            ['Date' => '2023-02-06', 'Revenue' => 14000.23, 'CM' => 6000.89, 'Ads Spends' => 2800.89, 'Storage Costs' => 1800.89, 'Shipping Costs' => 2300.23, 'Currency' => 'USD'],
        ];

        $output = [];

        df()
            ->read(from_memory(new ArrayMemory($data)))
            ->withEntry('Profit', ref('Revenue')->minus(ref('CM'))->minus(ref('Ads Spends'))->minus(ref('Storage Costs'))->minus(ref('Shipping Costs'))->round(lit(2)))
            ->write(
                to_chartjs_var(
                    bar_chart(
                        ref('Date'),
                        refs(
                            ref('Revenue'),
                            ref('CM'),
                            ref('Ads Spends'),
                            ref('Storage Costs'),
                            ref('Shipping Costs'),
                            ref('Profit'),
                        )
                    ),
                    $output
                )
            )
            ->run();

        $this->assertSame(
            [
                'type' => 'bar',
                'data' => [
                    'labels' => ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05', '2023-02-06'],
                    'datasets' => [
                        [
                            'label' => 'Revenue',
                            'data' => [10000.53, 10234.56, 11000.98, 10890.34, 13750.12, 14000.23],
                        ],
                        [
                            'label' => 'CM',
                            'data' => [5000.12, 5102.23, 5200.32, 5300.98, 5950.78, 6000.89],
                        ],
                        [
                            'label' => 'Ads Spends',
                            'data' => [2000.78, 2050.12, 2100.67, 2150.56, 2750.78, 2800.89],
                        ],
                        [
                            'label' => 'Storage Costs',
                            'data' => [1000.34, 1050.78, 1100.87, 1150.67, 1750.78, 1800.89],
                        ],
                        [
                            'label' => 'Shipping Costs',
                            'data' => [1500.45, 1550.99, 1600.34, 1650.87, 2250.12, 2300.23],
                        ],
                        [
                            'label' => 'Profit',
                            'data' => [498.84, 480.44, 998.78, 637.26, 1047.66, 1097.33],
                        ],
                    ],
                ],
            ],
            $output
        );
    }

    public function test_loading_data_to_line_chart() : void
    {
        $data = [
            ['Date' => '2023-01-01', 'Revenue' => 10000.53, 'CM' => 5000.12, 'Ads Spends' => 2000.78, 'Storage Costs' => 1000.34, 'Shipping Costs' => 1500.45, 'Currency' => 'USD'],
            ['Date' => '2023-01-02', 'Revenue' => 10234.56, 'CM' => 5102.23, 'Ads Spends' => 2050.12, 'Storage Costs' => 1050.78, 'Shipping Costs' => 1550.99, 'Currency' => 'USD'],
            ['Date' => '2023-01-03', 'Revenue' => 11000.98, 'CM' => 5200.32, 'Ads Spends' => 2100.67, 'Storage Costs' => 1100.87, 'Shipping Costs' => 1600.34, 'Currency' => 'USD'],
            ['Date' => '2023-01-04', 'Revenue' => 10890.34, 'CM' => 5300.98, 'Ads Spends' => 2150.56, 'Storage Costs' => 1150.67, 'Shipping Costs' => 1650.87, 'Currency' => 'USD'],
            ['Date' => '2023-01-05', 'Revenue' => 13750.12, 'CM' => 5950.78, 'Ads Spends' => 2750.78, 'Storage Costs' => 1750.78, 'Shipping Costs' => 2250.12, 'Currency' => 'USD'],
            ['Date' => '2023-02-06', 'Revenue' => 14000.23, 'CM' => 6000.89, 'Ads Spends' => 2800.89, 'Storage Costs' => 1800.89, 'Shipping Costs' => 2300.23, 'Currency' => 'USD'],
        ];

        df()
            ->read(from_memory(new ArrayMemory($data)))
            ->withEntry('Profit', ref('Revenue')->minus(ref('CM'))->minus(ref('Ads Spends'))->minus(ref('Storage Costs'))->minus(ref('Shipping Costs'))->round(lit(2)))
            ->write(
                to_chartjs_file(
                    $chart = line_chart(
                        ref('Date'),
                        refs(
                            ref('Revenue'),
                            ref('CM'),
                            ref('Ads Spends'),
                            ref('Storage Costs'),
                            ref('Shipping Costs'),
                            ref('Profit'),
                        )
                    ),
                    $output = __DIR__ . '/Output/line_chart.html'
                )
            )
            ->run();

        $this->assertSame(
            [
                'type' => 'line',
                'data' => [
                    'labels' => ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05', '2023-02-06'],
                    'datasets' => [
                        [
                            'label' => 'Revenue',
                            'data' => [10000.53, 10234.56, 11000.98, 10890.34, 13750.12, 14000.23],
                        ],
                        [
                            'label' => 'CM',
                            'data' => [5000.12, 5102.23, 5200.32, 5300.98, 5950.78, 6000.89],
                        ],
                        [
                            'label' => 'Ads Spends',
                            'data' => [2000.78, 2050.12, 2100.67, 2150.56, 2750.78, 2800.89],
                        ],
                        [
                            'label' => 'Storage Costs',
                            'data' => [1000.34, 1050.78, 1100.87, 1150.67, 1750.78, 1800.89],
                        ],
                        [
                            'label' => 'Shipping Costs',
                            'data' => [1500.45, 1550.99, 1600.34, 1650.87, 2250.12, 2300.23],
                        ],
                        [
                            'label' => 'Profit',
                            'data' => [498.84, 480.44, 998.78, 637.26, 1047.66, 1097.33],
                        ],
                    ],
                ],
            ],
            $chart->data(),
        );
        $this->assertFileExists($output);
    }

    public function test_loading_data_to_pie_chart() : void
    {
        $data = [
            ['Date' => '2023-01-01', 'Revenue' => 10000.53, 'CM' => 5000.12, 'Ads Spends' => 2000.78, 'Storage Costs' => 1000.34, 'Shipping Costs' => 1500.45, 'Currency' => 'USD'],
            ['Date' => '2023-01-02', 'Revenue' => 10234.56, 'CM' => 5102.23, 'Ads Spends' => 2050.12, 'Storage Costs' => 1050.78, 'Shipping Costs' => 1550.99, 'Currency' => 'USD'],
            ['Date' => '2023-01-03', 'Revenue' => 11000.98, 'CM' => 5200.32, 'Ads Spends' => 2100.67, 'Storage Costs' => 1100.87, 'Shipping Costs' => 1600.34, 'Currency' => 'USD'],
            ['Date' => '2023-01-04', 'Revenue' => 10890.34, 'CM' => 5300.98, 'Ads Spends' => 2150.56, 'Storage Costs' => 1150.67, 'Shipping Costs' => 1650.87, 'Currency' => 'USD'],
            ['Date' => '2023-01-05', 'Revenue' => 13750.12, 'CM' => 5950.78, 'Ads Spends' => 2750.78, 'Storage Costs' => 1750.78, 'Shipping Costs' => 2250.12, 'Currency' => 'USD'],
            ['Date' => '2023-02-06', 'Revenue' => 14000.23, 'CM' => 6000.89, 'Ads Spends' => 2800.89, 'Storage Costs' => 1800.89, 'Shipping Costs' => 2300.23, 'Currency' => 'USD'],
        ];

        $chart = pie_chart(
            ref('Date'),
            refs(
                ref('Revenue'),
                ref('CM'),
                ref('Ads Spends'),
                ref('Storage Costs'),
                ref('Shipping Costs'),
                ref('Profit'),
            )
        )
            ->setOptions(['label' => 'PnL']);

        df()
            ->read(from_memory(new ArrayMemory($data)))
            ->withEntry('Profit', ref('Revenue')->minus(ref('CM'))->minus(ref('Ads Spends'))->minus(ref('Storage Costs'))->minus(ref('Shipping Costs'))->round(lit(2)))
            ->aggregate(
                first(ref('Date')->as('Date')),
                sum(ref('Revenue')->as('Revenue')),
                sum(ref('CM')->as('CM')),
                sum(ref('Ads Spends')->as('Ads Spends')),
                sum(ref('Storage Costs')->as('Storage Costs')),
                sum(ref('Shipping Costs')->as('Shipping Costs')),
                sum(ref('Profit')->as('Profit')),
            )
            ->write(
                to_chartjs_file(
                    $chart,
                    $output = __DIR__ . '/Output/pie_chart.html'
                )
            )
            ->run();

        $this->assertSame(
            [
                'type' => 'pie',
                'data' => [
                    'labels' => ['Revenue', 'CM', 'Ads Spends', 'Storage Costs', 'Shipping Costs', 'Profit'],
                    'datasets' => [
                        [
                            'data' => [
                                69876.76000000001,
                                32555.319999999996,
                                13853.8,
                                7854.33,
                                10853,
                                4760.3099999999995,
                            ],
                            'label' => '2023-01-01',
                        ],
                    ],
                ],
                'options' => [
                    'label' => 'PnL',
                ],
            ],
            $chart->data(),
        );
        $this->assertFileExists($output);
    }
}
