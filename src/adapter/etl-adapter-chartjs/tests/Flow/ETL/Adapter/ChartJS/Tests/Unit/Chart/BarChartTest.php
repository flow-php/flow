<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Tests\Unit\Chart;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use Flow\ETL\Adapter\ChartJS\Chart\BarChart;
use Flow\ETL\DSL\From;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class BarChartTest extends TestCase
{
    public function test_collecting_data_from_rows() : void
    {
        $data = [
            ['Date' => '2023-01-01', 'Revenue' => 10000.53, 'CM' => 5000.12, 'Ads Spends' => 2000.78, 'Storage Costs' => 1000.34, 'Shipping Costs' => 1500.45, 'Currency' => 'USD'],
            ['Date' => '2023-01-02', 'Revenue' => 10234.56, 'CM' => 5102.23, 'Ads Spends' => 2050.12, 'Storage Costs' => 1050.78, 'Shipping Costs' => 1550.99, 'Currency' => 'USD'],
            ['Date' => '2023-01-03', 'Revenue' => 11000.98, 'CM' => 5200.32, 'Ads Spends' => 2100.67, 'Storage Costs' => 1100.87, 'Shipping Costs' => 1600.34, 'Currency' => 'USD'],
            ['Date' => '2023-01-04', 'Revenue' => 10890.34, 'CM' => 5300.98, 'Ads Spends' => 2150.56, 'Storage Costs' => 1150.67, 'Shipping Costs' => 1650.87, 'Currency' => 'USD'],
            ['Date' => '2023-01-05', 'Revenue' => 13750.12, 'CM' => 5950.78, 'Ads Spends' => 2750.78, 'Storage Costs' => 1750.78, 'Shipping Costs' => 2250.12, 'Currency' => 'USD'],
            ['Date' => '2023-02-06', 'Revenue' => 14000.23, 'CM' => 6000.89, 'Ads Spends' => 2800.89, 'Storage Costs' => 1800.89, 'Shipping Costs' => 2300.23, 'Currency' => 'USD'],
        ];

        $rows = (new Flow())
            ->read(From::memory(new ArrayMemory($data)))
            ->fetch();

        $chart = new BarChart(ref('Date'), refs(ref('Revenue'), ref('CM'), ref('Ads Spends'), ref('Storage Costs'), ref('Shipping Costs')));

        $chart->setDatasetOptions(ref('Revenue'), ['backgroundColor' => 'green']);

        $chart->collect($rows);

        $this->assertSame(
            [
                'type' => 'bar',
                'data' => [
                    'labels' => ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05', '2023-02-06'],
                    'datasets' => [
                        [
                            'label' => 'Revenue',
                            'data' => [10000.53, 10234.56, 11000.98, 10890.34, 13750.12, 14000.23],
                            'backgroundColor' => 'green',
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
                    ],
                ],
            ],
            $chart->data(),
        );
    }
<<<<<<< Updated upstream

    public function test_setting_option_for_non_existing_dataset() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Dataset "CM" does not exist');

        $chart = new BarChart(ref('Date'), refs(ref('Revenue'), ref('Ads Spends'), ref('Storage Costs'), ref('Shipping Costs')));

        $chart->setDatasetOptions(ref('CM'), ['label' => 'CM']);
    }
=======
>>>>>>> Stashed changes
}
