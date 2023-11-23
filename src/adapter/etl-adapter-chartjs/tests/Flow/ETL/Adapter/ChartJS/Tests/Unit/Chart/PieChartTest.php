<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\ChartJS\Tests\Unit\Chart;

use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\ChartJS\Chart\PieChart;
use Flow\ETL\DSL\From;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Flow;
use Flow\ETL\Memory\ArrayMemory;
use PHPUnit\Framework\TestCase;

final class PieChartTest extends TestCase
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

        $chart = new PieChart(ref('Date'), [ref('Revenue'), ref('CM'), ref('Ads Spends'), ref('Storage Costs'), ref('Shipping Costs')]);
        $chart->collect($rows);

        $this->assertSame(
            [
                'type' => 'pie',
                'data' => [
                    'labels' => ['Revenue', 'CM', 'Ads Spends', 'Storage Costs', 'Shipping Costs'],
                    'datasets' => [
                        [
                            'data' => [
                                10000.53,
                                5000.12,
                                2000.78,
                                1000.34,
                                1500.45,
                                10234.56,
                                5102.23,
                                2050.12,
                                1050.78,
                                1550.99,
                                11000.98,
                                5200.32,
                                2100.67,
                                1100.87,
                                1600.34,
                                10890.34,
                                5300.98,
                                2150.56,
                                1150.67,
                                1650.87,
                                13750.12,
                                5950.78,
                                2750.78,
                                1750.78,
                                2250.12,
                                14000.23,
                                6000.89,
                                2800.89,
                                1800.89,
                                2300.23,
                            ],
                            'label' => '2023-02-06',
                        ],
                    ],
                ],
            ],
            $chart->data(),
        );
    }

    public function test_setting_option_for_non_existing_dataset() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Dataset "CM" does not exist');

        $chart = new PieChart(ref('Date'), [ref('Revenue'), ref('Ads Spends'), ref('Storage Costs'), ref('Shipping Costs')]);
        $chart->setDatasetOptions(ref('CM'), ['label' => 'CM']);
    }
}
