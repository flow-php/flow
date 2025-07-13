<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\{DeltaBinaryPackedDecoder, DeltaBinaryPackedEncoder};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DeltaBinaryPackedEncodingRoundtripTest extends TestCase
{
    /**
     * @return array<string, array<array<int>>>
     */
    public static function problematicInt64ValuesProvider() : array
    {
        return [
            'large_positive_values' => [[7595602394150081560, 5962760613797072767, 6865028960044616023]],
            'mixed_large_values' => [[1798933430085394339, 8845978805043041461, 6588414276814208324]],
            'edge_case_values' => [[PHP_INT_MAX, PHP_INT_MAX - 1000, PHP_INT_MAX - 2000]],
            'failing_compression_test_sample' => [[
                7595602394150081560, // Expected, but got 7595602394150079999
                5962760613797072767, // Expected, but got -360199917913499541
                6865028960044616023, // Expected, but got -8316002229977079081
            ]],
        ];
    }

    /**
     * @param array<int> $values
     */
    #[DataProvider('problematicInt64ValuesProvider')]
    public function test_roundtrip_problematic_int64_values(array $values) : void
    {
        $encoded = (new DeltaBinaryPackedEncoder())->encode($values);
        $decoded = (new DeltaBinaryPackedDecoder())->decode($encoded, count($values));

        self::assertSame($values, $decoded, 'Values should roundtrip correctly through delta encoding');
    }
}
