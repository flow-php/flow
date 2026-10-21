<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Data\DeltaBinaryPackedEncoder;
use Flow\Parquet\Data\DeltaByteArrayEncoder;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function array_map;
use function implode;
use function strlen;

final class DeltaByteArrayEncoderTest extends TestCase
{
    /**
     * @return array<string, array{list<string>, list<int>, list<string>}>
     */
    public static function values_provider(): array
    {
        return [
            'spec example' => [
                ['axis', 'axle', 'babble', 'babyhood'],
                [0,      2,      0,        3],
                ['axis', 'le',   'babble', 'yhood'],
            ],
            'repeated value' => [['a', 'a'], [0, 1], ['a', '']],
            'shorter value under a full prefix' => [['abc', 'ab'], [0, 2], ['abc', '']],
            'nul bytes in the prefix' => [["\0\0a", "\0\0b"], [0, 2], ["\0\0a", 'b']],
            'prefix stopping at a nul byte' => [["a\0", 'a'], [0, 1], ["a\0", '']],
            'empty first value' => [['', 'a'], [0, 0], ['', 'a']],
        ];
    }

    /**
     * @param list<string> $values
     * @param list<int> $prefixLengths
     * @param list<string> $suffixes
     */
    #[DataProvider('values_provider')]
    public function test_encode(array $values, array $prefixLengths, array $suffixes): void
    {
        $encoder = new DeltaBinaryPackedEncoder();

        static::assertSame(
            $encoder->encode($prefixLengths) . $encoder->encode(array_map(strlen(...), $suffixes))
                . implode('', $suffixes),
            (new DeltaByteArrayEncoder())->encode($values),
        );
    }
}
