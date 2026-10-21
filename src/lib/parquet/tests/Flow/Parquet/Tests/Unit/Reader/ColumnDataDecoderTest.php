<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Parquet\Data\DeltaByteArrayEncoder;
use Flow\Parquet\Data\DeltaLengthByteArrayEncoder;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeaderV2;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Reader\ColumnDataDecoder;
use Flow\Parquet\Writer\ValueStorage\RLEBooleanValueStorage;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

final class ColumnDataDecoderTest extends TestCase
{
    /**
     * @return array<string, array{Encodings, string}>
     */
    public static function delta_string_pages_provider(): array
    {
        return [
            'DELTA_LENGTH_BYTE_ARRAY' => [
                Encodings::DELTA_LENGTH_BYTE_ARRAY,
                (new DeltaLengthByteArrayEncoder())->encode(['axis', 'axle', '']),
            ],
            'DELTA_BYTE_ARRAY' => [
                Encodings::DELTA_BYTE_ARRAY,
                (new DeltaByteArrayEncoder())->encode(['axis', 'axle', '']),
            ],
        ];
    }

    #[DataProvider('delta_string_pages_provider')]
    public function test_delta_string_page_v1_decodes(Encodings $encoding, string $buffer): void
    {
        static::assertSame(
            ['axis', 'axle', ''],
            iterator_to_array(
                (new ColumnDataDecoder())
                    ->decodeData(
                        $buffer,
                        FlatColumn::string('s', Repetition::REQUIRED),
                        new DataPageHeader($encoding, Encodings::RLE, Encodings::RLE, 3),
                    )
                    ->rawValues(),
                false,
            ),
        );
    }

    #[DataProvider('delta_string_pages_provider')]
    public function test_delta_string_page_v2_decodes(Encodings $encoding, string $buffer): void
    {
        static::assertSame(
            ['axis', 'axle', ''],
            iterator_to_array(
                (new ColumnDataDecoder())
                    ->decodeDataV2(
                        $buffer,
                        FlatColumn::string('s', Repetition::REQUIRED),
                        new DataPageHeaderV2(3, 0, 3, $encoding, 0, 0, false, null),
                    )
                    ->rawValues(),
                false,
            ),
        );
    }

    public function test_an_rle_boolean_page_v1_decodes(): void
    {
        $column = FlatColumn::boolean('b', Repetition::REQUIRED);
        $storage = new RLEBooleanValueStorage();
        $storage->addValues($column, [true, false, true]);

        static::assertSame(
            [true, false, true],
            iterator_to_array(
                (new ColumnDataDecoder())
                    ->decodeData(
                        $storage->getBuffer(),
                        $column,
                        new DataPageHeader(Encodings::RLE, Encodings::RLE, Encodings::RLE, 3),
                    )
                    ->rawValues(),
                false,
            ),
        );
    }

    public function test_an_rle_boolean_page_v2_decodes(): void
    {
        $column = FlatColumn::boolean('b', Repetition::REQUIRED);
        $storage = new RLEBooleanValueStorage();
        $storage->addValues($column, [true, false, true]);

        static::assertSame(
            [true, false, true],
            iterator_to_array(
                (new ColumnDataDecoder())
                    ->decodeDataV2(
                        $storage->getBuffer(),
                        $column,
                        new DataPageHeaderV2(3, 0, 3, Encodings::RLE, 0, 0, false, null),
                    )
                    ->rawValues(),
                false,
            ),
        );
    }
}
