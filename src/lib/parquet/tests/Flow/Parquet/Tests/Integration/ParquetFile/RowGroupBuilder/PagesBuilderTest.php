<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile\RowGroupBuilder;

use function Flow\ETL\DSL\generate_random_int;
use Faker\Factory;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Page\Header\{DataPageHeader, DictionaryPageHeader, Type};
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroupBuilder\{ColumnChunkStatistics,
    DremelShredder,
    FlatColumnData,
    PageSizeCalculator,
    PagesBuilder,
    Validator\ColumnDataValidator};
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\{Compressions, Encodings, Schema};
use Flow\Parquet\{Option, Options};
use PHPUnit\Framework\TestCase;

final class PagesBuilderTest extends TestCase
{
    public function test_building_multiple_pages_for_large_int32_column() : void
    {
        $schema = Schema::with(FlatColumn::int32('int32'));
        $values = \array_map(static fn ($i) => $i, \range(1, 1024));

        $options = new Options();
        $options->set(Option::PAGE_SIZE_BYTES, 1024); // 1024 / 4 = 256 - this is the total number of integers we want to keep in a single page
        $statistics = new ColumnChunkStatistics($flatColumn = $schema->getFlat('int32'));

        $data = FlatColumnData::initialize($flatColumn);

        foreach ($values as $value) {
            $statistics->add($value);
            $data->merge($this->dremelShredder()->shred($flatColumn, ['int32' => $value]));
        }

        $pages = (new PagesBuilder(Compressions::UNCOMPRESSED, new PageSizeCalculator($options), $options))
            ->build(
                $flatColumn,
                $data->values('int32'),
                $statistics
            );

        self::assertCount(4, $pages->dataPageContainers());
        self::assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::PLAIN,
                    Encodings::RLE,
                    Encodings::RLE,
                    256,
                ),
                null,
                null
            ),
            $pages->dataPageContainers()[0]->pageHeader
        );
    }

    public function test_building_pages_for_enum_columns() : void
    {
        $schema = Schema::with(FlatColumn::enum('enum'));
        $enum = [
            0 => 'RED',
            1 => 'GREEN',
            2 => 'BLUE',
        ];
        $values = \array_map(static fn ($i) => $enum[generate_random_int(0, 2)], \range(0, 99));
        $statistics = new ColumnChunkStatistics($flatColumn = $schema->getFlat('enum'));

        $data = FlatColumnData::initialize($flatColumn);

        foreach ($values as $value) {
            $statistics->add($value);
            $data->merge($this->dremelShredder()->shred($flatColumn, ['enum' => $value]));
        }

        $options = new Options();
        $pages = (new PagesBuilder(Compressions::UNCOMPRESSED, new PageSizeCalculator($options), $options))
            ->build(
                $flatColumn,
                $data->values('enum'),
                $statistics
            );

        self::assertEquals(
            new PageHeader(
                Type::DICTIONARY_PAGE,
                \strlen($pages->dictionaryPageContainer()->pageBuffer),
                \strlen($pages->dictionaryPageContainer()->pageBuffer),
                null,
                null,
                new DictionaryPageHeader(
                    Encodings::PLAIN_DICTIONARY,
                    \count($enum),
                )
            ),
            $pages->dictionaryPageContainer()->pageHeader
        );
        self::assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::RLE_DICTIONARY,
                    Encodings::RLE,
                    Encodings::RLE,
                    \count($values),
                ),
                null,
                null
            ),
            $pages->dataPageContainers()[0]->pageHeader
        );
    }

    public function test_building_pages_for_integer_column() : void
    {
        $schema = Schema::with(FlatColumn::int32('int32'));
        $values = \array_map(static fn ($i) => $i, \range(0, 99));

        $statistics = new ColumnChunkStatistics($flatColumn = $schema->getFlat('int32'));

        $data = FlatColumnData::initialize($flatColumn);

        foreach ($values as $value) {
            $statistics->add($value);
            $data->merge($this->dremelShredder()->shred($flatColumn, ['int32' => $value]));
        }

        $options = new Options();
        $pages = (new PagesBuilder(Compressions::UNCOMPRESSED, new PageSizeCalculator($options), $options))
            ->build(
                $flatColumn,
                $data->values('int32'),
                $statistics
            );

        self::assertCount(1, $pages->dataPageContainers());
        self::assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::PLAIN,
                    Encodings::RLE,
                    Encodings::RLE,
                    \count($values),
                ),
                null,
                null
            ),
            $pages->dataPageContainers()[0]->pageHeader
        );
    }

    public function test_building_pages_for_json_columns() : void
    {
        $schema = Schema::with(FlatColumn::json('json'));
        $faker = Factory::create();
        $values = \array_map(static fn ($i) => \json_encode(['id' => $faker->uuid], JSON_THROW_ON_ERROR), \range(0, 99));
        $statistics = new ColumnChunkStatistics($flatColumn = $schema->getFlat('json'));

        $data = FlatColumnData::initialize($flatColumn);

        foreach ($values as $value) {
            $statistics->add($value);
            $data->merge($this->dremelShredder()->shred($flatColumn, ['json' => $value]));
        }

        $options = new Options();
        $pages = (new PagesBuilder(Compressions::UNCOMPRESSED, new PageSizeCalculator($options), $options))
            ->build(
                $flatColumn,
                $data->values('json'),
                $statistics
            );

        self::assertNull($pages->dictionaryPageContainer());
        self::assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::PLAIN,
                    Encodings::RLE,
                    Encodings::RLE,
                    \count($values),
                ),
                null,
                null
            ),
            $pages->dataPageContainers()[0]->pageHeader
        );
    }

    public function test_building_pages_for_string_columns_with_very_low_cardinality() : void
    {
        $schema = Schema::with(FlatColumn::string('string'));
        $values = \array_map(static fn ($i) => 'abcdefghij', \range(0, 99));
        $options = Options::default()->set(Option::PAGE_SIZE_BYTES, 50);
        $statistics = new ColumnChunkStatistics($flatColumn = $schema->getFlat('string'));

        $data = FlatColumnData::initialize($flatColumn);

        foreach ($values as $value) {
            $statistics->add($value);
            $data->merge($this->dremelShredder()->shred($flatColumn, ['string' => $value]));
        }

        $pages = (new PagesBuilder(Compressions::UNCOMPRESSED, new PageSizeCalculator($options), $options))
            ->build(
                $flatColumn,
                $data->values('string'),
                $statistics
            );

        self::assertCount(1, $pages->dataPageContainers());
        self::assertEquals(
            new PageHeader(
                Type::DICTIONARY_PAGE,
                \strlen($pages->dictionaryPageContainer()->pageBuffer),
                \strlen($pages->dictionaryPageContainer()->pageBuffer),
                null,
                null,
                new DictionaryPageHeader(
                    Encodings::PLAIN_DICTIONARY,
                    1, // string is constant, so we only have one value in dictionary
                )
            ),
            $pages->dictionaryPageContainer()->pageHeader
        );
        self::assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::RLE_DICTIONARY,
                    Encodings::RLE,
                    Encodings::RLE,
                    100,
                ),
                null,
                null
            ),
            $pages->dataPageContainers()[0]->pageHeader
        );
    }

    public function test_building_pages_for_uuid_columns() : void
    {
        $schema = Schema::with(FlatColumn::string('uuid'));
        $faker = Factory::create();
        $values = \array_map(static fn ($i) => $faker->uuid, \range(0, 99));
        /** @var FlatColumn $flatColumn */
        $statistics = new ColumnChunkStatistics($flatColumn = $schema->getFlat('uuid'));

        $data = FlatColumnData::initialize($flatColumn);

        foreach ($values as $value) {
            $statistics->add($value);
            $data->merge($this->dremelShredder()->shred($flatColumn, ['uuid' => $value]));
        }

        $options = new Options();
        $pages = (new PagesBuilder(Compressions::UNCOMPRESSED, new PageSizeCalculator($options), $options))
            ->build(
                $schema->getFlat('uuid'),
                $data->values('uuid'),
                $statistics
            );

        self::assertNull($pages->dictionaryPageContainer());
        self::assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::PLAIN,
                    Encodings::RLE,
                    Encodings::RLE,
                    \count($values),
                ),
                null,
                null
            ),
            $pages->dataPageContainers()[0]->pageHeader
        );
    }

    private function dremelShredder() : DremelShredder
    {
        return new DremelShredder(new ColumnDataValidator(), DataConverter::initialize(Options::default()));
    }
}
