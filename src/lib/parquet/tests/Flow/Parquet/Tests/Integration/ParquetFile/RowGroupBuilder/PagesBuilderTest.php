<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile\RowGroupBuilder;

use Faker\Factory;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\DictionaryPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnChunkStatistics;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PagesBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageSizeCalculator;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use PHPUnit\Framework\TestCase;

final class PagesBuilderTest extends TestCase
{
    public function test_building_multiple_pages_for_large_int32_column() : void
    {
        $column = FlatColumn::int32('int32');
        $values = \array_map(static fn ($i) => $i, \range(1, 1024));

        $options = new Options();
        $options->set(Option::PAGE_SIZE_BYTES, 1024); // 1024 / 4 = 256 - this is the total number of integers we want to keep in a single page
        $statistics = new ColumnChunkStatistics($column);

        foreach ($values as $value) {
            $statistics->add($value);
        }
        $pages = (new PagesBuilder(DataConverter::initialize($options), new PageSizeCalculator($options), $options))
            ->build($column, $values, $statistics);

        $this->assertCount(4, $pages->dataPageContainers());
        $this->assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::PLAIN,
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
        $column = FlatColumn::enum('enum');
        $enum = [
            0 => 'RED',
            1 => 'GREEN',
            2 => 'BLUE',
        ];
        $values = \array_map(static fn ($i) => $enum[\random_int(0, 2)], \range(0, 99));
        $statistics = new ColumnChunkStatistics($column);

        foreach ($values as $value) {
            $statistics->add($value);
        }

        $options = new Options();
        $pages = (new PagesBuilder(DataConverter::initialize($options), new PageSizeCalculator($options), $options))
            ->build($column, $values, $statistics);

        $this->assertEquals(
            new PageHeader(
                Type::DICTIONARY_PAGE,
                \strlen($pages->dictionaryPageContainer()->pageBuffer),
                \strlen($pages->dictionaryPageContainer()->pageBuffer),
                null,
                null,
                new DictionaryPageHeader(
                    Encodings::PLAIN,
                    $pages->valuesCount(),
                )
            ),
            $pages->dictionaryPageContainer()->pageHeader
        );
        $this->assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::RLE_DICTIONARY,
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
        $column = FlatColumn::int32('int32');
        $values = \array_map(static fn ($i) => $i, \range(0, 99));

        $statistics = new ColumnChunkStatistics($column);

        foreach ($values as $value) {
            $statistics->add($value);
        }

        $options = new Options();
        $pages = (new PagesBuilder(DataConverter::initialize($options), new PageSizeCalculator($options), $options))
            ->build($column, $values, $statistics);

        $this->assertCount(1, $pages->dataPageContainers());
        $this->assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::PLAIN,
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
        $column = FlatColumn::json('json');
        $faker = Factory::create();
        $values = \array_map(static fn ($i) => \json_encode(['id' => $faker->uuid], JSON_THROW_ON_ERROR), \range(0, 99));
        $statistics = new ColumnChunkStatistics($column);

        foreach ($values as $value) {
            $statistics->add($value);
        }

        $options = new Options();
        $pages = (new PagesBuilder(DataConverter::initialize($options), new PageSizeCalculator($options), $options))
            ->build($column, $values, $statistics);

        $this->assertNull($pages->dictionaryPageContainer()->pageHeader);
        $this->assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::PLAIN,
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
        $column = FlatColumn::string('string');
        $values = \array_map(static fn ($i) => 'abcdefghij', \range(0, 99));
        $options = Options::default()->set(Option::PAGE_SIZE_BYTES, 50);
        $statistics = new ColumnChunkStatistics($column);

        foreach ($values as $value) {
            $statistics->add($value);
        }

        $pages = (new PagesBuilder(DataConverter::initialize($options), new PageSizeCalculator($options), $options))
            ->build($column, $values, $statistics);

        $this->assertCount(1, $pages->dataPageContainers());
        $this->assertEquals(
            new PageHeader(
                Type::DICTIONARY_PAGE,
                \strlen($pages->dictionaryPageContainer()->pageBuffer),
                \strlen($pages->dictionaryPageContainer()->pageBuffer),
                null,
                null,
                new DictionaryPageHeader(
                    Encodings::PLAIN,
                    1, // string is constant, so we only have one value in dictionary
                )
            ),
            $pages->dictionaryPageContainer()->pageHeader
        );
        $this->assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::RLE_DICTIONARY,
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
        $column = FlatColumn::string('uuid');
        $faker = Factory::create();
        $values = \array_map(static fn ($i) => $faker->uuid, \range(0, 99));
        $statistics = new ColumnChunkStatistics($column);

        foreach ($values as $value) {
            $statistics->add($value);
        }
        $options = new Options();
        $pages = (new PagesBuilder(DataConverter::initialize($options), new PageSizeCalculator($options), $options))
            ->build($column, $values, $statistics);

        $this->assertNull($pages->dictionaryPageContainer());
        $this->assertEquals(
            new PageHeader(
                Type::DATA_PAGE,
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                \strlen($pages->dataPageContainers()[0]->pageBuffer),
                new DataPageHeader(
                    Encodings::PLAIN,
                    \count($values),
                ),
                null,
                null
            ),
            $pages->dataPageContainers()[0]->pageHeader
        );
    }
}
