<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\ParquetFile\RowGroupBuilder;

use Faker\Factory;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\DataPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\DictionaryPageHeader;
use Flow\Parquet\ParquetFile\Page\Header\Type;
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PagesBuilder;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use PHPUnit\Framework\TestCase;

final class PagesBuilderTest extends TestCase
{
    public function test_building_pages_for_integer_column() : void
    {
        $column = FlatColumn::int32('int32');
        $values = \array_map(static fn ($i) => $i, \range(0, 99));

        $pages = (new PagesBuilder(DataConverter::initialize(new Options())))->build($column, $values);

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

    public function test_building_pages_for_string_columns() : void
    {
        $column = FlatColumn::string('string');
        $faker = Factory::create();
        $values = \array_map(static fn ($i) => $faker->text(10), \range(0, 99));

        $pages = (new PagesBuilder(DataConverter::initialize(new Options())))->build($column, $values);

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
                    Encodings::PLAIN_DICTIONARY,
                    \count($values),
                ),
                null,
                null
            ),
            $pages->dataPageContainers()[0]->pageHeader
        );
    }
}
