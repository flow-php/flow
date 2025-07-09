<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Page\Header\{DataPageHeader, DataPageHeaderV2, DictionaryPageHeader, Type};
use Flow\Parquet\ParquetFile\Page\PageHeader;
use Flow\Parquet\Writer\{PageContainer, PageContainers};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class PageContainersTest extends TestCase
{
    public static function buffer_size_provider() : \Generator
    {
        yield 'small buffer' => [10, 20];
        yield 'medium buffer' => [100, 200];
        yield 'large buffer' => [1000, 2000];
        yield 'empty buffer' => [0, 0];
    }

    public static function container_count_provider() : \Generator
    {
        yield 'empty' => [0];
        yield 'single' => [1];
        yield 'multiple' => [3];
        yield 'many' => [10];
    }

    public static function encoding_types_provider() : \Generator
    {
        yield 'plain' => [Encodings::PLAIN];
        yield 'rle' => [Encodings::RLE];
        yield 'bit_packed' => [Encodings::BIT_PACKED];
        yield 'rle_dictionary' => [Encodings::RLE_DICTIONARY];
        yield 'delta_binary_packed' => [Encodings::DELTA_BINARY_PACKED];
    }

    public static function page_types_provider() : \Generator
    {
        yield 'data page' => [Type::DATA_PAGE, 'data page container'];
        yield 'data page v2' => [Type::DATA_PAGE_V2, 'data page v2 container'];
        yield 'dictionary page' => [Type::DICTIONARY_PAGE, 'dictionary page container'];
    }

    public function test_add_data_page_container() : void
    {
        $containers = new PageContainers();
        $pageContainer = $this->createDataPageContainer();

        $containers->add($pageContainer);

        self::assertCount(1, $containers->dataPageContainers());
        self::assertSame($pageContainer, $containers->dataPageContainers()[0]);
        self::assertNull($containers->dictionaryPageContainer());
    }

    public function test_add_dictionary_page_container() : void
    {
        $containers = new PageContainers();
        $pageContainer = $this->createDictionaryPageContainer();

        $containers->add($pageContainer);

        self::assertSame($pageContainer, $containers->dictionaryPageContainer());
        self::assertEmpty($containers->dataPageContainers());
    }

    public function test_add_duplicate_dictionary_page_throws_exception() : void
    {
        $containers = new PageContainers();
        $pageContainer1 = $this->createDictionaryPageContainer();
        $pageContainer2 = $this->createDictionaryPageContainer();

        $containers->add($pageContainer1);

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Dictionary page container already set');

        $containers->add($pageContainer2);
    }

    public function test_add_mixed_page_containers() : void
    {
        $containers = new PageContainers();
        $dataContainer1 = $this->createDataPageContainer();
        $dictionaryContainer = $this->createDictionaryPageContainer();
        $dataContainer2 = $this->createDataPageContainer();

        $containers->add($dataContainer1);
        $containers->add($dictionaryContainer);
        $containers->add($dataContainer2);

        self::assertCount(2, $containers->dataPageContainers());
        self::assertSame($dictionaryContainer, $containers->dictionaryPageContainer());
        self::assertSame($dataContainer1, $containers->dataPageContainers()[0]);
        self::assertSame($dataContainer2, $containers->dataPageContainers()[1]);
    }

    public function test_add_multiple_data_page_containers() : void
    {
        $containers = new PageContainers();
        $pageContainer1 = $this->createDataPageContainer(10);
        $pageContainer2 = $this->createDataPageContainer(20);
        $pageContainer3 = $this->createDataPageContainer(30);

        $containers->add($pageContainer1);
        $containers->add($pageContainer2);
        $containers->add($pageContainer3);

        self::assertCount(3, $containers->dataPageContainers());
        self::assertSame($pageContainer1, $containers->dataPageContainers()[0]);
        self::assertSame($pageContainer2, $containers->dataPageContainers()[1]);
        self::assertSame($pageContainer3, $containers->dataPageContainers()[2]);
    }

    public function test_buffer_with_data_pages_only() : void
    {
        $containers = new PageContainers();
        $pageContainer1 = $this->createDataPageContainer(valuesCount: 10, headerBuffer: 'header1', pageBuffer: 'data1');
        $pageContainer2 = $this->createDataPageContainer(valuesCount: 20, headerBuffer: 'header2', pageBuffer: 'data2');

        $containers->add($pageContainer1);
        $containers->add($pageContainer2);

        $buffer = $containers->buffer();

        self::assertSame('header1data1header2data2', $buffer);
    }

    public function test_buffer_with_dictionary_and_data_pages() : void
    {
        $containers = new PageContainers();
        $dataContainer = $this->createDataPageContainer(valuesCount: 10, headerBuffer: 'dataheader', pageBuffer: 'datadata');
        $dictionaryContainer = $this->createDictionaryPageContainer(valuesCount: 5, headerBuffer: 'dictheader', pageBuffer: 'dictdata');

        $containers->add($dataContainer);
        $containers->add($dictionaryContainer);

        $buffer = $containers->buffer();

        self::assertSame('dictheaderdictdatadataheaderdatadata', $buffer);
    }

    public function test_buffer_with_dictionary_page_only() : void
    {
        $containers = new PageContainers();
        $dictionaryContainer = $this->createDictionaryPageContainer(valuesCount: 5, headerBuffer: 'header', pageBuffer: 'data');

        $containers->add($dictionaryContainer);

        $buffer = $containers->buffer();

        self::assertSame('headerdata', $buffer);
    }

    public function test_buffer_with_empty_containers() : void
    {
        $containers = new PageContainers();

        $buffer = $containers->buffer();

        self::assertSame('', $buffer);
    }

    public function test_compressed_size_with_data_pages_only() : void
    {
        $containers = new PageContainers();
        $pageContainer1 = $this->createDataPageContainer(valuesCount: 10, headerBuffer: 'header1', pageBuffer: 'data1');
        $pageContainer2 = $this->createDataPageContainer(valuesCount: 20, headerBuffer: 'header2', pageBuffer: 'data2');

        $containers->add($pageContainer1);
        $containers->add($pageContainer2);

        $compressedSize = $containers->compressedSize();

        $expectedSize = $pageContainer1->totalCompressedSize() + $pageContainer2->totalCompressedSize();
        self::assertSame($expectedSize, $compressedSize);
    }

    public function test_compressed_size_with_dictionary_and_data_pages() : void
    {
        $containers = new PageContainers();
        $dataContainer = $this->createDataPageContainer();
        $dictionaryContainer = $this->createDictionaryPageContainer();

        $containers->add($dataContainer);
        $containers->add($dictionaryContainer);

        $compressedSize = $containers->compressedSize();

        $expectedSize = $dataContainer->totalCompressedSize() + $dictionaryContainer->totalCompressedSize();
        self::assertSame($expectedSize, $compressedSize);
    }

    public function test_constructor_with_empty_array() : void
    {
        $containers = new PageContainers([]);

        self::assertEmpty($containers->dataPageContainers());
        self::assertNull($containers->dictionaryPageContainer());
        self::assertSame(0, $containers->compressedSize());
    }

    public function test_constructor_with_multiple_data_containers() : void
    {
        $container1 = $this->createDataPageContainer(10);
        $container2 = $this->createDataPageContainer(20);
        $container3 = $this->createDataPageContainer(30);

        $containers = new PageContainers([$container1, $container2, $container3]);

        self::assertCount(3, $containers->dataPageContainers());
        self::assertSame($container1, $containers->dataPageContainers()[0]);
        self::assertSame($container2, $containers->dataPageContainers()[1]);
        self::assertSame($container3, $containers->dataPageContainers()[2]);
    }

    #[DataProvider('container_count_provider')]
    public function test_constructor_with_varying_container_counts(int $count) : void
    {
        $pageContainers = [];

        for ($i = 0; $i < $count; $i++) {
            $pageContainers[] = $this->createDataPageContainer($i + 1);
        }

        $containers = new PageContainers($pageContainers);

        self::assertCount($count, $containers->dataPageContainers());

        for ($i = 0; $i < $count; $i++) {
            self::assertSame($pageContainers[$i], $containers->dataPageContainers()[$i]);
        }
    }

    public function test_data_page_containers_returns_empty_array_initially() : void
    {
        $containers = new PageContainers();

        $result = $containers->dataPageContainers();

        self::assertIsArray($result);
        self::assertEmpty($result);
    }

    public function test_dictionary_page_container_returns_null_initially() : void
    {
        $containers = new PageContainers();

        $result = $containers->dictionaryPageContainer();

        self::assertNull($result);
    }

    public function test_encodings_with_data_page_header() : void
    {
        $containers = new PageContainers();
        $pageContainer = $this->createDataPageContainer();

        $containers->add($pageContainer);

        $encodings = $containers->encodings();

        self::assertContains(Encodings::PLAIN, $encodings);
        self::assertContains(Encodings::RLE, $encodings);
    }

    public function test_encodings_with_data_page_header_v2() : void
    {
        $containers = new PageContainers();
        $pageContainer = $this->createDataPageV2Container();

        $containers->add($pageContainer);

        $encodings = $containers->encodings();

        self::assertContains(Encodings::PLAIN, $encodings);
    }

    public function test_encodings_with_dictionary_page() : void
    {
        $containers = new PageContainers();
        $pageContainer = $this->createDictionaryPageContainer();

        $containers->add($pageContainer);

        $encodings = $containers->encodings();

        self::assertContains(Encodings::RLE_DICTIONARY, $encodings);
    }

    public function test_encodings_with_empty_containers() : void
    {
        $containers = new PageContainers();

        $encodings = $containers->encodings();

        self::assertEmpty($encodings);
    }

    public function test_encodings_with_mixed_page_types() : void
    {
        $containers = new PageContainers();
        $dataContainer = $this->createDataPageContainer();
        $dictionaryContainer = $this->createDictionaryPageContainer();
        $dataV2Container = $this->createDataPageV2Container();

        $containers->add($dataContainer);
        $containers->add($dictionaryContainer);
        $containers->add($dataV2Container);

        $encodings = $containers->encodings();

        self::assertContains(Encodings::PLAIN, $encodings);
        self::assertContains(Encodings::RLE, $encodings);
        self::assertContains(Encodings::RLE_DICTIONARY, $encodings);
    }

    public function test_encodings_with_multiple_data_pages() : void
    {
        $containers = new PageContainers();
        $pageContainer1 = $this->createDataPageContainer(encoding: Encodings::PLAIN);
        $pageContainer2 = $this->createDataPageContainer(encoding: Encodings::BIT_PACKED);

        $containers->add($pageContainer1);
        $containers->add($pageContainer2);

        $encodings = $containers->encodings();

        self::assertContains(Encodings::PLAIN, $encodings);
        self::assertContains(Encodings::BIT_PACKED, $encodings);
        self::assertContains(Encodings::RLE, $encodings);
    }

    public function test_encodings_with_unique_values_only() : void
    {
        $containers = new PageContainers();
        $pageContainer1 = $this->createDataPageContainer(encoding: Encodings::PLAIN);
        $pageContainer2 = $this->createDataPageContainer(encoding: Encodings::PLAIN);

        $containers->add($pageContainer1);
        $containers->add($pageContainer2);

        $encodings = $containers->encodings();

        $plainCount = array_count_values(array_map(fn ($e) => $e->value, $encodings))[Encodings::PLAIN->value] ?? 0;
        self::assertSame(1, $plainCount);
    }

    public function test_uncompressed_size_with_data_pages_only() : void
    {
        $containers = new PageContainers();
        $pageContainer1 = $this->createDataPageContainer(valuesCount: 10, headerBuffer: 'header1', pageBuffer: 'data1');
        $pageContainer2 = $this->createDataPageContainer(valuesCount: 20, headerBuffer: 'header2', pageBuffer: 'data2');

        $containers->add($pageContainer1);
        $containers->add($pageContainer2);

        $uncompressedSize = $containers->uncompressedSize();

        $expectedSize = $pageContainer1->totalUncompressedSize() + $pageContainer2->totalUncompressedSize();
        self::assertSame($expectedSize, $uncompressedSize);
    }

    public function test_uncompressed_size_with_dictionary_and_data_pages() : void
    {
        $containers = new PageContainers();
        $dataContainer = $this->createDataPageContainer();
        $dictionaryContainer = $this->createDictionaryPageContainer();

        $containers->add($dataContainer);
        $containers->add($dictionaryContainer);

        $uncompressedSize = $containers->uncompressedSize();

        $expectedSize = $dataContainer->totalUncompressedSize() + $dictionaryContainer->totalUncompressedSize();
        self::assertSame($expectedSize, $uncompressedSize);
    }

    public function test_uncompressed_size_with_empty_containers() : void
    {
        $containers = new PageContainers();

        $uncompressedSize = $containers->uncompressedSize();

        self::assertSame(0, $uncompressedSize);
    }

    public function test_values_count_with_data_pages_only() : void
    {
        $containers = new PageContainers();
        $pageContainer1 = $this->createDataPageContainer(valuesCount: 10);
        $pageContainer2 = $this->createDataPageContainer(valuesCount: 20);
        $pageContainer3 = $this->createDataPageContainer(valuesCount: 30);

        $containers->add($pageContainer1);
        $containers->add($pageContainer2);
        $containers->add($pageContainer3);

        $valuesCount = $containers->valuesCount();

        self::assertSame(60, $valuesCount);
    }

    public function test_values_count_with_dictionary_and_data_pages() : void
    {
        $containers = new PageContainers();
        $dataContainer = $this->createDataPageContainer(valuesCount: 15);
        $dictionaryContainer = $this->createDictionaryPageContainer(valuesCount: 5);

        $containers->add($dataContainer);
        $containers->add($dictionaryContainer);

        $valuesCount = $containers->valuesCount();

        self::assertSame(15, $valuesCount);
    }

    public function test_values_count_with_empty_containers() : void
    {
        $containers = new PageContainers();

        $valuesCount = $containers->valuesCount();

        self::assertSame(0, $valuesCount);
    }

    public function test_values_count_with_zero_value_pages() : void
    {
        $containers = new PageContainers();
        $pageContainer1 = $this->createDataPageContainer(valuesCount: 0);
        $pageContainer2 = $this->createDataPageContainer(valuesCount: 0);

        $containers->add($pageContainer1);
        $containers->add($pageContainer2);

        $valuesCount = $containers->valuesCount();

        self::assertSame(0, $valuesCount);
    }

    public function test_workflow_add_dictionary_then_data_pages() : void
    {
        $containers = new PageContainers();
        $dictionaryContainer = $this->createDictionaryPageContainer(valuesCount: 3);
        $dataContainer1 = $this->createDataPageContainer(valuesCount: 10);
        $dataContainer2 = $this->createDataPageContainer(valuesCount: 20);

        $containers->add($dictionaryContainer);
        $containers->add($dataContainer1);
        $containers->add($dataContainer2);

        self::assertSame($dictionaryContainer, $containers->dictionaryPageContainer());
        self::assertCount(2, $containers->dataPageContainers());
        self::assertSame(30, $containers->valuesCount());
        self::assertNotEmpty($containers->buffer());
        self::assertGreaterThan(0, $containers->compressedSize());
        self::assertGreaterThan(0, $containers->uncompressedSize());
    }

    public function test_workflow_data_pages_only() : void
    {
        $containers = new PageContainers();
        $dataContainer1 = $this->createDataPageContainer(valuesCount: 5);
        $dataContainer2 = $this->createDataPageContainer(valuesCount: 10);
        $dataContainer3 = $this->createDataPageContainer(valuesCount: 15);

        $containers->add($dataContainer1);
        $containers->add($dataContainer2);
        $containers->add($dataContainer3);

        self::assertNull($containers->dictionaryPageContainer());
        self::assertCount(3, $containers->dataPageContainers());
        self::assertSame(30, $containers->valuesCount());
        self::assertNotEmpty($containers->buffer());
        self::assertGreaterThan(0, $containers->compressedSize());
        self::assertGreaterThan(0, $containers->uncompressedSize());
        self::assertNotEmpty($containers->encodings());
    }

    private function createDataPageContainer(
        int $valuesCount = 10,
        string $headerBuffer = 'header',
        string $pageBuffer = 'pagedata',
        Encodings $encoding = Encodings::PLAIN,
    ) : PageContainer {
        $dataPageHeader = new DataPageHeader(
            encoding: $encoding,
            repetitionLevelEncoding: Encodings::RLE,
            definitionLevelEncoding: Encodings::RLE,
            valuesCount: $valuesCount
        );

        $pageHeader = new PageHeader(
            Type::DATA_PAGE,
            strlen($pageBuffer),
            strlen($pageBuffer),
            dataPageHeader: $dataPageHeader,
            dataPageHeaderV2: null,
            dictionaryPageHeader: null
        );

        return new PageContainer(
            $headerBuffer,
            $pageBuffer,
            [],
            null,
            $pageHeader
        );
    }

    private function createDataPageV2Container(
        int $valuesCount = 10,
        string $headerBuffer = 'header',
        string $pageBuffer = 'pagedata',
        Encodings $encoding = Encodings::PLAIN,
    ) : PageContainer {
        $dataPageHeaderV2 = new DataPageHeaderV2(
            valuesCount: $valuesCount,
            nullsCount: 0,
            rowsCount: $valuesCount,
            encoding: $encoding,
            definitionsByteLength: 0,
            repetitionsByteLength: 0,
            isCompressed: false,
            statistics: null
        );

        $pageHeader = new PageHeader(
            Type::DATA_PAGE_V2,
            strlen($pageBuffer),
            strlen($pageBuffer),
            dataPageHeader: null,
            dataPageHeaderV2: $dataPageHeaderV2,
            dictionaryPageHeader: null
        );

        return new PageContainer(
            $headerBuffer,
            $pageBuffer,
            [],
            null,
            $pageHeader
        );
    }

    private function createDictionaryPageContainer(
        int $valuesCount = 5,
        string $headerBuffer = 'dictheader',
        string $pageBuffer = 'dictdata',
        Encodings $encoding = Encodings::RLE_DICTIONARY,
    ) : PageContainer {
        $dictionaryPageHeader = new DictionaryPageHeader(
            encoding: $encoding,
            valuesCount: $valuesCount
        );

        $pageHeader = new PageHeader(
            Type::DICTIONARY_PAGE,
            strlen($pageBuffer),
            strlen($pageBuffer),
            dataPageHeader: null,
            dataPageHeaderV2: null,
            dictionaryPageHeader: $dictionaryPageHeader
        );

        return new PageContainer(
            $headerBuffer,
            $pageBuffer,
            [],
            ['value1', 'value2', 'value3', 'value4', 'value5'],
            $pageHeader
        );
    }
}
