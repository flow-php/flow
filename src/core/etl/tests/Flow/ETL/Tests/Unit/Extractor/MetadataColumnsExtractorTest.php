<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Config;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\MetadataColumnsExtractor;
use Flow\ETL\Tests\Context\ExtractorClasses;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use ReflectionClass;

use function method_exists;

final class MetadataColumnsExtractorTest extends FlowTestCase
{
    /**
     * @return list<class-string<Extractor>>
     */
    public static function classesDeclaringMetadataColumns(): array
    {
        $declaring = [];

        foreach (ExtractorClasses::all() as $class) {
            if ((new ReflectionClass($class))->implementsInterface(MetadataColumnsExtractor::class)) {
                $declaring[] = $class;
            }
        }

        return $declaring;
    }

    /**
     * @return \Generator<string, array{class-string<Extractor>}>
     */
    public static function provide_extractor_classes_without_metadata(): Generator
    {
        foreach (ExtractorClasses::all() as $class) {
            if (!(new ReflectionClass($class))->implementsInterface(MetadataColumnsExtractor::class)) {
                yield $class => [$class];
            }
        }
    }

    /**
     * A source that has nothing to describe must not carry the setter at all - a no-op
     * withMetadataColumns() reads as "supported, and it did nothing".
     *
     * @param class-string<Extractor> $class
     */
    #[DataProvider('provide_extractor_classes_without_metadata')]
    public function test_an_extractor_without_metadata_does_not_carry_the_setter(string $class): void
    {
        static::assertFalse(method_exists($class, 'withMetadataColumns'));
    }

    public function test_the_capability_is_declared_only_by_sources_that_have_metadata(): void
    {
        static::assertSame(
            [
                'Flow\ETL\Adapter\CSV\CSVExtractor',
                'Flow\ETL\Adapter\Excel\ExcelExtractor',
                'Flow\ETL\Adapter\GoogleSheet\GoogleSheetExtractor',
                'Flow\ETL\Adapter\JSON\JSONMachine\JsonExtractor',
                'Flow\ETL\Adapter\JSON\JSONMachine\JsonLinesExtractor',
                'Flow\ETL\Adapter\Parquet\ParquetExtractor',
                'Flow\ETL\Adapter\Text\TextExtractor',
                'Flow\ETL\Adapter\XML\XMLParserExtractor',
                'Flow\ETL\Adapter\XML\XMLReaderExtractor',
                'Flow\Floe\FloeExtractor',
            ],
            self::classesDeclaringMetadataColumns(),
        );
    }

    public function test_the_census_covers_every_extractor_in_src(): void
    {
        static::assertCount(33, ExtractorClasses::all());
    }

    public function test_the_config_delegator_is_gone(): void
    {
        static::assertFalse(method_exists(Config::class, 'shouldPutInputIntoRows'));
        static::assertFalse(method_exists(ConfigBuilder::class, 'putInputIntoRows'));
        static::assertFalse(method_exists(ConfigBuilder::class, 'dontPutInputIntoRows'));
    }
}
