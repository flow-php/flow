<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\API\Config\ConfigBuilder;
use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\Runtime\Config;
use Flow\ETL\Tests\Context\ExtractorClasses;
use Flow\ETL\Tests\Context\MetadataColumnsContract;
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

        foreach (ExtractorClasses::all(ExtractorClasses::root('core/etl')) as $class) {
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
        foreach (ExtractorClasses::all(ExtractorClasses::root('core/etl')) as $class) {
            if (!(new ReflectionClass($class))->implementsInterface(MetadataColumnsExtractor::class)) {
                yield $class => [$class];
            }
        }
    }

    /**
     * @param class-string<Extractor> $class
     */
    #[DataProvider('provide_extractor_classes_without_metadata')]
    public function test_an_extractor_without_metadata_does_not_carry_the_setter(string $class): void
    {
        MetadataColumnsContract::doesNotCarryTheSetter($class);
    }

    public function test_the_capability_is_declared_only_by_sources_that_have_metadata(): void
    {
        static::assertSame(['Flow\ETL\IO\Floe\FloeExtractor'], self::classesDeclaringMetadataColumns());
    }

    public function test_the_census_covers_every_extractor_in_src(): void
    {
        static::assertCount(8, ExtractorClasses::all(ExtractorClasses::root('core/etl')));
    }

    public function test_the_config_delegator_is_gone(): void
    {
        static::assertFalse(method_exists(Config::class, 'shouldPutInputIntoRows'));
        static::assertFalse(method_exists(ConfigBuilder::class, 'putInputIntoRows'));
        static::assertFalse(method_exists(ConfigBuilder::class, 'dontPutInputIntoRows'));
    }
}
