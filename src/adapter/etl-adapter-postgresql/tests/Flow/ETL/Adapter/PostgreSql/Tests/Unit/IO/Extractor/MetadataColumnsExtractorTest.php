<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit\IO\Extractor;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\File\MetadataColumnsExtractor;
use Flow\ETL\Tests\Context\ExtractorClasses;
use Flow\ETL\Tests\Context\MetadataColumnsContract;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use ReflectionClass;

final class MetadataColumnsExtractorTest extends FlowTestCase
{
    /**
     * @return Generator<class-string<Extractor>, array{class-string<Extractor>}>
     */
    public static function extractors(): Generator
    {
        foreach (ExtractorClasses::all(ExtractorClasses::root('adapter/etl-adapter-postgresql')) as $class) {
            yield $class => [$class];
        }
    }

    /**
     * @param class-string<Extractor> $class
     */
    #[DataProvider('extractors')]
    public function test_an_extractor_carries_the_metadata_setter_only_when_it_declares_metadata(string $class): void
    {
        if ((new ReflectionClass($class))->implementsInterface(MetadataColumnsExtractor::class)) {
            MetadataColumnsContract::declares($class);

            return;
        }

        MetadataColumnsContract::doesNotCarryTheSetter($class);
    }
}
