<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Extractor;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor\SourceFile;
use Flow\ETL\Tests\Context\SelfDescribingFilesContext;
use Flow\ETL\Tests\Double\FileReadingExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Path\Filter\OnlyFiles;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class FileReadingTest extends FlowTestCase
{
    public function test_an_empty_listing_derives_an_empty_schema(): void
    {
        static::assertEquals(
            schema(),
            (new FileReadingExtractor())->derive(SelfDescribingFilesContext::describing()->generator()),
        );
    }

    public function test_every_opened_file_is_closed(): void
    {
        $files = SelfDescribingFilesContext::describing(schema(int_schema('id')), schema(str_schema('name')));

        (new FileReadingExtractor())->derive($files->generator(), unionByName: true);

        static::assertSame($files->files, $files->closed());
    }

    public function test_the_file_is_closed_when_it_cannot_describe_itself(): void
    {
        $files = SelfDescribingFilesContext::failing();

        try {
            (new FileReadingExtractor())->derive($files->generator());
        } catch (RuntimeException) {
            static::assertSame($files->files, $files->closed());

            return;
        }

        static::fail('the fold swallowed the describing failure');
    }

    public function test_the_fold_stops_at_the_first_file(): void
    {
        $files = SelfDescribingFilesContext::describing(schema(int_schema('id')), schema(str_schema('name')));

        static::assertEquals(schema(int_schema('id')), (new FileReadingExtractor())->derive($files->generator()));
        static::assertSame(1, $files->advanced);
    }

    public function test_the_memo_never_starts_the_generator_again(): void
    {
        $extractor = new FileReadingExtractor();
        $extractor->derive(SelfDescribingFilesContext::describing(schema(int_schema('id')))->generator());

        $second = SelfDescribingFilesContext::describing(schema(str_schema('name')));

        static::assertEquals(schema(int_schema('id')), $extractor->derive($second->generator()));
        static::assertSame(0, $second->advanced);
    }

    public function test_the_path_filter_forgets_the_memo(): void
    {
        $extractor = new FileReadingExtractor();
        $extractor->derive(SelfDescribingFilesContext::describing(schema(int_schema('id')))->generator());
        $extractor->withPathFilter(new OnlyFiles());

        static::assertEquals(
            schema(str_schema('name')),
            $extractor->derive(SelfDescribingFilesContext::describing(schema(str_schema('name')))->generator()),
        );
    }

    public function test_union_by_name_folds_every_file(): void
    {
        $files = SelfDescribingFilesContext::describing(schema(int_schema('id')), schema(str_schema('name')));

        static::assertEquals(
            schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
            (new FileReadingExtractor())->derive($files->generator(), unionByName: true),
        );
        static::assertSame(2, $files->advanced);
    }

    public function test_source_files_yields_one_source_per_listed_path(): void
    {
        $filesystem = memory_filesystem();
        $filesystem->writeTo(path('memory://orders/year=2024/a.csv'))->close();
        $filesystem->writeTo(path('memory://orders/year=2025/b.csv'))->close();

        static::assertEquals(
            [
                new SourceFile(path('memory://orders/year=2024/a.csv')),
                new SourceFile(path('memory://orders/year=2025/b.csv')),
            ],
            iterator_to_array(
                (new FileReadingExtractor())->listing($filesystem, path('memory://orders/*/*.csv')),
                false,
            ),
        );
    }
}
