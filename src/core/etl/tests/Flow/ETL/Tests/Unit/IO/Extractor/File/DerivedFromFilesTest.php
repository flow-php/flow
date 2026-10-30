<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\IO\Extractor\File\DerivedFromFiles;
use Flow\ETL\Tests\Context\SelfDescribingFilesContext;
use Flow\ETL\Tests\Double\FakeSelfDescribingFile;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema;

use function array_slice;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class DerivedFromFilesTest extends FlowTestCase
{
    public function test_an_empty_listing_derives_an_empty_schema(): void
    {
        static::assertEquals(schema(), (new DerivedFromFiles())->fold(
            SelfDescribingFilesContext::describing()->generator(),
            false,
        ));
    }

    public function test_every_file_but_the_first_is_closed_the_first_stays_with_its_owner(): void
    {
        $files = SelfDescribingFilesContext::describing(schema(int_schema('id')), schema(str_schema('name')));

        (new DerivedFromFiles())->fold($files->generator(), true);

        static::assertSame(array_slice($files->files, 1), $files->closed());
    }

    public function test_a_first_file_that_cannot_describe_itself_stays_with_its_owner(): void
    {
        $files = SelfDescribingFilesContext::failing();

        try {
            (new DerivedFromFiles())->fold($files->generator(), false);
        } catch (RuntimeException) {
            static::assertSame([], $files->closed());

            return;
        }

        static::fail('the fold swallowed the describing failure');
    }

    public function test_the_fold_stops_at_the_first_file(): void
    {
        $files = SelfDescribingFilesContext::describing(schema(int_schema('id')), schema(str_schema('name')));

        static::assertEquals(schema(int_schema('id')), (new DerivedFromFiles())->fold($files->generator(), false));
        static::assertSame(1, $files->advanced);
    }

    public function test_the_memo_never_starts_the_generator_again(): void
    {
        $derived = new DerivedFromFiles();
        $derived->fold(SelfDescribingFilesContext::describing(schema(int_schema('id')))->generator(), false);

        $second = SelfDescribingFilesContext::describing(schema(str_schema('name')));

        static::assertEquals(schema(int_schema('id')), $derived->fold($second->generator(), false));
        static::assertSame(0, $second->advanced);
    }

    public function test_union_by_name_folds_every_file(): void
    {
        $files = SelfDescribingFilesContext::describing(schema(int_schema('id')), schema(str_schema('name')));

        static::assertEquals(
            schema(int_schema('id', nullable: true), str_schema('name', nullable: true)),
            (new DerivedFromFiles())->fold($files->generator(), true),
        );
        static::assertSame(2, $files->advanced);
    }

    public function test_the_fold_remembers_the_file_it_read_first(): void
    {
        $derived = new DerivedFromFiles();
        $derived->fold(
            SelfDescribingFilesContext::describing(schema(int_schema('id')), schema(int_schema('id')))->generator(),
            true,
        );

        static::assertSame('memory://orders/file0.csv', $derived->from());
    }

    public function test_nothing_folded_was_read_from_no_file(): void
    {
        static::assertSame('', (new DerivedFromFiles())->from());
    }

    public function test_footers_count_the_files_the_fold_read(): void
    {
        static::assertSame(
            2,
            (new DerivedFromFiles())->footers(
                SelfDescribingFilesContext::describing(schema(int_schema('id')), schema(int_schema('id')))->generator(),
                true,
            )->files,
        );
    }

    public function test_footers_never_read_a_file_the_fold_already_did(): void
    {
        $derived = new DerivedFromFiles();
        $derived->fold(SelfDescribingFilesContext::describing(schema(int_schema('id')))->generator(), false);

        $second = SelfDescribingFilesContext::describing(schema(int_schema('id')));

        static::assertSame(1, $derived->footers($second->generator(), false)->files);
        static::assertSame(0, $second->advanced);
    }

    public function test_a_learned_schema_has_no_footers_and_reads_none(): void
    {
        $derived = new DerivedFromFiles();
        $derived->learn(schema(int_schema('id')));
        $files = SelfDescribingFilesContext::describing(schema(str_schema('name')));

        try {
            $derived->footers($files->generator(), false);

            static::fail('Expected footers() to refuse a learned schema.');
        } catch (InvalidLogicException $e) {
            static::assertSame('No footer was folded: the schema was learned', $e->getMessage());
        }

        static::assertSame(0, $files->advanced);
    }

    public function test_a_learned_schema_remembers_the_source_it_was_read_from(): void
    {
        $derived = new DerivedFromFiles();

        $derived->learn(schema(int_schema('id')), 'file:///a.csv');
        static::assertSame('file:///a.csv', $derived->from());

        $derived->learn(schema(int_schema('id')));
        static::assertSame('', $derived->from());
    }

    public function test_a_learned_schema_is_served_back(): void
    {
        $derived = new DerivedFromFiles();

        static::assertNull($derived->schema());
        static::assertEquals(schema(int_schema('id')), $derived->learn(schema(int_schema('id'))));
        static::assertEquals(schema(int_schema('id')), $derived->schema());
    }

    public function test_a_learned_schema_is_what_the_fold_returns(): void
    {
        $derived = new DerivedFromFiles();
        $derived->learn(schema(int_schema('id')));

        static::assertEquals(
            schema(int_schema('id')),
            $derived->fold(SelfDescribingFilesContext::describing(schema(str_schema('name')))->generator(), false),
        );
    }

    public function test_a_later_file_that_cannot_describe_itself_is_closed(): void
    {
        $files = new SelfDescribingFilesContext(
            new FakeSelfDescribingFile(schema(int_schema('id')), SelfDescribingFilesContext::source(0)),
            new FakeSelfDescribingFile(new Schema(), SelfDescribingFilesContext::source(1), describingThrows: true),
        );

        try {
            (new DerivedFromFiles())->fold($files->generator(), true);
        } catch (RuntimeException) {
            static::assertSame([$files->files[1]], $files->closed());

            return;
        }

        static::fail('the fold swallowed the describing failure');
    }
}
