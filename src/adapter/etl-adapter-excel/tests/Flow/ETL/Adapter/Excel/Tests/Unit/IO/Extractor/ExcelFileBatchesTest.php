<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit\IO\Extractor;

use Flow\ETL\Adapter\Excel\IO\Extractor\ExcelFileBatches;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\InferredColumns;
use Flow\ETL\IO\Extractor\File\ReadWindow;
use Flow\ETL\IO\Inference\SchemaInference;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\FileReadMother;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;

use function array_map;
use function array_merge;
use function iterator_to_array;

final class ExcelFileBatchesTest extends FlowTestCase
{
    public function test_a_sampled_sheet_is_read_on_and_gives_the_rows_a_fresh_read_gives(): void
    {
        $before = ExcelFixtureContext::sharedStringsFolders();
        $sniffer = ExcelFixtureContext::sniffer('fixture.xlsx');
        $schema = ExcelFixtureContext::infer($sniffer);
        $sampled = new ExcelFileBatches(ExcelFixtureContext::reader(), $sniffer, null);
        $fresh = new ExcelFileBatches(ExcelFixtureContext::reader(), null, null);

        $fromSample = $sampled->batches(
            FileReadMother::of($schema, null, 2),
            new Criteria(),
            ExcelFixtureContext::source('fixture.xlsx'),
            $schema,
            new PhpBackend(),
            new ReadWindow(),
        );
        $rows = array_merge(...array_map(
            static fn(Rows $batch): array => $batch->toArray(),
            iterator_to_array($fromSample, false),
        ));
        $sampled->close();

        static::assertNotSame([], $rows);
        static::assertSame(0, $fromSample->getReturn());
        static::assertSame(
            $rows,
            array_merge(...array_map(
                static fn(Rows $batch): array => $batch->toArray(),
                iterator_to_array(
                    $fresh->batches(
                        FileReadMother::of($schema, null, 2),
                        new Criteria(),
                        ExcelFixtureContext::source('fixture.xlsx'),
                        $schema,
                        new PhpBackend(),
                        new ReadWindow(),
                    ),
                    false,
                ),
            )),
        );
        static::assertSame([], ExcelFixtureContext::leakedSharedStringsFoldersSince($before));
    }

    public function test_close_releases_the_sampled_sheets_the_read_never_took(): void
    {
        $before = ExcelFixtureContext::sharedStringsFolders();
        $sniffer = ExcelFixtureContext::globSniffer('diverging_header/*.xlsx');
        ExcelFixtureContext::infer($sniffer);

        (new ExcelFileBatches(ExcelFixtureContext::reader(), $sniffer, null))->close();

        static::assertSame([], ExcelFixtureContext::leakedSharedStringsFoldersSince($before));
    }

    public function test_a_sheet_whose_header_diverges_is_refused_and_closed(): void
    {
        $before = ExcelFixtureContext::sharedStringsFolders();
        $sniffer = ExcelFixtureContext::sniffer('diverging_header/a.xlsx');
        $schema = ExcelFixtureContext::infer($sniffer);
        $sniffer->close();

        try {
            iterator_to_array((new ExcelFileBatches(
                ExcelFixtureContext::reader(),
                null,
                new InferredColumns($schema, [], new SchemaInference(), 'a.xlsx'),
            ))->batches(
                FileReadMother::of($schema, null, 2),
                new Criteria(),
                ExcelFixtureContext::source('diverging_header/b.xlsx'),
                $schema,
                new PhpBackend(),
                new ReadWindow(),
            ));
            static::fail('a diverging header must be refused');
        } catch (InferredSchemaException $e) {
            static::assertStringContainsString('the schema inferred from a.xlsx', $e->getMessage());
        }

        static::assertSame([], ExcelFixtureContext::leakedSharedStringsFoldersSince($before));
    }
}
