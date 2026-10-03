<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Excel\Tests\Unit;

use Flow\ETL\Adapter\Excel\ExcelFileBatches;
use Flow\ETL\Adapter\Excel\Tests\Context\ExcelFixtureContext;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Exception\InferredSchemaException;
use Flow\ETL\Extractor\File\InferredColumns;
use Flow\ETL\Extractor\File\ReadWindow;
use Flow\ETL\Rows;
use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Tests\FlowTestCase;

use function array_map;
use function array_merge;
use function iterator_to_array;

final class ExcelFileBatchesTest extends FlowTestCase
{
    public function test_a_sampled_sheet_is_read_on_and_gives_the_rows_a_fresh_read_gives(): void
    {
        $before = ExcelFixtureContext::sharedStringsFolders();
        $sampler = ExcelFixtureContext::sampler('fixture.xlsx');
        $schema = ExcelFixtureContext::infer($sampler);
        $sampled = new ExcelFileBatches(ExcelFixtureContext::reader(), $sampler, null);
        $fresh = new ExcelFileBatches(ExcelFixtureContext::reader(), null, null);

        $fromSample = $sampled->batches(
            ExcelFixtureContext::source('fixture.xlsx'),
            $schema,
            2,
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
                        ExcelFixtureContext::source('fixture.xlsx'),
                        $schema,
                        2,
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
        $sampler = ExcelFixtureContext::globSampler('diverging_header/*.xlsx');
        ExcelFixtureContext::infer($sampler);

        (new ExcelFileBatches(ExcelFixtureContext::reader(), $sampler, null))->close();

        static::assertSame([], ExcelFixtureContext::leakedSharedStringsFoldersSince($before));
    }

    public function test_a_sheet_whose_header_diverges_is_refused_and_closed(): void
    {
        $before = ExcelFixtureContext::sharedStringsFolders();
        $sampler = ExcelFixtureContext::sampler('diverging_header/a.xlsx');
        $schema = ExcelFixtureContext::infer($sampler);
        $sampler->close();

        try {
            iterator_to_array((new ExcelFileBatches(
                ExcelFixtureContext::reader(),
                null,
                new InferredColumns($schema, [], new SchemaInference(), 'a.xlsx'),
            ))->batches(
                ExcelFixtureContext::source('diverging_header/b.xlsx'),
                $schema,
                2,
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
