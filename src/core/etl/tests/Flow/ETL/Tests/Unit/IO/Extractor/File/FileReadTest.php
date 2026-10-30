<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Extractor\File;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Json;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\File\FileRead;
use Flow\ETL\IO\Extractor\File\SourceFiles;
use Flow\ETL\Tests\Context\FileColumnsContext;
use Flow\ETL\Tests\Context\MemoryFiles;
use Flow\ETL\Tests\Double\Described;
use Flow\ETL\Tests\Double\InMemoryUnskippableFileBatches;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_array;

final class FileReadTest extends FlowTestCase
{
    public function test_it_normalizes_and_rebuilds_equal(): void
    {
        $read = new FileRead(
            new SourceFiles(new NativeLocalFilesystem(), path(__DIR__ . '/*.csv')),
            FileColumnsContext::discovering(['year' => true]),
            schema(int_schema('id')),
            7,
        );
        $description = Description::compose(Described::class, $read->normalize());

        static::assertNotNull($description);
        static::assertEquals($read, FileRead::fromArray(
            type_array()->assert(Json::decode(Json::encode($description->normalize()))['data'] ?? null),
            new Rebuild(),
        ));
    }

    public function test_a_read_skips_the_offset_of_its_criteria(): void
    {
        static::assertSame(
            [['id' => 3]],
            (new FileRead(
                new SourceFiles(MemoryFiles::with(['memory://a.csv' => 'abcd']), path('memory://a.csv')),
                FileColumnsContext::discovering([]),
                schema(int_schema('id')),
                10,
            ))
                ->scan(
                    flow_context()->backend(),
                    new Criteria(offset: 2),
                    new InMemoryUnskippableFileBatches(['memory://a.csv' => [1, 2, 3]]),
                )
                ->current()
                ->toArray(),
        );
    }
}
