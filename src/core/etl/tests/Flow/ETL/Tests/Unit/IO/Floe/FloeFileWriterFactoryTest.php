<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\IO\Floe;

use Flow\ETL\IO\Floe\FloeFileWriterFactory;
use Flow\ETL\Storage\Floe\Options;
use Flow\ETL\Tests\Context\FloeFilesContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FloeFileWriterFactoryTest extends FlowTestCase
{
    public function test_every_stream_of_a_run_is_written_under_the_first_opened_stream_schema(): void
    {
        $filesystem = memory_filesystem();
        $writers = new FloeFileWriterFactory(null, null, new Options());
        $schema = schema(int_schema('id'), str_schema('name', nullable: true));
        $first = $writers->writer($filesystem->writeTo(path('memory://a.floe')), $schema, new PhpBackend());
        $second = $writers->writer(
            $filesystem->writeTo(path('memory://b.floe')),
            schema(int_schema('id'), str_schema('name')),
            new PhpBackend(),
        );

        $first->write(array_to_rows([['id' => 1, 'name' => 'a']], $schema));
        $second->write(array_to_rows([['id' => 2, 'name' => 'b']], schema(int_schema('id'), str_schema('name'))));
        $first->close();
        $second->close();

        static::assertEquals($schema, FloeFilesContext::files($filesystem)->read(path('memory://b.floe'))->schema());
    }

    public function test_a_declared_schema_is_the_written_one(): void
    {
        $filesystem = memory_filesystem();
        $declared = schema(int_schema('id', nullable: true));
        $writer = (new FloeFileWriterFactory($declared, null, new Options()))->writer(
            $filesystem->writeTo(path('memory://a.floe')),
            schema(int_schema('id')),
            new PhpBackend(),
        );

        $writer->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $writer->close();

        static::assertEquals($declared, FloeFilesContext::files($filesystem)->read(path('memory://a.floe'))->schema());
    }
}
