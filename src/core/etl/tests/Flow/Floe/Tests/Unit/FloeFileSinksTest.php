<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Unit;

use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Floe\FloeFileSinks;
use Flow\Floe\Options;
use Flow\Floe\Tests\Context\FloeFilesContext;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class FloeFileSinksTest extends FlowTestCase
{
    public function test_every_stream_of_a_run_is_written_under_the_first_batch_schema(): void
    {
        $filesystem = memory_filesystem();
        $sinks = new FloeFileSinks($filesystem, null, null, new Options());
        $schema = schema(int_schema('id'), str_schema('name', nullable: true));
        $first = $sinks->open($filesystem->writeTo(path('memory://a.floe')), new PhpBackend());
        $second = $sinks->open($filesystem->writeTo(path('memory://b.floe')), new PhpBackend());

        $first->write(array_to_rows([['id' => 1, 'name' => 'a']], $schema));
        $second->write(array_to_rows([['id' => 2, 'name' => null]], $schema));
        $first->close();
        $second->close();

        static::assertEquals(
            $schema,
            FloeFilesContext::phpReader($filesystem)->read(path('memory://b.floe'))->schema(),
        );
    }

    public function test_a_declared_schema_is_the_written_one(): void
    {
        $filesystem = memory_filesystem();
        $declared = schema(int_schema('id', nullable: true));
        $sink = (new FloeFileSinks($filesystem, $declared, null, new Options()))->open(
            $filesystem->writeTo(path('memory://a.floe')),
            new PhpBackend(),
        );

        $sink->write(array_to_rows([['id' => 1]], schema(int_schema('id'))));
        $sink->close();

        static::assertEquals(
            $declared,
            FloeFilesContext::phpReader($filesystem)->read(path('memory://a.floe'))->schema(),
        );
    }
}
