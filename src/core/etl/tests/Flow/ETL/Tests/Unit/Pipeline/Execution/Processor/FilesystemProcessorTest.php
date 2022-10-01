<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Pipeline\Execution\Processor;

use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;
use Flow\ETL\Config;
use Flow\ETL\ConfigBuilder;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\FileExtractor;
use Flow\ETL\Filesystem;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Pipeline\Execution\LogicalPlan;
use Flow\ETL\Pipeline\Execution\Processor\FilesystemProcessor;
use Flow\ETL\Pipeline\Pipes;
use Flow\ETL\Rows;
use Flow\ETL\Tests\Double\EmptyExtractor;

final class FilesystemProcessorTest extends TestCase
{
    public function test_append_in_non_thread_safe_mode() : void
    {
        $loader = new class() implements FileLoader, Loader {
            public function destination() : Path
            {
                return new Path(__FILE__);
            }

            public function load(Rows $rows, FlowContext $context) : void
            {
            }

            public function __serialize() : array
            {
                return [];
            }

            public function __unserialize(array $data) : void
            {
            }
        };

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Appending to destination "file:/' . __FILE__ . '" in non thread safe mode is not supported.');

        $processor = new FilesystemProcessor();

        $this->assertEquals(
            new LogicalPlan(new EmptyExtractor(), Pipes::empty()),
            $processor->process(
                new LogicalPlan(new EmptyExtractor(), new Pipes([$loader])),
                (new FlowContext((new ConfigBuilder())->build()))->setMode(SaveMode::Append)
            )
        );
    }

    public function test_append_in_thread_safe_mode_to_existing_file() : void
    {
        $loader = new class() implements FileLoader, Loader {
            public function destination() : Path
            {
                return new Path(__FILE__);
            }

            public function load(Rows $rows, FlowContext $context) : void
            {
            }

            public function __serialize() : array
            {
                return [];
            }

            public function __unserialize(array $data) : void
            {
            }
        };

        $fs = $this->createMock(Filesystem::class);
        $fs->method('fileExists')
            ->with(new Path(__FILE__))
            ->willReturn(true);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Appending to existing single file destination "file:/' . __FILE__ . '" in non thread safe mode is not supported.');

        $processor = new FilesystemProcessor();

        $this->assertEquals(
            new LogicalPlan(new EmptyExtractor(), Pipes::empty()),
            $processor->process(
                new LogicalPlan(new EmptyExtractor(), new Pipes([$loader])),
                (new FlowContext((new ConfigBuilder())->filesystem($fs)->build()))->setMode(SaveMode::Append)->setThreadSafe()
            )
        );
    }

    public function test_exception_if_exists() : void
    {
        $loader = new class() implements FileLoader, Loader {
            public function destination() : Path
            {
                return new Path(__FILE__);
            }

            public function load(Rows $rows, FlowContext $context) : void
            {
            }

            public function __serialize() : array
            {
                return [];
            }

            public function __unserialize(array $data) : void
            {
            }
        };

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Destination path "file:/' . __FILE__ . '" already exists, please change path to different or set different SaveMode');

        $processor = new FilesystemProcessor();

        $processor->process(
            new LogicalPlan(new EmptyExtractor(), new Pipes([$loader])),
            (new FlowContext(Config::default()))->setMode(SaveMode::ExceptionIfExists)
        );
    }

    public function test_ignore_mode() : void
    {
        $loader = new class() implements FileLoader, Loader {
            public function destination() : Path
            {
                return new Path(__FILE__);
            }

            public function load(Rows $rows, FlowContext $context) : void
            {
            }

            public function __serialize() : array
            {
                return [];
            }

            public function __unserialize(array $data) : void
            {
            }
        };

        $fs = $this->createMock(Filesystem::class);
        $fs->method('exists')
            ->with(new Path(__FILE__))
            ->willReturn(true);

        $processor = new FilesystemProcessor();

        $this->assertEquals(
            new LogicalPlan(new EmptyExtractor(), Pipes::empty()),
            $processor->process(
                new LogicalPlan(new EmptyExtractor(), new Pipes([$loader])),
                (new FlowContext((new ConfigBuilder())->filesystem($fs)->build()))->setMode(SaveMode::Ignore)
            )
        );
    }

    public function test_non_existing_extractor_file() : void
    {
        $extractor = new class() implements Extractor, FileExtractor {
            public function extract(FlowContext $context) : \Generator
            {
                yield new Rows();
            }

            public function source() : Path
            {
                return new Path(__DIR__ . '/non/existing/file.php');
            }
        };

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not existing path used to extract data: ' . $extractor->source()->uri());

        $processor = new FilesystemProcessor();

        $processor->process(
            new LogicalPlan($extractor, Pipes::empty()),
            new FlowContext(Config::default())
        );
    }

    public function test_overwrite_mode() : void
    {
        $loader = new class() implements FileLoader, Loader {
            public function destination() : Path
            {
                return new Path(__FILE__);
            }

            public function load(Rows $rows, FlowContext $context) : void
            {
            }

            public function __serialize() : array
            {
                return [];
            }

            public function __unserialize(array $data) : void
            {
            }
        };

        $fs = $this->createMock(Filesystem::class);
        $fs->method('exists')
            ->with(new Path(__FILE__))
            ->willReturn(true);

        $fs->expects($this->once())
            ->method('rm')
            ->with(new Path(__FILE__));

        $processor = new FilesystemProcessor();

        $processor->process(
            new LogicalPlan(new EmptyExtractor(), new Pipes([$loader])),
            (new FlowContext((new ConfigBuilder())->filesystem($fs)->build()))->setMode(SaveMode::Overwrite)
        );
    }
}
