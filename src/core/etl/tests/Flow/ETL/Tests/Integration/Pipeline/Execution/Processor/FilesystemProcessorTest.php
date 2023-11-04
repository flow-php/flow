<?php declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Pipeline\Execution\Processor;

use Flow\ETL\Config;
use Flow\ETL\DSL\CSV;
use Flow\ETL\DSL\From;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\ETL\Pipeline\Execution\ExecutionPlan;
use Flow\ETL\Pipeline\Execution\Processor\FilesystemProcessor;
use Flow\ETL\Pipeline\Pipes;
use PHPUnit\Framework\TestCase;

final class FilesystemProcessorTest extends TestCase
{
    public function test_append_mode_to_a_single_file() : void
    {
        $path = \sys_get_temp_dir() . '/flow-etl-filesystem-processor-test-overwrite-mode.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())
            ->read(From::array([['id' => 1], ['id' => 2]]))
            ->write(CSV::to($path))
            ->run();

        $processor = new FilesystemProcessor();
        $extractor = CSV::from($path);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Appending to existing single file destination \"file:/{$path}\" is not supported.");

        $processor->process(
            new ExecutionPlan(
                $extractor,
                new Pipes([CSV::to($path)])
            ),
            (new FlowContext(Config::default()))
                ->setThreadSafe()
                ->setMode(SaveMode::Append)
        );
    }

    public function test_append_mode_without_thread_safe() : void
    {
        $path = \sys_get_temp_dir() . '/flow-etl-filesystem-processor-test-overwrite-mode.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())
            ->read(From::array([['id' => 1], ['id' => 2]]))
            ->write(CSV::to($path))
            ->run();

        $processor = new FilesystemProcessor();
        $extractor = CSV::from($path);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Appending to destination \"file:/{$path}\" in non thread safe mode is not supported");

        $processor->process(
            new ExecutionPlan(
                $extractor,
                new Pipes([CSV::to($path)])
            ),
            (new FlowContext(Config::default()))->setMode(SaveMode::Append)
        );
    }

    public function test_exception_if_exists_mode() : void
    {
        $path = \sys_get_temp_dir() . '/flow-etl-filesystem-processor-test-overwrite-mode.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())
            ->read(From::array([['id' => 1], ['id' => 2]]))
            ->write(CSV::to($path))
            ->run();

        $processor = new FilesystemProcessor();
        $extractor = CSV::from($path);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage("Destination path \"file:/{$path}\" already exists, please change path to different or set different SaveMode");

        $processor->process(
            new ExecutionPlan(
                $extractor,
                new Pipes([CSV::to($path)])
            ),
            (new FlowContext(Config::default()))->setMode(SaveMode::ExceptionIfExists)
        );
    }

    public function test_ignore_mode() : void
    {
        $path = \sys_get_temp_dir() . '/flow-etl-filesystem-processor-test-overwrite-mode.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())
            ->read(From::array([['id' => 1], ['id' => 2]]))
            ->write(CSV::to($path))
            ->run();

        $processor = new FilesystemProcessor();
        $extractor = CSV::from($path);

        $plan = $processor->process(
            new ExecutionPlan(
                $extractor,
                new Pipes([CSV::to($path)])
            ),
            (new FlowContext(Config::default()))->setMode(SaveMode::Ignore)
        );

        $this->assertFileExists($path);
        $this->assertCount(0, $plan->pipes->all());
    }

    public function test_overwrite_mode() : void
    {
        $path = \sys_get_temp_dir() . '/flow-etl-filesystem-processor-test-overwrite-mode.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())
            ->read(From::array([['id' => 1], ['id' => 2]]))
            ->write(CSV::to($path))
            ->run();

        $processor = new FilesystemProcessor();
        $extractor = CSV::from($path);

        $processor->process(
            new ExecutionPlan(
                $extractor,
                new Pipes([CSV::to($path)])
            ),
            (new FlowContext(Config::default()))->setMode(SaveMode::Overwrite)
        );

        $this->assertFileDoesNotExist($path);
    }

    public function test_throwing_exception_when_extractor_source_does_not_exists() : void
    {
        $processor = new FilesystemProcessor();
        $extractor = CSV::from('/not_existing_file.csv');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Not existing path used to extract data: file://not_existing_file.csv');

        $processor->process(
            new ExecutionPlan(
                $extractor,
                Pipes::empty()
            ),
            (new FlowContext(Config::default()))
        );
    }
}
