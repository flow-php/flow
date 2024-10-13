<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use function Flow\ETL\DSL\config;
use function Flow\Filesystem\DSL\path;
use Flow\CLI\PipelineFactory;
use Flow\ETL\Exception\{InvalidArgumentException, InvalidFileFormatException};
use PHPUnit\Framework\TestCase;

final class PipelineFactoryTest extends TestCase
{
    public function test_empty_php_file() : void
    {
        $this->expectException(InvalidArgumentException::class);

        $factory = new PipelineFactory(path(__DIR__ . '/../Fixtures/empty.php'), config());
        $factory->fromPHP();
    }

    public function test_non_existing_file() : void
    {
        $this->expectException(InvalidFileFormatException::class);

        $factory = new PipelineFactory(path('fake'), config());
        $factory->fromPHP();
    }

    public function test_non_php_file() : void
    {
        $this->expectExceptionMessage('Expected "php" file format, "txt" given.');
        $this->expectException(InvalidFileFormatException::class);

        $factory = new PipelineFactory(path(__DIR__ . '/../Fixtures/empty.txt'), config());
        $factory->fromPHP();
    }

    public function test_with_data_frame_in_file() : void
    {
        $factory = new PipelineFactory(path(__DIR__ . '/../Fixtures/with-dataframe.php'), config());
        $factory->fromPHP();

        $this->addToAssertionCount(1);
    }

    public function test_without_data_frame_in_file() : void
    {
        $this->expectExceptionMessage('Expecting Flow-PHP DataFrame, received: ');
        $this->expectException(InvalidArgumentException::class);

        $factory = new PipelineFactory(path(__DIR__ . '/../Fixtures/without-dataframe.php'), config());
        $factory->fromPHP();
    }
}
