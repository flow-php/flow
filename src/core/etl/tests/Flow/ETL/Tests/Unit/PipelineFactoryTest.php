<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\PipelineFactory;
use PHPUnit\Framework\TestCase;

final class PipelineFactoryTest extends TestCase
{
    public function testEmptyPhpFile() : void
    {
        $this->expectExceptionMessage('Input file must be a valid PHP one!');
        $this->expectException(InvalidArgumentException::class);

        $factory = new PipelineFactory(__DIR__ . '/../Fixtures/empty.php');
        $factory->run();
    }

    public function testNonExistingFile() : void
    {
        $this->expectExceptionMessage("Input file (fake) doesn't exists!");
        $this->expectException(InvalidArgumentException::class);

        $factory = new PipelineFactory('fake');
        $factory->run();
    }

    public function testNonPhpFile() : void
    {
        $this->expectExceptionMessage('Input file must be a PHP one!');
        $this->expectException(InvalidArgumentException::class);

        $factory = new PipelineFactory(__DIR__ . '/../Fixtures/empty.txt');
        $factory->run();
    }

    public function testWithDataFrameInFile() : void
    {
        $factory = new PipelineFactory(__DIR__ . '/../Fixtures/with-dataframe.php');
        $factory->run();

        $this->addToAssertionCount(1);
    }

    public function testWithoutDataFrameInFile() : void
    {
        $this->expectExceptionMessage('Expecting Flow-PHP DataFrame, received: ');
        $this->expectException(InvalidArgumentException::class);

        $factory = new PipelineFactory(__DIR__ . '/../Fixtures/without-dataframe.php');
        $factory->run();
    }
}
