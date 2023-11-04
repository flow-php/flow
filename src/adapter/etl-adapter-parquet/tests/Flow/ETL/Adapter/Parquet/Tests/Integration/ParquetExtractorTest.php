<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\ETL\Config;
use Flow\ETL\DSL\From;
use Flow\ETL\DSL\Parquet;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Flow;
use Flow\ETL\FlowContext;
use Flow\Parquet\Options;
use PHPUnit\Framework\TestCase;

final class ParquetExtractorTest extends TestCase
{
    public function test_limit() : void
    {
        $path = \sys_get_temp_dir() . '/parquet_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())->read(From::array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(Parquet::to($path))
            ->run();

        $extractor = new ParquetExtractor(Path::realpath($path), Options::default());
        $extractor->changeLimit(2);

        $this->assertCount(
            2,
            \iterator_to_array($extractor->extract(new FlowContext(Config::default())))
        );
    }

    public function test_signal_stop() : void
    {
        $path = \sys_get_temp_dir() . '/parquet_extractor_signal_stop.csv';

        if (\file_exists($path)) {
            \unlink($path);
        }

        (new Flow())->read(From::array([['id' => 1], ['id' => 2], ['id' => 3], ['id' => 4], ['id' => 5]]))
            ->write(Parquet::to($path))
            ->run();

        $extractor = new ParquetExtractor(Path::realpath($path), Options::default());

        $generator = $extractor->extract(new FlowContext(Config::default()));

        $this->assertSame([['id' => 1]], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame([['id' => 2]], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->next();
        $this->assertSame([['id' => 3]], $generator->current()->toArray());
        $this->assertTrue($generator->valid());
        $generator->send(Signal::STOP);
        $this->assertFalse($generator->valid());
    }
}
