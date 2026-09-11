<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Stream;

use Flow\Filesystem\Stream\SourceStreamWrapper;
use Flow\Filesystem\Stream\StringSourceStream;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use XMLReader;

use function clearstatcache;
use function fclose;
use function feof;
use function file_exists;
use function Flow\Filesystem\DSL\path;
use function fopen;
use function fread;
use function ftell;
use function stream_get_contents;

final class SourceStreamWrapperTest extends TestCase
{
    /**
     * @param int<1, max> $length
     */
    #[TestWith([1])]
    #[TestWith([4])]
    #[TestWith([8192])]
    public function test_a_native_handle_reads_the_whole_source(int $length): void
    {
        $handle = fopen(
            SourceStreamWrapper::uri(new StringSourceStream(path('memory://a.txt'), 'foobarbaz'), $length),
            'rb',
        );

        static::assertNotFalse($handle);
        static::assertSame('foobarbaz', stream_get_contents($handle));
        static::assertTrue(feof($handle));

        fclose($handle);
    }

    public function test_reads_smaller_than_the_refill_are_served_from_it(): void
    {
        $handle = fopen(
            SourceStreamWrapper::uri(new StringSourceStream(path('memory://a.txt'), 'foobarbaz'), 8192),
            'rb',
        );

        static::assertNotFalse($handle);
        static::assertSame('foo', fread($handle, 3));
        static::assertSame(3, ftell($handle));
        static::assertSame('barbaz', fread($handle, 6));

        fclose($handle);
    }

    public function test_an_empty_source_is_at_its_end_after_the_first_read(): void
    {
        $handle = fopen(SourceStreamWrapper::uri(new StringSourceStream(path('memory://a.txt'), '')), 'rb');

        static::assertNotFalse($handle);
        static::assertSame('', stream_get_contents($handle));
        static::assertTrue(feof($handle));

        fclose($handle);
    }

    public function test_a_uri_exists_until_it_is_opened(): void
    {
        $uri = SourceStreamWrapper::uri(new StringSourceStream(path('memory://a.txt'), 'foo'));

        static::assertTrue(file_exists($uri));

        $handle = fopen($uri, 'rb');

        clearstatcache();

        static::assertNotFalse($handle);
        static::assertFalse(file_exists($uri));

        fclose($handle);
    }

    public function test_a_uri_opens_once(): void
    {
        $uri = SourceStreamWrapper::uri(new StringSourceStream(path('memory://a.txt'), 'foo'));
        $opened = null;

        static::assertTrue((new SourceStreamWrapper())->stream_open($uri, 'rb', 0, $opened));
        static::assertFalse((new SourceStreamWrapper())->stream_open($uri, 'rb', 0, $opened));
    }

    #[TestWith(['w'])]
    #[TestWith(['a'])]
    #[TestWith(['r+'])]
    public function test_a_writing_mode_does_not_open(string $mode): void
    {
        $opened = null;

        static::assertFalse((new SourceStreamWrapper())->stream_open(
            SourceStreamWrapper::uri(new StringSourceStream(path('memory://a.txt'), 'foo')),
            $mode,
            0,
            $opened,
        ));
    }

    public function test_an_unknown_uri_does_not_open(): void
    {
        $opened = null;

        static::assertFalse((new SourceStreamWrapper())->stream_open(
            SourceStreamWrapper::PROTOCOL . '://unknown',
            'rb',
            0,
            $opened,
        ));
    }

    public function test_it_neither_seeks_writes_locks_nor_flushes(): void
    {
        $wrapper = new SourceStreamWrapper();

        static::assertFalse($wrapper->stream_seek(0));
        static::assertSame(0, $wrapper->stream_write('x'));
        static::assertFalse($wrapper->stream_lock(LOCK_SH));
        static::assertFalse($wrapper->stream_flush());
        static::assertFalse($wrapper->stream_stat());
    }

    public function test_xml_reader_opens_a_source(): void
    {
        $reader = new XMLReader();

        static::assertTrue($reader->open(SourceStreamWrapper::uri(
            new StringSourceStream(path('memory://a.xml'), '<root><item>1</item></root>'),
            4,
        )));
        static::assertTrue($reader->read());
        static::assertSame('root', $reader->name);
        static::assertSame('<root><item>1</item></root>', $reader->readOuterXml());

        $reader->close();
    }
}
