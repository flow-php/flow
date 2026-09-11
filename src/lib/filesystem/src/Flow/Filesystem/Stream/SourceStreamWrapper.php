<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\SourceStream;

use function array_key_exists;
use function in_array;
use function str_contains;
use function str_starts_with;
use function stream_get_wrappers;
use function stream_wrapper_register;
use function strlen;
use function substr;

/**
 * Serves a SourceStream, read-only and forward-only, to a native reader that opens nothing but a URI, like
 * XMLReader::open().
 */
final class SourceStreamWrapper implements StreamWrapper
{
    public const PROTOCOL = 'flow-source';

    /**
     * @var null|resource
     */
    public $context;

    /**
     * @var array<string, array{SourceStream, int<1, max>}>
     */
    private static array $pending = [];

    private static int $sequence = 0;

    private string $buffer = '';

    private int $bufferPosition = 0;

    private bool $eof = false;

    /**
     * @var int<1, max>
     */
    private int $length = 8192;

    private int $offset = 0;

    private SourceStream $source;

    public static function register(): void
    {
        if (!in_array(self::PROTOCOL, stream_get_wrappers(), true)) {
            stream_wrapper_register(self::PROTOCOL, self::class);
        }
    }

    /**
     * The URI opens $source once. The source stays the caller's to close.
     *
     * @param int<1, max> $length bytes requested from $source per read
     */
    public static function uri(SourceStream $source, int $length = 8192): string
    {
        self::register();

        $uri = self::PROTOCOL . '://' . ++self::$sequence;
        self::$pending[$uri] = [$source, $length];

        return $uri;
    }

    public function stream_close(): void {}

    public function stream_eof(): bool
    {
        return $this->eof;
    }

    public function stream_flush(): bool
    {
        return false;
    }

    public function stream_lock(int $operation): bool
    {
        return false;
    }

    public function stream_open(string $path, string $mode, int $options, ?string &$opened_path): bool
    {
        if (!str_starts_with($mode, 'r') || str_contains($mode, '+') || !array_key_exists($path, self::$pending)) {
            return false;
        }

        [$this->source, $this->length] = self::$pending[$path];
        unset(self::$pending[$path]);

        return true;
    }

    public function stream_read(int $count): string|false
    {
        if ($this->bufferPosition >= strlen($this->buffer)) {
            if ($this->eof) {
                return '';
            }

            $this->buffer = $this->source->read($this->length, $this->offset);
            $this->bufferPosition = 0;
            $this->offset += strlen($this->buffer);

            if ($this->buffer === '') {
                $this->eof = true;

                return '';
            }
        }

        $chunk = substr($this->buffer, $this->bufferPosition, $count);
        $this->bufferPosition += strlen($chunk);

        return $chunk;
    }

    public function stream_seek(int $offset, int $whence = SEEK_SET): bool
    {
        return false;
    }

    public function stream_stat(): array|false
    {
        return false;
    }

    public function stream_tell(): int
    {
        return $this->offset - strlen($this->buffer) + $this->bufferPosition;
    }

    public function stream_write(string $data): int
    {
        return 0;
    }

    /**
     * A URI not opened yet exists; libxml stats it before it opens it.
     */
    public function url_stat(string $path, int $flags): array|false
    {
        return array_key_exists($path, self::$pending) ? ['mode' => 0o100444] : false;
    }
}
