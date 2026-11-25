<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Double;

use RuntimeException;

use function max;
use function min;
use function parse_url;
use function str_repeat;

final class NeverEofStreamWrapper
{
    /** @var null|resource */
    public $context;

    private int $position = 0;

    private int $readsPastEnd = 0;

    private int $size = 0;

    public function stream_eof(): bool
    {
        return false;
    }

    public function stream_open(string $path, string $mode, int $options, ?string &$openedPath): bool
    {
        // the host of the URL is the size of the stream in bytes: never-eof://2560
        $this->size = (int) parse_url($path, PHP_URL_HOST);

        return true;
    }

    public function stream_read(int $count): string
    {
        if ($this->position >= $this->size) {
            if (++$this->readsPastEnd >= 3) {
                throw new RuntimeException('NeverEofStreamWrapper was read past its end 3 times');
            }

            return '';
        }

        $length = max(0, min($count, $this->size - $this->position));
        $this->position += $length;

        return str_repeat('a', $length);
    }

    public function stream_seek(int $offset, int $whence = SEEK_SET): bool
    {
        if ($whence !== SEEK_SET) {
            return false;
        }

        $this->position = $offset;

        return true;
    }

    /** @return array<string, int> */
    public function stream_stat(): array
    {
        return ['size' => $this->size];
    }

    public function stream_tell(): int
    {
        return $this->position;
    }
}
