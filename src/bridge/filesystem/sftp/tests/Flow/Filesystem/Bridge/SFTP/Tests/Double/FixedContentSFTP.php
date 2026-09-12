<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\Tests\Double;

use Override;
use phpseclib3\Net\SFTP;

use function min;
use function str_repeat;
use function strlen;
use function substr;

final class FixedContentSFTP extends SFTP
{
    private function __construct(
        private readonly string $content,
        private readonly ?int $shortReadAfter,
    ) {
        parent::__construct('localhost');
    }

    public static function serving(string $content): self
    {
        return new self($content, null);
    }

    /**
     * @param positive-int $bytes
     */
    public static function shortRead(int $bytes): self
    {
        return new self(str_repeat('x', $bytes), 0);
    }

    #[Override]
    public function filesize($path, $recursive = false)
    {
        return strlen($this->content);
    }

    #[Override]
    public function get($remote_file, $local_file = false, $offset = 0, $length = -1, $progressCallback = null)
    {
        if ($this->shortReadAfter !== null) {
            return '';
        }

        if ($length < 0) {
            return substr($this->content, $offset);
        }

        return substr($this->content, $offset, min($length, strlen($this->content) - $offset));
    }
}
