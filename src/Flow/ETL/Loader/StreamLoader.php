<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Formatter;
use Flow\ETL\Loader;
use Flow\ETL\Rows;

final class StreamLoader implements Loader
{
    private string $url;

    private string $mode;

    private int $truncate;

    private Formatter $formatter;

    /**
     * @param string $url all protocols supported by PHP are allowed https://www.php.net/manual/en/wrappers.php
     * @param string $mode only writing modes explained in https://www.php.net/manual/en/function.fopen.php are supported
     * @param int $truncate if 0, then columns in display are not truncated
     * @param null|Formatter $formatter - if not passed AsciiTableFormatter is used
     */
    public function __construct(string $url, string $mode = 'w', int $truncate = 20, Formatter $formatter = null)
    {
        $this->url = $url;
        $this->mode = $mode;
        $this->formatter = $formatter ?? new Formatter\AsciiTableFormatter();
        $this->truncate = $truncate;
    }

    public static function stdout(int $truncate = 20, Formatter $formatter = null) : self
    {
        return new self('php://stdout', 'w', $truncate, $formatter);
    }

    public static function stderr(int $truncate = 20, Formatter $formatter = null) : self
    {
        return new self('php://stderr', 'w', $truncate, $formatter);
    }

    public static function output(int $truncate = 20, Formatter $formatter = null) : self
    {
        return new self('php://output', 'w', $truncate, $formatter);
    }

    /**
     * @return array{url: string, mode: string, truncate: int, formatter: Formatter}
     */
    public function __serialize() : array
    {
        return [
            'url' => $this->url,
            'mode' => $this->mode,
            'truncate' => $this->truncate,
            'formatter' => $this->formatter,
        ];
    }

    /**
     * @param array{url: string, mode: string, truncate: int, formatter: Formatter} $data
     * @psalm-suppress MoreSpecificImplementedParamType
     */
    public function __unserialize(array $data) : void
    {
        $this->url = $data['url'];
        $this->mode = $data['mode'];
        $this->truncate = $data['truncate'];
        $this->formatter = $data['formatter'];
    }

    public function load(Rows $rows) : void
    {
        try {
            $stream = \fopen($this->url, $this->mode);
        } catch (\Throwable $e) {
            throw new RuntimeException("Can't open stream for url: {$this->url} in mode: {$this->mode}. Reason: " . $e->getMessage(), (int) $e->getCode(), $e);
        }

        if ($stream === false) {
            throw new RuntimeException("Can't open stream for url: {$this->url} in mode: {$this->mode}");
        }
        \fwrite($stream, $this->formatter->format($rows, $this->truncate));
        \fclose($stream);
    }
}
