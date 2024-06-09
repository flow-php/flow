<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

final class TmpfileBuffer implements LocalBuffer
{
    /**
     * @var ?resource
     */
    private $stream;

    public function release() : void
    {
        if (\is_resource($this->stream)) {
            /**
             * @psalm-suppress InvalidPropertyAssignmentValue
             */
            \fclose($this->stream);
        }
    }

    public function seek(int $offset, int $whence = SEEK_SET) : void
    {
        \fseek($this->stream(), $offset, $whence);
    }

    /**
     * @return resource
     */
    public function stream()
    {
        if ($this->stream === null) {
            $this->stream = \tmpfile();
        }

        return $this->stream;
    }

    public function tell() : int|false
    {
        return \ftell($this->stream());
    }

    public function write(string $data) : void
    {
        \fwrite($this->stream(), $data);
    }
}
