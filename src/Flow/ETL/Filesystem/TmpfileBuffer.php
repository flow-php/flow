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

    /**
     * @return resource
     */
    public function stream()
    {
        if ($this->stream === null) {
            /** @phpstan-ignore-next-line */
            $this->stream = \tmpfile();
        }

        /** @phpstan-ignore-next-line */
        return $this->stream;
    }

    public function write(string $data) : void
    {
        \fwrite($this->stream(), $data);
    }
}
