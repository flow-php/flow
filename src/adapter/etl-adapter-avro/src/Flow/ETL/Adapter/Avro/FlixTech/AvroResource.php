<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Exception\InvalidLogicException;

final class AvroResource extends \AvroIO
{
    /**
     * @var resource file handle for AvroFile instance
     */
    private $file_handle;

    /**
     * @param false|resource $resource
     *
     * @throws InvalidLogicException
     */
    public function __construct($resource)
    {
        if ($resource === false) {
            throw new InvalidLogicException('Stream not open');
        }

        $this->file_handle = $resource;
    }

    /**
     * Closes the file.
     *
     * @throws \AvroIOException if there was an error closing the file
     *
     * @return bool true if successful
     */
    public function close()
    {
        /** @psalm-suppress InvalidPropertyAssignmentValue */
        $res = \fclose($this->file_handle);

        if (false === $res) {
            throw new \AvroIOException('Error closing file.');
        }

        return $res;
    }

    /**
     * @throws \AvroIOException if there was an error flushing the file
     *
     * @return bool true if the flush was successful
     */
    public function flush()
    {
        $res = \fflush($this->file_handle);

        if (false === $res) {
            throw new \AvroIOException('Could not flush file.');
        }

        return true;
    }

    /**
     * @return bool true if the pointer is at the end of the file,
     *              and false otherwise
     *
     * @see AvroIO::is_eof() as behavior differs from feof()
     */
    public function is_eof()
    {
        $this->read(1);

        if (\feof($this->file_handle)) {
            return true;
        }
        $this->seek(-1, self::SEEK_CUR);

        return false;
    }

    /**
     * @param int $len count of bytes to read
     *
     * @throws \AvroIOException if length value is negative or if the read failed
     *
     * @return string bytes read
     */
    public function read($len)
    {
        if (0 > $len) {
            throw new \AvroIOException(
                \sprintf('Invalid length value passed to read: %d', $len)
            );
        }

        if (0 == $len) {
            return '';
        }

        $bytes = \fread($this->file_handle, $len);

        if (false === $bytes) {
            throw new \AvroIOException('Could not read from file');
        }

        return $bytes;
    }

    /**
     * @param int $offset
     * @param int $whence
     *
     * @throws \AvroIOException if seek failed
     *
     * @return bool true upon success
     *
     * @see AvroIO::seek()
     */
    public function seek($offset, $whence = SEEK_SET)
    {
        $res = \fseek($this->file_handle, $offset, $whence);
        // Note: does not catch seeking beyond end of file
        if (-1 === $res) {
            throw new \AvroIOException(
                \sprintf(
                    'Could not execute seek (offset = %d, whence = %d)',
                    $offset,
                    $whence
                )
            );
        }

        return true;
    }

    /**
     * @throws \AvroIOException
     *
     * @return int current position within the file
     */
    public function tell()
    {
        $position = \ftell($this->file_handle);

        if (false === $position) {
            throw new \AvroIOException('Could not execute tell on reader');
        }

        return $position;
    }

    /**
     * @param string $arg
     *
     *@throws \AvroIOException if write failed
     *
     * @return int count of bytes written
     */
    public function write($arg)
    {
        $len = \fwrite($this->file_handle, $arg);

        if (false === $len) {
            throw new \AvroIOException(\sprintf('Could not write to file'));
        }

        return $len;
    }
}
