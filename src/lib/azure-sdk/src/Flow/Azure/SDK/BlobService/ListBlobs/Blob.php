<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\ListBlobs;

use DateTimeImmutable;
use DateTimeZone;
use InvalidArgumentException;

use function array_key_exists;
use function is_array;
use function is_int;
use function is_string;

final readonly class Blob
{
    /**
     * @param array<array-key, mixed> $data
     */
    public function __construct(
        private array $data,
    ) {}

    public function lastModifiedAt(): ?DateTimeImmutable
    {
        if (!array_key_exists('Properties', $this->data) || !is_array($this->data['Properties'])) {
            return null;
        }

        if (
            !array_key_exists('Last-Modified', $this->data['Properties'])
            || !is_string($this->data['Properties']['Last-Modified'])
            || $this->data['Properties']['Last-Modified'] === ''
        ) {
            return null;
        }

        $parsed = DateTimeImmutable::createFromFormat(
            'D, d M Y H:i:s \G\M\T',
            $this->data['Properties']['Last-Modified'],
            new DateTimeZone('GMT'),
        );

        return $parsed === false ? null : $parsed;
    }

    public function name(): string
    {
        if (!array_key_exists('Name', $this->data) || !is_string($this->data['Name'])) {
            throw new InvalidArgumentException('Blob name must be a string');
        }

        return $this->data['Name'];
    }

    public function size(): int
    {
        if (!array_key_exists('Properties', $this->data) || !is_array($this->data['Properties'])) {
            throw new InvalidArgumentException('Blob properties must be an array');
        }

        if (!array_key_exists('Content-Length', $this->data['Properties'])) {
            throw new InvalidArgumentException('Content-Length must be a string or integer');
        }

        if (
            !is_string($this->data['Properties']['Content-Length'])
            && !is_int($this->data['Properties']['Content-Length'])
        ) {
            throw new InvalidArgumentException('Content-Length must be a string or integer');
        }

        return (int) $this->data['Properties']['Content-Length'];
    }
}
