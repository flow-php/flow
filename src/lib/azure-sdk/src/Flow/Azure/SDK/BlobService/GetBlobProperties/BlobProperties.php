<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetBlobProperties;

use DateTimeImmutable;
use DateTimeZone;
use Psr\Http\Message\ResponseInterface;

use function implode;
use function str_starts_with;
use function strlen;
use function strtolower;
use function substr;

final readonly class BlobProperties
{
    private const string METADATA_PREFIX = 'x-ms-meta-';

    public function __construct(
        private ResponseInterface $response,
    ) {}

    public function cacheControl(): ?string
    {
        return $this->header('Cache-Control');
    }

    public function contentDisposition(): ?string
    {
        return $this->header('Content-Disposition');
    }

    public function contentEncoding(): ?string
    {
        return $this->header('Content-Encoding');
    }

    public function contentLanguage(): ?string
    {
        return $this->header('Content-Language');
    }

    public function contentType(): ?string
    {
        return $this->header('Content-Type');
    }

    public function lastModifiedAt(): ?DateTimeImmutable
    {
        $raw = $this->response->getHeaderLine('Last-Modified');

        if ($raw === '') {
            return null;
        }

        $parsed = DateTimeImmutable::createFromFormat('D, d M Y H:i:s \G\M\T', $raw, new DateTimeZone('GMT'));

        return $parsed === false ? null : $parsed;
    }

    /**
     * @return array<string, string>
     */
    public function metadata(): array
    {
        $metadata = [];

        foreach ($this->response->getHeaders() as $name => $values) {
            $name = strtolower((string) $name);

            if (str_starts_with($name, self::METADATA_PREFIX)) {
                $metadata[substr($name, strlen(self::METADATA_PREFIX))] = implode(', ', $values);
            }
        }

        return $metadata;
    }

    public function size(): int
    {
        return (int) $this->response->getHeaderLine('Content-Length');
    }

    private function header(string $name): ?string
    {
        $value = $this->response->getHeaderLine($name);

        return $value === '' ? null : $value;
    }
}
