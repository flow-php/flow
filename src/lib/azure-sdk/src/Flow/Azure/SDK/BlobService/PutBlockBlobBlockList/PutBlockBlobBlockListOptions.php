<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\PutBlockBlobBlockList;

use Flow\Azure\SDK\BlobService;
use Flow\Azure\SDK\EndpointOptions;

final class PutBlockBlobBlockListOptions implements EndpointOptions
{
    private string $userAgentHeader = 'flow-php/azure-sdk';

    private ?string $cacheControl = null;

    private ?string $contentDisposition = null;

    private ?string $contentEncoding = null;

    private ?string $contentLanguage = null;

    private ?int $contentLength = null;

    private ?string $contentType = null;

    private ?string $encryptionAlgorithm = null;

    private ?string $encryptionKey = null;

    private ?string $encryptionKeySha256 = null;

    private ?string $ifNoneMatch = null;

    /** @var array<string, string> */
    private array $metadata = [];

    private ?string $requestId = null;

    private ?int $timeoutSeconds = null;

    private string $version = BlobService::VERSION;

    /**
     * @return array<string, string>
     */
    public function toHeaders(): array
    {
        $headers = [];

        $headers['x-ms-version'] = $this->version;

        $headers['user-agent'] = $this->userAgentHeader();

        if ($this->requestId !== null) {
            $headers['x-ms-client-request-id'] = $this->requestId;
        }

        if ($this->encryptionAlgorithm !== null) {
            $headers['x-ms-encryption-algorithm'] = $this->encryptionAlgorithm;
        }

        if ($this->encryptionKey !== null) {
            $headers['x-ms-encryption-key'] = $this->encryptionKey;
        }

        if ($this->encryptionKeySha256 !== null) {
            $headers['x-ms-encryption-key-sha256'] = $this->encryptionKeySha256;
        }

        if ($this->ifNoneMatch !== null) {
            $headers['If-None-Match'] = $this->ifNoneMatch;
        }

        if ($this->contentLength !== null) {
            $headers['Content-Length'] = (string) $this->contentLength;
        }

        if ($this->contentType !== null) {
            $headers['x-ms-blob-content-type'] = $this->contentType;
        }

        if ($this->contentEncoding !== null) {
            $headers['x-ms-blob-content-encoding'] = $this->contentEncoding;
        }

        if ($this->contentLanguage !== null) {
            $headers['x-ms-blob-content-language'] = $this->contentLanguage;
        }

        if ($this->cacheControl !== null) {
            $headers['x-ms-blob-cache-control'] = $this->cacheControl;
        }

        if ($this->contentDisposition !== null) {
            $headers['x-ms-blob-content-disposition'] = $this->contentDisposition;
        }

        foreach ($this->metadata as $name => $value) {
            $headers['x-ms-meta-' . $name] = $value;
        }

        return $headers;
    }

    /**
     * @return array<string, string>
     */
    public function toURIParameters(): array
    {
        $uriParameters = [];

        if ($this->timeoutSeconds !== null) {
            $uriParameters['timeout'] = (string) $this->timeoutSeconds;
        }

        return $uriParameters;
    }

    public function withCacheControl(?string $cacheControl): self
    {
        $this->cacheControl = $cacheControl;

        return $this;
    }

    public function withContentDisposition(?string $contentDisposition): self
    {
        $this->contentDisposition = $contentDisposition;

        return $this;
    }

    public function withContentEncoding(?string $contentEncoding): self
    {
        $this->contentEncoding = $contentEncoding;

        return $this;
    }

    public function withContentLanguage(?string $contentLanguage): self
    {
        $this->contentLanguage = $contentLanguage;

        return $this;
    }

    public function withContentLength(int $contentLength): self
    {
        $this->contentLength = $contentLength;

        return $this;
    }

    public function withContentType(?string $contentType): self
    {
        $this->contentType = $contentType;

        return $this;
    }

    public function withEncryption(
        string $encryptionKey,
        string $encryptionAlgorithm,
        ?string $encryptionKeySha256 = null,
    ): self {
        $this->encryptionKey = $encryptionKey;
        $this->encryptionKeySha256 = $encryptionKeySha256;
        $this->encryptionAlgorithm = $encryptionAlgorithm;

        return $this;
    }

    public function withIfNoneMatch(string $ifNoneMatch): self
    {
        $this->ifNoneMatch = $ifNoneMatch;

        return $this;
    }

    /**
     * @param array<string, string> $metadata
     */
    public function withMetadata(array $metadata): self
    {
        $this->metadata = $metadata;

        return $this;
    }

    public function withRequestId(string $requestId): self
    {
        $this->requestId = $requestId;

        return $this;
    }

    public function withTimeoutSeconds(int $timeoutSeconds): self
    {
        $this->timeoutSeconds = $timeoutSeconds;

        return $this;
    }

    public function withVersion(string $version): self
    {
        $this->version = $version;

        return $this;
    }

    public function userAgentHeader(): string
    {
        return $this->userAgentHeader;
    }

    public function withUserAgent(string $userAgentHeader): void
    {
        $this->userAgentHeader = $userAgentHeader;
    }
}
