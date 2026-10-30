<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\PutBlockBlobBlockList;

use Flow\Azure\SDK\BlobService;
use Flow\Azure\SDK\EndpointOptions;

final class PutBlockBlobBlockListOptions implements EndpointOptions
{
    private string $userAgentHeader = 'flow-php/azure-sdk';

    private ?int $contentLength = null;

    private ?string $encryptionAlgorithm = null;

    private ?string $encryptionKey = null;

    private ?string $encryptionKeySha256 = null;

    private ?string $ifNoneMatch = null;

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

    public function withContentLength(int $contentLength): self
    {
        $this->contentLength = $contentLength;

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
