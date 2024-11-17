<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetBlob;

use Flow\Azure\SDK\{BlobService, EndpointOptions, Endpoints\UserAgentHeader};

final class GetBlobOptions implements EndpointOptions
{
    use UserAgentHeader;

    private ?string $encryptionAlgorithm = null;

    private ?string $encryptionKey = null;

    private ?string $encryptionKeySha256 = null;

    private ?string $leaseId = null;

    private ?string $origin = null;

    private ?Range $range = null;

    private bool $rangeGetContentCrc64 = false;

    private bool $rangeGetContentMd5 = false;

    private ?string $requestId = null;

    private ?string $snapshot = null;

    private ?int $timeoutSeconds = null;

    private string $version = BlobService::VERSION;

    private ?string $versionId = null;

    public function toHeaders() : array
    {
        $headers = [];

        $headers['user-agent'] = $this->userAgentHeader();
        $headers['x-ms-version'] = $this->version;

        if ($this->range !== null) {
            $headers['x-ms-range'] = $this->range->toString();
        }

        if ($this->requestId !== null) {
            $headers['x-ms-client-request-id'] = $this->requestId;
        }

        if ($this->leaseId !== null) {
            $headers['x-ms-lease-id'] = $this->leaseId;
        }

        if ($this->rangeGetContentMd5) {
            $headers['x-ms-range-get-content-md5'] = 'true';
        }

        if ($this->rangeGetContentCrc64) {
            $headers['x-ms-range-get-content-crc64'] = 'true';
        }

        if ($this->origin !== null) {
            $headers['Origin'] = $this->origin;
        }

        if ($this->encryptionKey !== null) {
            $headers['x-ms-encryption-key'] = $this->encryptionKey;
        }

        if ($this->encryptionKeySha256 !== null) {
            $headers['x-ms-encryption-key-sha256'] = $this->encryptionKeySha256;
        }

        if ($this->encryptionAlgorithm !== null) {
            $headers['x-ms-encryption-algorithm'] = $this->encryptionAlgorithm;
        }

        return $headers;
    }

    public function toURIParameters() : array
    {
        $uriParameters = [];

        if ($this->versionId !== null) {
            $uriParameters['versionId'] = $this->versionId;
        }

        if ($this->timeoutSeconds !== null) {
            $uriParameters['timeout'] = $this->timeoutSeconds;
        }

        if ($this->snapshot !== null) {
            $uriParameters['snapshot'] = $this->snapshot;
        }

        return $uriParameters;
    }

    public function withEncryption(string $encryptionKey, string $encryptionAlgorithm, ?string $encryptionKeySha256 = null) : self
    {
        $this->encryptionKey = $encryptionKey;
        $this->encryptionKeySha256 = $encryptionKeySha256;
        $this->encryptionAlgorithm = $encryptionAlgorithm;

        return $this;
    }

    public function withLeaseId(string $leaseId) : self
    {
        $this->leaseId = $leaseId;

        return $this;
    }

    public function withOrigin(string $origin) : self
    {
        $this->origin = $origin;

        return $this;
    }

    public function withRange(Range $range) : self
    {
        $this->range = $range;

        return $this;
    }

    public function withRangeGetContentCrc64(bool $rangeGetContentCrc64) : self
    {
        $this->rangeGetContentCrc64 = $rangeGetContentCrc64;

        return $this;
    }

    public function withRangeGetContentMd5(bool $rangeGetContentMd5) : self
    {
        $this->rangeGetContentMd5 = $rangeGetContentMd5;

        return $this;
    }

    public function withRequestId(string $requestId) : self
    {
        $this->requestId = $requestId;

        return $this;
    }

    public function withSnapshot(string $snapshot) : self
    {
        $this->snapshot = $snapshot;

        return $this;
    }

    public function withTimeoutSeconds(int $timeoutSeconds) : self
    {
        $this->timeoutSeconds = $timeoutSeconds;

        return $this;
    }

    public function withVersion(string $version) : self
    {
        $this->version = $version;

        return $this;
    }

    public function withVersionId(string $versionId) : self
    {
        $this->versionId = $versionId;

        return $this;
    }
}
