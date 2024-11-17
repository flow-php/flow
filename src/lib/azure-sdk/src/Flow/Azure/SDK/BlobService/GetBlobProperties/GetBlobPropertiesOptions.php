<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetBlobProperties;

use Flow\Azure\SDK\{BlobService, EndpointOptions, Endpoints\UserAgentHeader};

final class GetBlobPropertiesOptions implements EndpointOptions
{
    use UserAgentHeader;

    private ?string $encryptionAlgorithm = null;

    private ?string $encryptionKey = null;

    private ?string $encryptionKeySha256 = null;

    private ?string $leaseId = null;

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

        if ($this->requestId !== null) {
            $headers['x-ms-client-request-id'] = $this->requestId;
        }

        if ($this->leaseId !== null) {
            $headers['x-ms-lease-id'] = $this->leaseId;
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
