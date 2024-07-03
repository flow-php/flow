<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\PutBlockBlobBlock;

use Flow\Azure\SDK\{BlobService, EndpointOptions, Endpoints\UserAgentHeader};

final class PutBlockBlobBlockOptions implements EndpointOptions
{
    use UserAgentHeader;

    private ?string $encryptionAlgorithm = null;

    private ?string $encryptionKey = null;

    private ?string $encryptionKeySha256 = null;

    private ?string $requestId = null;

    private ?int $timeoutSeconds = null;

    private string $version = BlobService::VERSION;

    public function toHeaders() : array
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

        return $headers;
    }

    public function toURIParameters() : array
    {
        $uriParameters = [];

        if ($this->timeoutSeconds !== null) {
            $uriParameters['timeout'] = $this->timeoutSeconds;
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

    public function withRequestId(string $requestId) : self
    {
        $this->requestId = $requestId;

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
}
