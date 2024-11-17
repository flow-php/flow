<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\CreateContainer;

use Flow\Azure\SDK\{BlobService, EndpointOptions, Endpoints\UserAgentHeader};

final class CreateContainerOptions implements EndpointOptions
{
    use UserAgentHeader;

    private ?PublicAccess $publicAccess = null;

    private ?string $requestId = null;

    private ?int $timeoutSeconds = null;

    private string $version = BlobService::VERSION;

    public function toHeaders() : array
    {
        $headers = [];

        $headers['user-agent'] = $this->userAgentHeader();
        $headers['x-ms-version'] = $this->version;

        if ($this->requestId !== null) {
            $headers['x-ms-client-request-id'] = $this->requestId;
        }

        if ($this->publicAccess !== null) {
            $headers['x-ms-blob-public-access'] = $this->publicAccess->value;
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

    public function withPublicAccess(PublicAccess $publicAccess) : self
    {
        $this->publicAccess = $publicAccess;

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
