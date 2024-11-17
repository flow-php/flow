<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\GetBlockBlobBlockList;

use Flow\Azure\SDK\Endpoints\UserAgentHeader;
use Flow\Azure\SDK\{BlobService, EndpointOptions};

final class GetBlockBlobBlockListOptions implements EndpointOptions
{
    use UserAgentHeader;

    private BlockListType $blockListType = BlockListType::ALL;

    private ?string $leaseId = null;

    private ?string $requestId = null;

    private ?string $snapshot = null;

    private ?int $timeoutSeconds = null;

    private string $version = BlobService::VERSION;

    private ?string $versionId = null;

    public function toHeaders() : array
    {
        $headers = [];

        $headers['x-ms-version'] = $this->version;
        $headers['user-agent'] = $this->userAgentHeader();

        if ($this->requestId !== null) {
            $headers['x-ms-client-request-id'] = $this->requestId;
        }

        if ($this->leaseId !== null) {
            $headers['x-ms-lease-id'] = $this->leaseId;
        }

        return $headers;
    }

    public function toURIParameters() : array
    {
        $uriParameters = [];

        $uriParameters['blocklisttype'] = $this->blockListType->value;

        if ($this->timeoutSeconds !== null) {
            $uriParameters['timeout'] = $this->timeoutSeconds;
        }

        if ($this->snapshot !== null) {
            $uriParameters['snapshot'] = $this->snapshot;
        }

        if ($this->versionId !== null) {
            $uriParameters['versionId'] = $this->versionId;
        }

        return $uriParameters;
    }

    public function withBlockListType(BlockListType $blockListType) : self
    {
        $this->blockListType = $blockListType;

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

    public function withVersionId(string $versionId) : self
    {
        $this->versionId = $versionId;

        return $this;
    }
}
