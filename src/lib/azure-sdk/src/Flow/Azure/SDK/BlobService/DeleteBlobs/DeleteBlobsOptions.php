<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\BlobService\DeleteBlobs;

use Flow\Azure\SDK\BlobService;
use Flow\Azure\SDK\BlobService\DeleteBlob\DeleteSnapshots;
use Flow\Azure\SDK\EndpointOptions;

final class DeleteBlobsOptions implements EndpointOptions
{
    private string $userAgentHeader = 'flow-php/azure-sdk';

    private ?DeleteSnapshots $deleteSnapshots = null;

    private ?string $requestId = null;

    private ?int $timeoutSeconds = null;

    private string $version = BlobService::VERSION;

    /**
     * @return array<string, string>
     */
    public function toHeaders(): array
    {
        $headers = [];

        $headers['user-agent'] = $this->userAgentHeader();
        $headers['x-ms-version'] = $this->version;

        if ($this->requestId !== null) {
            $headers['x-ms-client-request-id'] = $this->requestId;
        }

        return $headers;
    }

    /**
     * @return array<string, string>
     */
    public function toSubrequestHeaders(): array
    {
        if ($this->deleteSnapshots === null) {
            return [];
        }

        return ['x-ms-delete-snapshots' => $this->deleteSnapshots->value];
    }

    /**
     * @return array<string, int>
     */
    public function toURIParameters(): array
    {
        if ($this->timeoutSeconds === null) {
            return [];
        }

        return ['timeout' => $this->timeoutSeconds];
    }

    public function withDeleteSnapshots(DeleteSnapshots $deleteSnapshots): self
    {
        $this->deleteSnapshots = $deleteSnapshots;

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
