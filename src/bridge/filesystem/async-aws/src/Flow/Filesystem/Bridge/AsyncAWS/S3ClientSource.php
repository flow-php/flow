<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\Core\Configuration;
use AsyncAws\S3\S3Client;

interface S3ClientSource
{
    public function client(): S3Client;

    /**
     * @return null|array<Configuration::OPTION_*, null|string> null for a live client that cannot be rebuilt elsewhere
     */
    public function normalize(): ?array;
}
