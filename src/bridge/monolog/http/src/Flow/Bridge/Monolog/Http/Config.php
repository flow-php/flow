<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http;

use Flow\Bridge\Monolog\Http\Config\{RequestConfig, ResponseConfig};

final class Config
{
    public function __construct(
        public readonly RequestConfig $request = new RequestConfig(),
        public readonly ResponseConfig $response = new ResponseConfig()
    ) {

    }
}
