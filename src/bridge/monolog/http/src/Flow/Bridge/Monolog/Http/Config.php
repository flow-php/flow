<?php

declare(strict_types=1);

namespace Flow\Bridge\Monolog\Http;

use Flow\Bridge\Monolog\Http\Config\{RequestConfig, ResponseConfig};

final readonly class Config
{
    public function __construct(
        public RequestConfig $request = new RequestConfig(),
        public ResponseConfig $response = new ResponseConfig(),
    ) {

    }
}
