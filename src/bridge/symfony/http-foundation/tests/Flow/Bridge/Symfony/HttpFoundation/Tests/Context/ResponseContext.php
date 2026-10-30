<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Tests\Context;

use Flow\Bridge\Symfony\HttpFoundation\Response\FlowStreamedResponse;

use function ob_get_clean;
use function ob_start;

final class ResponseContext
{
    public static function send(FlowStreamedResponse $response): string
    {
        ob_start();
        $response->send();

        return (string) ob_get_clean();
    }
}
