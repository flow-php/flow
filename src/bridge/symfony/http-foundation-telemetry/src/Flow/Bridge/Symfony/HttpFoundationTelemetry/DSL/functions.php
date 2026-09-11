<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundationTelemetry\DSL;

use Flow\Bridge\Symfony\HttpFoundationTelemetry\RequestCarrier;
use Flow\Bridge\Symfony\HttpFoundationTelemetry\ResponseCarrier;
use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;

#[DocumentationDSL(module: Module::SYMFONY_HTTP_FOUNDATION_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function symfony_request_carrier(Request $request): RequestCarrier
{
    return new RequestCarrier($request);
}

#[DocumentationDSL(module: Module::SYMFONY_HTTP_FOUNDATION_TELEMETRY_BRIDGE, type: DSLType::HELPER)]
function symfony_response_carrier(Response $response): ResponseCarrier
{
    return new ResponseCarrier($response);
}
