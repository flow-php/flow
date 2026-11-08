<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\Console\DSL;

use Flow\Bridge\Symfony\Console\ConsoleHeartbeat;
use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type as DSLType;
use Symfony\Component\Console\Output\OutputInterface;

#[DocumentationDSL(module: Module::SYMFONY_CONSOLE_BRIDGE, type: DSLType::HELPER)]
function console_heartbeat(OutputInterface $output): ConsoleHeartbeat
{
    return new ConsoleHeartbeat($output);
}
