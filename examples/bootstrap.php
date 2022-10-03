<?php declare(strict_types=1);

const __FLOW_DATA__ = __DIR__ . '/data';
const __FLOW_OUTPUT__ = __DIR__ . '/output';
const __FLOW_VAR__ = __DIR__ . '/var';
const __FLOW_VAR_RUN__ = __DIR__ . '/var/run';
const __FLOW_SRC__ = __DIR__ . '/../src';
const __FLOW_AUTOLOAD__ =  __DIR__ . '/../vendor/autoload.php';
const __FLOW_EXAMPLES_AUTOLOAD__ =  __DIR__ . '/vendor/autoload.php';

// library autoload for all dependencies
require __FLOW_AUTOLOAD__;

// examples autoload for additional dependencies
require __FLOW_EXAMPLES_AUTOLOAD__;

if (!\is_dir(__FLOW_VAR__)) {
    \mkdir(__FLOW_VAR__);
}

if (!\is_dir(__FLOW_VAR_RUN__)) {
    \mkdir(__FLOW_VAR_RUN__);
}
