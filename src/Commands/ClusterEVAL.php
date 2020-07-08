<?php

declare(strict_types=1);

namespace Rabbit\DB\Redis\Commands;

use Predis\Command\ServerEval;

/**
 * Class ClusterEVAL
 * @package Rabbit\DB\Redis\Commands
 */
class ClusterEVAL extends ServerEval
{
    use ClusterSlot;
}