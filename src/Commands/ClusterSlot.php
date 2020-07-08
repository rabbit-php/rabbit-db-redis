<?php
declare(strict_types=1);

namespace Rabbit\DB\Redis\Commands;

/**
 * Trait ClusterSlot
 * @package Rabbit\DB\Redis\Commands
 */
trait ClusterSlot
{
    /**
     * @return int|null
     */
    public function getSlot()
    {
        return 0;
    }
}